import os
# from pathlib import Path

import wget

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt

from _readdbc import ffi, lib
from tempfile import NamedTemporaryFile
from dbfread import DBF
import time

from utils import CACHEPATH, RAW_DATA, PRODUCED_DATASETS

months = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']


# def create_cache_path():
#     '''create cache directory
#     Returns
#     -------
#         CACHEPATH, RAW_DATA, PRODUCED_DATASETS
#     '''
#     if not os.path.exists(os.path.join(Path.home(), 'tcc_hemato_sus')):
#         os.mkdir(os.path.join(Path.home(), 'tcc_hemato_sus'))

#     CACHEPATH = os.path.join(Path.home(), 'tcc_hemato_sus')

#     for f in ['produced_datasets', 'raw_data']:
#         if not os.path.exists(os.path.join(Path(CACHEPATH), f)):
#             os.mkdir(os.path.join(Path(CACHEPATH), f))
#     PRODUCED_DATASETS = os.path.join(Path(CACHEPATH), 'produced_datasets')
#     RAW_DATA = os.path.join(Path(CACHEPATH), 'raw_data')

#     return CACHEPATH, RAW_DATA, PRODUCED_DATASETS


def download_file(filename, ftp_url, local_folder, force_download):
    """Download files if it doesn't exist and save locally in .parquet file."""
    local_file = os.path.join(local_folder, filename) + '.parquet'
    local_file_dbc = os.path.join(local_folder, filename) + '.dbc'
    url_file = ftp_url + '/' + filename + '.dbc'
    print(url_file)
    if not os.path.exists(local_file) or force_download == 'deep':
#         print('parquet não existe')
        if not os.path.exists(local_file_dbc) or force_download == 'deep':
            try:
                wget.download(url_file, local_file)
#                 print('downloading', url_file)
            except:
#                 print('deu merda')
                time.sleep(60)
                download_file(filename, ftp_url, local_folder, force_download)
        df = read_dbc(local_file_dbc, encoding='iso-8859-1')
        df.to_parquet(local_file, engine='fastparquet')
        print('saving', local_file)


def _mem_usage(pandas_obj):
    if isinstance(pandas_obj, pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else:  # we assume if not a df it's a series
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2  # convert bytes to megabytes
    return "Total memory usage: {:03.2f} MB".format(usage_mb)


def optimize_df(df):
    '''Optimize df downcasting int and float columns and
    turning object columns in categorical when they have less then 50%
    unique values of the total.

    Ripped from 'https://www.dataquest.io/blog/pandas-big-data'
    to deal with large pandas dataframe without using parallel os distributed computing.
    (Don't deal with datetime columns.)

    Parameters
    ----------
    df: pandas.dataframe
        df to be optimized

    Return
    ------
    optimized pandas.DataFrame
    dict with optimized column dtypes to use when reading from the database.
    '''

    print('Optimizing df...\n')

    # downcast int dtypes
    gl_int = df.select_dtypes(include=['int'])
    converted_int = gl_int.apply(pd.to_numeric, downcast='unsigned')

    # downcast float dtypes
    gl_float = df.select_dtypes(include=['float'])
    converted_float = gl_float.apply(pd.to_numeric, downcast='float')

    # deal with string columns
    gl_obj = df.select_dtypes(include=['object']).copy()
    converted_obj = pd.DataFrame()

    for col in gl_obj.columns:
        num_unique_values = len(gl_obj[col].unique())
        num_total_values = len(gl_obj[col])
        if num_unique_values / num_total_values < 0.5:
            converted_obj.loc[:, col] = gl_obj[col].astype('category')
        else:
            converted_obj.loc[:, col] = gl_obj[col]

    # join converted columns
    optimized_gl = df.copy()
    optimized_gl[converted_int.columns] = converted_int
    optimized_gl[converted_float.columns] = converted_float
    optimized_gl[converted_obj.columns] = converted_obj

    # make dict with optimized dtypes
    dtypes = optimized_gl.dtypes  # optimized_gl.drop(dates, axis=1).dtypes
    dtypes_col = dtypes.index
    dtypes_type = [i.name for i in dtypes.values]
    column_types = dict(zip(dtypes_col, dtypes_type))

    print('Original df size:', _mem_usage(df))
    print('Optimized df size:', _mem_usage(optimized_gl))

    df = df.astype(column_types)
    
    # especific for hemato dataset
    df.DT_INTER = pd.to_datetime(df.DT_INTER)
    df.DT_SAIDA = pd.to_datetime(df.DT_SAIDA)

    return df


def read_dbc(local_file, encoding='utf-8'):
    """
    Opens a DATASUS .dbc file and return its contents as a pandas
    Dataframe.
    :param filename: .dbc filename
    :param encoding: encoding of the data
    :return: Pandas Dataframe.
    """
    if isinstance(local_file, str):
        filename = local_file.encode()
    with NamedTemporaryFile(delete=False) as tf:
        dbc2dbf(local_file, tf.name.encode())
        dbf = DBF(tf.name, encoding=encoding)
        df = pd.DataFrame(list(dbf))
    os.unlink(tf.name)

    return df


def dbc2dbf(infile, outfile):
    """
    Converts a DATASUS dbc file to a DBF database.
    :param infile: .dbc file name
    :param outfile: name of the .dbf file to be created.
    """
    if isinstance(infile, str):
        infile = infile.encode()
    if isinstance(outfile, str):
        outfile = outfile.encode()
    p = ffi.new('char[]', os.path.abspath(infile))
    q = ffi.new('char[]', os.path.abspath(outfile))

    lib.dbc2dbf([p], [q])


def make_hemato_df(state, year, month, force_download):
    '''Download the files from DATASUS, drop some columns and return the hematologic df.
    Param:
    -----
    state: string
        initials of brazilian's states
    year: int
        year in format YYYY
    month: int
        month in format MM
    '''
    if state == 'AC' and year == 2009 and month == 9:
        return None

    state = state.upper()
    if not isinstance(year, str):
        year = str(year)
    year = year[2:]
    month = str(month).zfill(2)

    ftp_url = 'ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/dados'
    filename = 'RD{}{}{}'.format(state, year, month)

    local_folder = os.path.join(RAW_DATA)

    download_file(filename, ftp_url, local_folder, force_download)
    temp_df = pd.read_parquet(os.path.join(local_folder, filename + '.parquet'))
#     temp_df = read_dbc(local_file, encoding='iso-8859-1') ****

    temp_df = select_columns_and_cids(temp_df, state)

    return temp_df


def select_columns_and_cids(temp_df, state):
    # deal with temp_df
    columns_to_drop = ['ESPEC', 'IDENT', 'CEP', 'UTI_MES_IN', 'UTI_MES_AN', 'UTI_MES_AL', 'MARCA_UTI',
                       'UTI_INT_IN', 'UTI_INT_AN', 'UTI_INT_AL', 'PROC_SOLIC', 'PROC_REA', 'VAL_ORTP',
                       'VAL_RN', 'VAL_SADTSR', 'VAL_OBSANG', 'VAL_PED1AC', 'VAL_TRANSP', 'COBRANCA',
                       'NATUREZA', 'NAT_JUR', 'RUBRICA', 'IND_VDRL', 'TOT_PT_SP', 'HOMONIMO', 'NUM_FILHOS',
                       'CONTRACEP1', 'CONTRACEP2', 'GESTRISCO', 'SEQ_AIH5', 'CNAER', 'GESTOR_COD', 'GESTOR_TP',
                       'GESTOR_CPF', 'REGCT', 'SEQUENCIA', 'REMESSA', 'TPDISEC1', 'TPDISEC2', 'TPDISEC3',
                       'TPDISEC4', 'TPDISEC5', 'TPDISEC6', 'TPDISEC7', 'TPDISEC8', 'TPDISEC9', 'DIAG_SECUN']
    l = []
    while True:
        try:  # dealing with differents ds
            temp_df.drop(columns_to_drop, axis=1, inplace=True)
            columns_to_drop = columns_to_drop + l

            break
        except KeyError as inst:
            #             debug_info(inst)
            l = []
            for i in str(inst)[2:-20].split():
                x = i[1:-1]
                l.append(x)
                try:
                    columns_to_drop.remove(x)
                except ValueError:
                    columns_to_drop.remove(x[:-2])

    temp_df.replace('', np.nan, inplace=True)  # nan are represented with ''
    temp_df.dropna(axis=1, inplace=True)

    temp_df['UF'] = state
    temp_df['DIAG_PRINC_CAT'] = temp_df.DIAG_PRINC.str[:3]

    # select only hematologic diseases
    LIST_HEMATO = ['R71', 'D500', 'D508', 'D510', 'D518', 'D520', 'D521', 'D339',
                   'D550', 'D551', 'D561', 'D563', 'D568', 'D569', 'D570', 'D571',
                   'D572', 'D573', 'D578', 'D582', 'D580', 'D581', 'D589', 'D590',
                   'D591', 'D593', 'D594', 'D595', 'D599', 'D65', 'D66', 'D67',
                   'D680', 'M250', 'M362', 'D681', 'D682', 'D683', 'D684', 'D688',
                   'D693', 'D689', 'D690', 'D691', 'D692', 'D695', 'D698', 'D70',
                   'D72', 'D721', 'D728', 'D730', 'D731', 'D732', 'D750', 'D751',
                   'D45', 'D473', 'D752', 'D760', 'D761', 'D762', 'D890', 'D891',
                   'E831', 'K80', 'M250', 'M311', 'M329', 'I260', 'I74', 'I780',
                   'I82', 'D595', 'D600', 'D601', 'D610', 'D611', 'D613', 'D619',
                   'D641', 'D642', 'D648', 'D649', 'D460', 'D461', 'D462', 'D463',
                   'D464', 'D471', 'C819', 'C811', 'C812', 'C813', 'C829', 'C820',
                   'C821', 'C822', 'C830', 'C833', 'C837', 'C835', 'C844', 'C845',
                   'C915', 'C840', 'C841', 'C950', 'C910', 'C920', 'C925', 'C942',
                   'C911', 'C921', 'C90', 'C902', 'C901', 'C880', 'D472']
    
    hemato_df = temp_df[temp_df.DIAG_PRINC.isin(LIST_HEMATO)]
    
    '''
    OLD WAY!!!
    
    cap = ['D']
    groups = ['5', '6', '7', '8']
    varios_hem = temp_df[(temp_df.DIAG_PRINC_CAT.str[0].isin(cap)) & (temp_df.DIAG_PRINC_CAT.str[1].isin(groups))]

    leucemias = temp_df[temp_df.DIAG_PRINC_CAT.str[:2] == 'C9']

    list_linfomas = ['B211', 'B212', 'B213', 'C82', 'C820', 'C821', 'C822', 'C827', 'C829',
                     'C83', 'C830', 'C831', 'C832', 'C833', 'C834', 'C835', 'C836',
                     'C838', 'C839', 'C84', 'C842', 'C843', 'C844', 'C845', 'C85',
                     'C851', 'C857', 'C859', 'C963', 'L412']
    linfomas = temp_df[temp_df.DIAG_PRINC.isin(list_linfomas)]

    list_outras = ['O990', 'P612', 'P613', 'P614', 'Y440', 'Y441', 'O450', 'O460',
                   'O670', 'O723', 'P60', 'P616', 'T457']
    outras = temp_df[temp_df.DIAG_PRINC.isin(list_outras)]

    # make hematologic df
    hemato_df = pd.concat([varios_hem, leucemias, linfomas, outras], sort=False)
    '''

    return hemato_df


def make_all_dataset(force_download='no'):
    '''
    Make all hematologic internations from Brazil in the interval 2008 to 2017.
    When all in cache, takes only about 20" to parse all dataset.
    
    Download from DATASUS ftp when not availiable locally and save each year in a .parquet format.
    It takes about 45' to parse all .dbc files again using all files loccally (force_download='soft'), and hours to download them (force_download='deep' or first download).
    
    Parameters
    ----------
    period: iterable (optional), default (2017, 2008)
        closed interval, years to download
    force_download: string (optional), default 'no'
        'deep' = Force new download of the data.
        'soft' = use .dbc files already downloaded and make the parquet files again, util to change columns or CIDs, p. e.
        
    Returns
    -------
        pandas.DataFrame
    '''
    
    if force_download not in ['no', 'soft', 'deep']:
        raise ValueError("force_download must be either 'no', 'soft' or 'deep'.")
    
    initial_year = 2008
    final_year = 2017

    states = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT',
              'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO',
              'RR', 'SC', 'SP', 'SE', 'TO']
    list_months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep',
                   'Oct', 'Nov', 'Dec']

    file = os.path.join(PRODUCED_DATASETS, 'all_hemato_df.parquet')
    if os.path.exists(file) and force_download == 'no':
        df = pd.read_parquet(file)
        return optimize_df(df)

    hemato_list = []
    for year in range(final_year, initial_year - 1, -1):
        print('Downloading {}... '.format(year))

        filename = 'hemato_{}.parquet'.format(year)
        file = os.path.join(PRODUCED_DATASETS, filename)
        hemato_list.append(file)

        if not os.path.exists(file) or force_download != 'no':
            df_list = []
            for month in range(1, 13):
                print()
                print(list_months[month - 1])
                for state in states:
                    print(state, end=', ')
                    hemato_df = make_hemato_df(state, year, month, force_download)
                    df_list.append(hemato_df)

            df_year = pd.concat(df_list, sort=False)

#             df_year = optimize_df(df_year)
            print('\nSaving {}'.format(filename))
            df_year.to_parquet(file, engine='fastparquet')

        print('Feito {}!!!\n'.format(filename))

    list_df = []
    for file in hemato_list:
        print('\nParsing {}...'.format(file))
        df_temp = pd.read_parquet(file)
        list_df.append(df_temp)

    df_final = pd.concat(list_df, sort=False)
    df_final = optimize_df(df_final)

    df_final.to_parquet(os.path.join(PRODUCED_DATASETS, 'all_hemato_df.parquet'))

    return df_final


def make_weekly_graph(hemato):
    '''
    # Internações por dia da semana, em cada mês
    # (planejamento de plantões/sobreavisos)
    # não é a prevalência, é o número de novas internações (incidência)
    # TODO: fazer bootstrap para ver significância, fazer prevalência

    '''
    pivoted = hemato.pivot_table('N_AIH', index=hemato.DT_INTER.dt.weekday, columns=hemato.DT_INTER.dt.month,
                                 aggfunc='count')

    pivoted.columns.name = None
    pivoted.columns = months
    pivoted.index.name = None

    pivoted.plot(figsize=(12, 8), legend=True)
    plt.xticks(np.arange(7), ['Dom', 'Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb'])
    plt.ylabel('Número de internações')
    plt.title('Internações por dia da semana a cada mês:')
    plt.show()
    print('Parece que não muda muito o padrão.')


def make_daily_int_each_month(hemato):

    # Internações por dia, em cada mês
    # (planejamento de plantões/sobreavisos)
    # não é a prevalência, é o número de novas internações (incidência)


    pivoted = hemato.pivot_table('N_AIH', index=hemato.DT_INTER.dt.day, columns=hemato.DT_INTER.dt.month,
                                 aggfunc='count')

    pivoted.columns.name = None
    pivoted.columns = months
    pivoted.index.name = None

    pivoted.plot(figsize=(12, 8), legend=True, alpha=.9,
                 xticks=(np.arange(1, 32)), yticks=(np.arange(4601, step=500)), ylim=(0, 3000))
    plt.title('Internações totais por dia do mês:')
    plt.ylabel('Número de internações')
    plt.xlabel('Dia do mês')
    plt.show()



# CACHEPATH, RAW_DATA, PRODUCED_DATASETS = create_cache_path()