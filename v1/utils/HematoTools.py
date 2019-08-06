from optimizedf.optimize_df import optimize_df
import numpy as np
import pandas as pd
import os

import dask.dataframe as dd
import pickle

# from pysus.utilities.readdbc import read_dbc
import wget
import random
from zipfile import ZipFile
import re
from dbfread import DBF

import warnings
warnings.filterwarnings('ignore')

from v1.utils import CACHEPATH, RAW_DATA, PRODUCED_DATASETS, ALL_FILES

'''
def _download_rdc_file(filename):
    ''''''
    # Download rdc files from the datasus ftp if it doesn't exist and save locally in .parquet file. 
    ''''''
    if not filename.startswith('RD') and not filename.endswith('.dbc'):
        raise NameError('filename must be a valid SIH SUS .dbc file')

    if filename == 'RDAC0909.dbc':
        return None

    ftp_file = 'ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Dados/' + filename
    parquet_file = filename.split('.')[0] + '.parquet'
    local_file = os.path.join(RAW_DATA, parquet_file)
    if not os.path.exists(local_file):
        wget.download(ftp_file)
        df = read_dbc(filename, encoding='iso-8859-1')
        df.to_parquet(local_file, engine='fastparquet')
        os.unlink(filename)


def _make_all_files_parquet():
    
    df = dd.read_parquet(os.path.join(RAW_DATA, 'RD*.parquet'))

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

    COLS = 'N_AIH UF_ZI DT_INTER DT_SAIDA US_TOT DIAS_PERM ANO_CMPT DIAG_PRINC MORTE IDADE CNES\
            SEXO ETNIA COMPLEX CAR_INT MUNIC_RES MUNIC_MOV NASC'.split()


    df = df[COLS]
    df = df[df['DIAG_PRINC'].isin(LIST_HEMATO)]
    df['ETNIA'] = df['ETNIA'].fillna('0000')
    df = df.compute()
    df.UF_ZI = df.UF_ZI.str[:2]
    df.to_parquet(ALL_FILES)#, skipna=False) # 6 minutos

    
def make_all_dataset(force_download=False):
    ''''''''
    Make all hematologic internations from Brazil in the interval 2008 to 2017.
    When all in cache, takes only about 20" to parse all dataset.
    
    Download from DATASUS ftp when not availiable locally and save each year in a .parquet format.
    It takes about 45' to parse all .dbc files again using all files loccally (force_download='soft'), 
    and ~8 hours to download them (force_download='deep' or first download).
    
    Parameters
    ----------
    force_download: bool, default False
        
        string (optional), default 'no'
        'deep' = Force new download of the data.
        'soft' = use .dbc files already downloaded and make the parquet files again, util to change 
        columns or CIDs, p. e.
        
    Returns
    -------
        pandas.DataFrame
    ''''''
    if force_download and os.path.exists(ALL_FILES):
        os.unlink(ALL_FILES)

    if not os.path.exists(ALL_FILES):
        _download_all_files()
        _make_all_files_parquet()

    df = dd.read_parquet(ALL_FILES).compute()

    for c in ['NASC', 'DT_INTER', 'DT_SAIDA']:
        df[c] = pd.to_datetime(df[c])

    # optimizing
    p_file = os.path.join(PRODUCED_DATASETS, 'optimized_columns.p')
    if not os.path.exists(p_file):
        opt_columns = optimize_df(df)
        pickle.dump(opt_columns, open(p_file, 'wb'))
    opt_columns = pickle.load(open(p_file, 'rb'))
    df = df.astype(opt_columns)
    
    return df
        
        
def _download_all_files():
    initial_year = 8
    final_year = 18

    UFS = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT',
            'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO',
            'RR', 'SC', 'SP', 'SE', 'TO']

    n = 0
    print("{} doesn't exist, fetching files...".format(ALL_FILES.split('/')[-1]))
    for year in range(final_year, initial_year - 1, -1):
        y = str(year).zfill(2)
        print('\nAno: 20{}'.format(y))
        for month in range(1, 13):
            print()
            for uf in UFS:
                m = str(month).zfill(2)
                filename = 'RD{}{}{}.dbc'.format(uf, y, m)
                print('.', end = ' ')
                df = _download_rdc_file(filename)
    print()
'''
    
def download_zip(url, force_download=False, prefix=None):
    '''
    Download TAB_SIH.zip and save in cache.
    
    Returns
    -------
        local path to the downloaded file
    '''
    if prefix != None:
        filename = '{}_'.format(prefix) + wget.detect_filename(url)
        print('filename', filename)
    else:
        filename = wget.detect_filename(url)

    local_file = os.path.join(PRODUCED_DATASETS, filename)
    if not os.path.exists(local_file) or force_download:
        wget.download(url, local_file)
    return local_file


def download_layout(force_download=False):
    '''
    Download the PDF with the layout of the data and save it in the local folder.
    '''
    url = 'ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Doc/IT_SIHSUS_1603.pdf'
    filename = wget.detect_filename(url)
    if not os.path.exists(filename) or force_download:
        wget.download(url, filename)
    

def make_maps(force_download=False):
    
    CID_URL = 'http://www.datasus.gov.br/cid10/V2008/downloads/CID10CSV.zip'
    local_zipfile = download_zip(CID_URL, force_download)
    with ZipFile(local_zipfile) as zfile:
        l = zfile.namelist()
        
        with zfile.open('CID-10-SUBCATEGORIAS.CSV') as cid_file:
            cid_map = pd.read_csv(cid_file, sep=';', encoding='iso-8859-1', usecols=['SUBCAT', 'DESCRICAO'],
                                 index_col='SUBCAT', squeeze=True)

    
    TAB_SIH_URL = 'ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Auxiliar/TAB_SIH.zip'
    local_zipfile = download_zip(TAB_SIH_URL, force_download)
    with ZipFile(local_zipfile) as zfile:
        l = zfile.namelist()
        
        # br_municip.cnv
        with zfile.open('br_municip.cnv') as file:
            mun_map = pd.read_table(file, sep=b'\s{2,}', engine='python', encoding='iso-8859-1',
                                   skiprows=1, header=None, skipfooter=3).applymap(func_decode)
            mun_map[1] = mun_map[1].str.split(' ', 1).str.get(1)
            mun_map = pd.Series(mun_map[1].tolist(), index=mun_map[2].tolist())
            for i, mun in mun_map[mun_map.str.startswith('Município')].items():
                for i2 in i.split(','):
                    mun_map[str(i2).zfill(6)] = mun
                mun_map.drop(i, inplace=True)
            mun_map = mun_map.to_dict()
            for i in ['520000', '529999']:
                mun_map[i] = 'Município ignorado - GO'

            list_brasilia = []
            for i in range(530000, 530009 + 1):
                list_brasilia.append(str(i))
            for i in range(530011, 539999 + 1):
                list_brasilia.append(str(i))
            list_brasilia.append('530010')

            for i in list_brasilia:
                mun_map[i] = 'Brasília'

            list_ign = []
            for i in range(1, 9999):
                list_ign.append(str(i).zfill(6))

            list_ign = list_ign + ['000000', '999999']
            for i in list_ign:
                mun_map[i] = 'Ignorado ou exterior'

        # CARATEND.CNV
        with zfile.open('CARATEND.CNV') as file:
            caratend_map = pd.read_table(file, sep=b'\s{2,}', engine='python', encoding='iso-8859-1',
                                       skiprows=1, header=None).applymap(func_decode)
            caratend_map = { '01': 'Eletivo',
                             '02': 'Urgência',
                             '03': 'Acidente no local trabalho ou a serv da empresa',
                             '04': 'Acidente no trajeto para o trabalho',
                             '05': 'Outros tipo de acidente de trânsito',
                             '06': 'Out tp lesões e envenen por agent quím físicos'}
            #fiz manualmente porque não funcionou o encoding e o decode
    
        
        # COMPLEX2.CNV
        with zfile.open('COMPLEX2.CNV') as file:
            comp_map = pd.read_table(file, sep=b'\s{2,}', engine='python', encoding='iso-8859-1',
                                       skiprows=1, header=None).applymap(func_decode)
            comp_map = {'00': 'Não se aplica',
                        '01': 'Atenção básica',
                        '02': 'Média complexidade',
                        '03': 'Alta complexidade',
                        '99': 'Não se aplica'}
            #fiz manualmente porque não funcionou o encoding e o decode
    
        # FINANC.CNV
        with zfile.open('FINANC.CNV') as file:
            df = pd.read_table(file, sep=b'\s{2,}', engine='python', encoding='iso-8859-1',
                                   skiprows=1, header=None).applymap(func_decode)
            temp = pd.DataFrame([{1:'Não discriminado', 2:'00'}, {1:'Não discriminado', 2:'99'},
                                 {1:'04 Fundo de Ações Estratégicas e Compensações FAEC', 2:'04'}])
            df = pd.concat([df, temp], ignore_index=True).drop(0, axis=1).drop([0, 3])
            financ_map = pd.Series(df[1].tolist(), index=df[2].tolist())
        
        # ETNIA
        with zfile.open('etnia.cnv') as et_file:
            df_et = pd.read_table(et_file, sep=b'\s{2,}', engine='python', encoding='iso-8859-1',
                                   skiprows=1, header=None).applymap(func_decode)
            ni = pd.DataFrame([{1:'NÃO INFORMADO', 2:'0000'}, {1:'NÃO INFORMADO', 2:'9999'}])
            df_et = pd.concat([df_et, ni], ignore_index=True).drop(0, axis=1).drop(0)
            et_map = pd.Series(df_et[1].tolist(), df_et[2].tolist())
    
        # SEX
        with zfile.open('SEXO.CNV') as sex_file:
            df = pd.read_table(sex_file, sep=b'\s{2,}', engine='python',
                               skiprows=1, header=None).applymap(func_decode)
            sex_map = {df.loc[0, 2][0]: df.loc[0, 1],
                       df.loc[0, 2][2]: df.loc[0, 1],
                       df.loc[1, 2]:    df.loc[1, 1],
                       df.loc[2, 2][0]: df.loc[2, 1],
                       df.loc[2, 2][2]: df.loc[2, 1]}

        # UF
        with zfile.open('br_ufsigla.cnv') as uf:
            df = pd.read_table(uf, sep=b'\s{2,}', engine='python',
                               skiprows=1, header=None).applymap(func_decode).dropna(axis=1)
            pe = pd.DataFrame([{1:'PE', 2:'20'}, {1:'PE', 2:'26'}])
            df = pd.concat([df, pe], ignore_index=True)
            df.drop(12, inplace=True)
            uf_map = pd.Series(df[1].tolist(), df[2].tolist())
            
        # CNES
        list_df = []
        list_cnes = [item for item in l if re.search('\S+CNES\S+', item)]
        for file in list_cnes:
            zfile.extract(file)
            dbf = DBF(file, encoding='utf-8')
            cnes_temp = pd.DataFrame(list(dbf))
            list_df.append(cnes_temp)
            os.unlink(file)
        cnes_br = pd.concat(list_df, sort=False).drop_duplicates()
        cnes_br['UF_ZI'] = cnes_br.UF_ZI.map(uf_map)
        cnes_map = pd.Series(cnes_br.NOMEFANT.tolist(), index=cnes_br.CNES.tolist())
    
        # CBO.dbf
        file = 'CBO.dbf'
        zfile.extract(file)
        dbf = DBF(file, encoding='iso-8859-1')
        cbo_map = pd.DataFrame(list(dbf))
        cbo_map = pd.Series(cbo_map.DS_CBO.tolist(), index=cbo_map.CBO.tolist())
        os.unlink(file)
        
    
    
    return sex_map, cnes_map, uf_map, cid_map, et_map, financ_map, cbo_map, comp_map, mun_map, caratend_map


def func_decode(x):
    if type(x) == bytes:
        try:
            return x.decode()
        except UnicodeDecodeError:
            return x.decode(encoding='iso-8859-1')