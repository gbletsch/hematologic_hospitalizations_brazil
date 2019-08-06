import pandas as pd

from pysus.online_data import SIH
from optimizedf.optimize_df import optimize_df

import time
import logging
import itertools
import wget

import numpy as np
import os

import pickle

import random
from zipfile import ZipFile
import re
from dbfread import DBF

import warnings
warnings.filterwarnings('ignore')

#import seaborn as sns
#import matplotlib.pyplot as plt
#%matplotlib inline
#plt.style.use('seaborn')


PRODUCED_DATASETS = 'files'


LIST_CID = ['D46', 'D461', 'D462', 'D463', 'D464', 'D467', 'D469', 
            'C81', 'C810', 'C811', 'C812', 'C813', 'C817', 'C819', 
            'B212', 
            'C82', 'C820', 'C821', 'C822', 'C827', 'C829',
            'C83', 'C830', 'C831', 'C832', 'C833', 'C834', 'C835', 'C836', 'C837', 'C838', 'C839',
            'C84', 'C840', 'C841', 'C842', 'C843', 'C844', 'C845',
            'C85', 'C850', 'C851', 'C857', 'C859',
            'C88', 'C880', 'C881', 'C882', 'C883', 'C887', 'C889', 'C963',
            'C91', 'C910', 'C911', 'C912', 'C913', 'C914', 'C915', 'C917', 'C919',
            'C92', 'C920', 'C921', 'C922', 'C923', 'C924', 'C927', 'C929',
            'C93', 'C930', 'C931', 'C932', 'C937', 'C939',
            'C94', 'C940', 'C941', 'C942', 'C943', 'C944', 'C945', 'C947',
            'C95', 'C950', 'C951', 'C952', 'C957', 'C959',
            'C90', 'C900', 'C901', 'C902']

months = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']

ufs = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT',
          'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO',
          'RR', 'SC', 'SP', 'SE', 'TO']

COLS = 'N_AIH UF_ZI DT_INTER DT_SAIDA US_TOT DIAS_PERM ANO_CMPT DIAG_PRINC MORTE IDADE CNES\
        SEXO ETNIA COMPLEX CAR_INT MUNIC_RES MUNIC_MOV NASC'.split()

def download_all(job=None):
    tic = time.time()
    for i in itertools.product(range(2008, 2018), ufs, range(1, 13)):
        year, uf, month = i
        try:
            clear_output()
            display(('Downloading', uf, month, year))
            display(('Time elapsed', time.time() - tic))
            df = SIH.download(uf, year, month)
            job(df)
        except Exception as e:
            make_log(e, uf, month, year)

def make_log(e, uf, month, year):
    logging.basicConfig(filename='errors.log',level=logging.DEBUG)
    logging.time.localtime()
    logging.exception(e)
    logging.debug('Error on {} {} {}'.format(uf, month, year))
    

def make_hemato_ds():
    tic = time.time()
    all_df = pd.DataFrame()
    
    for i in itertools.product(range(2008, 2018), ufs, range(1, 13)):
        year, uf, month = i
        try:
            clear_output(wait=True)
            display(('processing', uf, month, year))
            display(('Time elapsed', time.time() - tic))
            df = SIH.download(uf, year, month)
            df = df[COLS]
            df = df[df['DIAG_PRINC'].isin(LIST_CID)]
            df['ETNIA'] = df['ETNIA'].fillna('0000')
            df.UF_ZI = df.UF_ZI.str[:2]
            all_df = all_df.append(df, ignore_index=True)
        except Exception as e:
            make_log(e, uf, month, year)
    
    all_df.to_parquet('all_hemato.parquet')


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
        
        
def pop_2010(force_download=False):
    '''
    I didn't find the 2010's data, had to make the dataset by myself.
    '''
    parent_path = 'ftp://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/resultados/'
    list_files = ["total_populacao_acre.zip","total_populacao_alagoas.zip","total_populacao_amapa.zip",
                "total_populacao_amazonas.zip", "total_populacao_bahia.zip", "total_populacao_ceara.zip", 
                "total_populacao_distrito_federal.zip", "total_populacao_espirito_santo.zip", 
                "total_populacao_goias.zip", "total_populacao_maranhao.zip","total_populacao_mato_grosso.zip",
                "total_populacao_mato_grosso_do_sul.zip","total_populacao_minas_gerais.zip",
                "total_populacao_para.zip",
                "total_populacao_paraiba.zip","total_populacao_parana.zip","total_populacao_pernambuco.zip",
                "total_populacao_piaui.zip","total_populacao_rio_de_janeiro.zip",
                "total_populacao_rio_grande_do_norte.zip",
                "total_populacao_rio_grande_do_sul.zip","total_populacao_rondonia.zip",
                "total_populacao_roraima.zip",
                "total_populacao_santa_catarina.zip","total_populacao_sao_paulo.zip",
                "total_populacao_sergipe.zip",
                "total_populacao_tocantins.zip"]
    states = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT',
              'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO',
              'RR', 'SC', 'SP', 'SE', 'TO']

    filename_2010 = '2010_UF_Municipio.parquet'
    local_file_2010 = os.path.join(PRODUCED_DATASETS, filename_2010)
    
    if not os.path.exists(local_file_2010):
        list_df = []
        for file, uf in zip(list_files, states):
            path = parent_path + file
            local_path = os.path.join(PRODUCED_DATASETS, file)
            if not os.path.exists(local_path):
                wget.download(path, local_path)
            with ZipFile(local_path) as zfile:
                file = zfile.namelist()[0]
                with zfile.open(file) as xls_file:
                    temp_df = pd.read_excel(xls_file)
                    temp_df = temp_df[['Nome do município', 'Total da população 2010']]
                    temp_df.columns = ['NOME', '2010']
                    temp_df.NOME = temp_df.NOME + ' ({})'.format(uf)
                    temp_df['SIGLA'] = uf
                    list_df.append(temp_df)
        df = pd.concat(list_df, sort=False, ignore_index=True).dropna()
        df.to_parquet(local_file_2010)
    else:
        df = pd.read_parquet(local_file_2010)
    
    df['2010'] = df['2010'].astype(int)
    return df[['SIGLA', 'NOME', '2010']]

def _2008(year=2008):
    path = 'ftp://ftp.ibge.gov.br/Estimativas_de_Populacao/Estimativas_{}/UF_Municipio.zip'.format(year)
    
    local_path = os.path.join(PRODUCED_DATASETS, '{}_UF_Municipio.zip'.format(year))
    if not os.path.exists(local_path):
        wget.download(path, local_path)
    with ZipFile(local_path) as zfile:
        file = zfile.namelist()[0]
        with zfile.open(file) as xls_file:
            pop = pd.read_excel(xls_file, skiprows=4).dropna()
            pop.columns = ['SIGLA', 'COD', 'COD.1', 'NOME', '{}'.format(year)]
            pop.drop(['COD', 'COD.1'], axis=1, inplace=True)
            pop.NOME = pop.NOME + ' (' + pop.SIGLA + ')'
    if year != 2009:
        pop['{}'.format(year)] = pop['{}'.format(year)].astype(int)
    return pop

def _2009(year=2009):
    df = _2008(year)
    df.drop(289, inplace=True)
    df['2009'] = df['2009'].astype(int)
    return df

def _2010(year=2010):
    return pop_2010()

def _2011(year=2011):
    return _2008(year)

def _2012(year=2012,
          path = 'ftp://ftp.ibge.gov.br/Estimativas_de_Populacao/Estimativas_2012/',
          filename = 'estimativa_2012_DOU_28_08_2012_xls.zip',
          skip=2):
    
    local_path = os.path.join(PRODUCED_DATASETS, filename)
    if not os.path.exists(local_path):
        wget.download(path + filename, local_path)
    
    with ZipFile(local_path) as zfile:
        file = zfile.namelist()[0]
        with zfile.open(file) as xls_file:
            df = pd.read_excel(xls_file, skiprows=skip, encoding='utf-8').dropna()
            df.columns = ['SIGLA', 'COD', 'COD.1', 'NOME', '{}'.format(year)]
            df.drop(['COD', 'COD.1'], axis=1, inplace=True)
            df.NOME = df.NOME + ' (' + df.SIGLA + ')'
    
    if year == 2012:
        year = '{}'.format(2012)
        mask = df[year].str.startswith('(*)', na=False)
        df.loc[mask, year] = df.loc[mask, year].str[3:]#.replace('.', '')
        df.loc[mask, year] = df.loc[mask, year].str.replace('.', '')
        df[year] = df[year].astype(int)
    return df

        
def _2013(year=2013):
    path = 'ftp://ftp.ibge.gov.br/Estimativas_de_Populacao/Estimativas_2013/'
    filename = 'estimativa_2013_dou_xls.zip'
    df = _2012(2013, path, filename)
    df['2013'] = df['2013'].astype(int)
    return df


def _2014(year=2014):
    path = 'ftp://ftp.ibge.gov.br/Estimativas_de_Populacao/Estimativas_2014/'
    filename = 'estimativa_dou_2014_xls.zip'
    
    local_path = os.path.join(PRODUCED_DATASETS, filename)
    if not os.path.exists(local_path):
        wget.download(path + filename, local_path)

    with ZipFile(local_path) as zfile:
        file = zfile.namelist()[0]
        with zfile.open(file) as xls_file:
            pop = pd.read_excel(xls_file, sheet_name='Municípios', skiprows=2).dropna()
            pop.columns = ['SIGLA', 'COD', 'COD.1', 'NOME', '{}'.format(year)]
            pop.drop(['COD', 'COD.1'], axis=1, inplace=True)
            pop.NOME = pop.NOME + ' (' + pop.SIGLA + ')'
    
    pop.loc[210, '2014'] = 41487
    pop['2014'] = pop['2014'].astype(int)
    return pop

        
def _2015(year=2015,
          path = 'ftp://ftp.ibge.gov.br/Estimativas_de_Populacao/Estimativas_2015/',
          filename = 'estimativa_dou_2015_20150915.xls',
          skip=2):
    
    local_path = os.path.join(PRODUCED_DATASETS, filename)
    if not os.path.exists(local_path):
        wget.download(path + filename, local_path)
    pop = pd.read_excel(local_path, sheet_name='Municípios', skiprows=skip).dropna()
    pop.columns = ['SIGLA', 'COD', 'COD.1', 'NOME', '{}'.format(year)]
    pop.drop(['COD', 'COD.1'], axis=1, inplace=True)
    pop.NOME = pop.NOME + ' (' + pop.SIGLA + ')'

    y = '{}'.format(year)
    pop.loc[210, y] = 41487
    
    if year == 2015:
        pop[y] = pop[y].astype(int)
    
    return pop

def _2016(year=2016):
    path = 'ftp://ftp.ibge.gov.br/Estimativas_de_Populacao/Estimativas_2016/'
    filename = 'estimativa_dou_2016_20160913.xlsx'
    df = _2015(2016, path, filename)
    mask = df['2016'].str.endswith('(2)', na=False)
    df.loc[mask, '2016'] = df[mask]['2016'].str[:-3]
    df['2016'] = df['2016'].astype(int)
    return df

def _2017(year=2017):
    path = 'ftp://ftp.ibge.gov.br/Estimativas_de_Populacao/Estimativas_2017/'
    filename = 'estimativa_dou_2017.xls'
    df = _2015(2017, path, filename, 1)
    for end in ['(1)', '(3)', '(4)', '(5)']:
        mask = df['2017'].str.endswith(end, na=False)
        df.loc[mask, '2017'] = df.loc[mask, '2017'].str[:-3]
    df['2017'] = df['2017'].astype(int)
    return df

def _2018(year=2018):
    path = 'ftp://ftp.ibge.gov.br/Estimativas_de_Populacao/Estimativas_2018/'
    filename = 'estimativa_dou_2018_20181019.xls'
    df = _2015(2018, path, filename, 1)
    year = '{}'.format(year)
    for end in range(1, 15):
        mask = df[year].str.endswith('({})'.format(end), na=False)
        df.loc[mask, year] = df.loc[mask, year].str[:-4]
        df.loc[mask, year] = df.loc[mask, year].str.replace('.', '')
    df[year] = df[year].astype(int)
    return df



def pop_ibge():
    df = _2008().merge(_2009())
    df = df.merge(_2010())
    df = df.merge(_2011())
    df = df.merge(_2012())
    df = df.merge(_2013())
    df = df.merge(_2014())
    df = df.merge(_2015())
    df = df.merge(_2016())
    df = df.merge(_2017())
    df = df.merge(_2018())
    df.drop([289, 3220, 3809, 5385], inplace=True)
    return df

def open_hemato_df():
    hemato = pd.read_parquet('files/all_hemato.parquet')
    opt_cols = optimize_df(hemato)
    hemato = hemato.astype(opt_cols)
    return hemato
