import wget
import os
from utils import PRODUCED_DATASETS
from zipfile import ZipFile
import time

import pandas as pd

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