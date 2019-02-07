from utils.utils import *
import random
import pandas
import os
from utils import PRODUCED_DATASETS
from utils.IBGETools import *

def test_make_all_data():
    force_download = 'no'
    df = make_all_dataset(force_download)
    sample_cols = 'DIAG_PRINC ANO_CMPT N_AIH DT_INTER'.split()
    assert random.choice(sample_cols) in df.columns
    assert isinstance(df, pandas.core.frame.DataFrame)
    
def test_download():
    URL = 'ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Auxiliar/TAB_SIH.zip'
    filename = wget.detect_filename(URL)
    local_file = os.path.join(PRODUCED_DATASETS, filename)
    returned = download_TAB_SIH_zip(force_download=True)
    assert os.path.exists(local_folder)
    assert local_file == returned
    
def test_read_zip():
    lf = download_TAB_SIH_zip()
    assert len(read_zipfile(lf)) == 241
    
def test_ibge_download():
    path = os.path.join(PRODUCED_DATASETS, 'estimativa_2013_dou_xls.zip')
    if os.path.exists(path):
        os.remove(path)
    df = pop_ibge()
    assert list(df.columns) == ['SIGLA', 'NOME', '2008', '2009', '2010', '2011', '2012',
                                '2013', '2014', '2015', '2016', '2017', '2018']
    