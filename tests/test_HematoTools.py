import os
from utils.HematoTools import download_rdc_file, _make_hemato_df, \
                                make_all_dataset, download_zip, download_layout, \
                                make_maps, func_decode
import pandas as pd
import random


from utils import RAW_DATA, PRODUCED_DATASETS#, CACHEPATH

def test_func_decode():
    assert type(func_decode(b'abcde')) == str

def test_download_zip():
    local_file = os.path.join(PRODUCED_DATASETS, 'CID10CSV.zip')
    if os.path.exists(local_file):
        os.unlink(local_file)
    result = download_zip('http://www.datasus.gov.br/cid10/V2008/downloads/CID10CSV.zip')
    assert local_file == result

def test_make_maps():
    local_cid = os.path.join(PRODUCED_DATASETS, 'CID10CSV.zip')
    local_tab = os.path.join(PRODUCED_DATASETS, 'TAB_SIH.zip')
    if os.path.exists(local_cid):
        os.unlink(local_cid)
    if os.path.exists(local_tab):
        os.unlink(local_tab)
    sex_map, cnes_map, uf_map, cid_map, et_map, financ_map, cbo_map, comp_map, mun_map = make_maps()
    assert uf_map['16'] == 'AP'
    assert len(cid_map) == 12451
    assert len(et_map) == 266
    assert len(financ_map) == 8
    assert len(cbo_map) == 2651
    assert len(comp_map) == 5
    assert len(mun_map) == 25625
    

def test_download_layout():
    if os.path.exists('IT_SIHSUS_1603.pdf'):
        os.unlink('IT_SIHSUS_1603.pdf')
    download_layout()
    assert os.path.exists('IT_SIHSUS_1603.pdf')

    
def test_download_rdc_file():
    filename = 'RDAC0808.dbc'
    parquet_file = filename.split('.')[0] + '.parquet'
    local_file = os.path.join(RAW_DATA, parquet_file)
    if os.path.exists(local_file):
        os.unlink(local_file)
    download_rdc_file(filename)
    assert(os.path.exists(local_file))
    
def test_make_hemato_df():
    df = _make_hemato_df('AC', 2008, 8)
    assert type(df) == pd.DataFrame
    
def test_make_all_dataset(test=random.choice(['soft', 'hard'])):
    if test == 'hard':
        all_file = os.path.join(PRODUCED_DATASETS, 'all_hemato_df.parquet')
        if os.path.exists(all_file):
            os.unlink(all_file)
        parquet_file = os.path.join(PRODUCED_DATASETS, 'hemato_2017.parquet')
        if os.path.exists(parquet_file):
            os.unlink(parquet_file)
        rd_file = os.path.join(RAW_DATA, 'RDAC1703.parquet')
        if os.path.exists(rd_file):
            os.unlink(rd_file)
    df1 = make_all_dataset()
    cols = 'N_AIH DT_INTER DT_SAIDA US_TOT DIAS_PERM ANO_CMPT DIAG_PRINC MORTE IDADE CNES\
            UF SEXO ETNIA COMPLEX CAR_INT MUNIC_RES MUNIC_MOV'.split()

    assert type(df1) == pd.DataFrame
    assert cols == df1.columns
    

