import os
from utils import PRODUCED_DATASETS
from utils.IBGETools import pop_ibge
    
    
def test_ibge_download():
    path = os.path.join(PRODUCED_DATASETS, 'estimativa_2013_dou_xls.zip')
    if os.path.exists(path):
        os.remove(path)
    df = pop_ibge()
    assert list(df.columns) == ['SIGLA', 'NOME', '2008', '2009', '2010', '2011', '2012',
                                '2013', '2014', '2015', '2016', '2017', '2018']
    