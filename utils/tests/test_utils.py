from utils.utils import *
import random
import pandas

def test_make_all_data():
    force_download = 'no'
    df = make_all_dataset(force_download)
    sample_cols = 'DIAG_PRINC ANO_CMPT N_AIH DT_INTER'.split()
    assert random.choice(sample_cols) in df.columns
    assert isinstance(df, pandas.core.frame.DataFrame)