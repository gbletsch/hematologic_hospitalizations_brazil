import os
from pathlib import Path


if not os.path.exists(os.path.join(Path.home(), 'tcc_hemato_sus')):
    os.mkdir(os.path.join(Path.home(), 'tcc_hemato_sus'))

CACHEPATH = os.path.join(Path.home(), 'tcc_hemato_sus')

for f in ['produced_datasets', 'raw_data']:
    if not os.path.exists(os.path.join(Path(CACHEPATH), f)):
        os.mkdir(os.path.join(Path(CACHEPATH), f))
PRODUCED_DATASETS = os.path.join(Path(CACHEPATH), 'produced_datasets')
RAW_DATA = os.path.join(Path(CACHEPATH), 'raw_data')