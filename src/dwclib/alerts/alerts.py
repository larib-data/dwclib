from functools import lru_cache
from importlib import resources

import pandas as pd

from dwclib import assets


@lru_cache
def load_rosetta_data():
    cols = [
        'Group',
        'REFID',
        'DisplayName',
        'Description',
        'Vendor_VMD',
        'Vendor_UOM',
        'UOM_MDC',
        'UOM_UCUM',
        'Vendor_ID',
        'CF_CODE10',
    ]
    dtypes = {c: 'string' for c in cols}
    dtypes['CF_CODE10'] = 'Int64'

    with resources.open_text(assets, 'rosetta_terms.csv') as fd:
        df = pd.read_csv(fd, usecols=cols, index_col='CF_CODE10', dtype=dtypes)
    df = df[df['Vendor_ID'] == 'Philips']
    df = df.drop(columns=['Vendor_ID'])
    df = df.loc[df.index.dropna()]
    df = df.loc[~df.index.duplicated()]
    return df
