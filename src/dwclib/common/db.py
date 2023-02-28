from configparser import ConfigParser
from os import getenv
from pathlib import Path
from warnings import warn

try:
    import pyodbc

    pyodbc.pooling = False
    odbcdriver = pyodbc.drivers()[0]
except (ImportError, IndexError):
    odbcdriver = None

from sqlalchemy.engine import URL

default_configfile = Path('~/.config/larib/config').expanduser()
configfile = getenv('CONFIGFILE', default_configfile)


if configfile.exists():
    config = ConfigParser()
    config.read(configfile)
    dwcdriver = config.get('dwclib', 'dwc_driver', fallback=odbcdriver)
    if dwcdriver is None:
        warn('No ODBC driver found for DWC', RuntimeWarning)

    dwcuri = URL.create(
        'mssql+pyodbc',
        username=config.get('dwclib', 'dwc_user'),
        password=config.get('dwclib', 'dwc_pass'),
        host=config.get('dwclib', 'dwc_host'),
        database=config.get('dwclib', 'dwc_dbname'),
        query={'driver': dwcdriver},
    )

    pguri = URL.create(
        'postgresql',
        username=config.get('dwclib', 'dwcmeta_user'),
        password=config.get('dwclib', 'dwcmeta_pass'),
        host=config.get('dwclib', 'dwcmeta_host'),
        database=config.get('dwclib', 'dwcmeta_dbname'),
    )
else:
    dwcuri = None
    pguri = None
