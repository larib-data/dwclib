from configparser import ConfigParser
from pathlib import Path

try:
    import pyodbc

    pyodbc.pooling = False
    odbcdriver = pyodbc.drivers()[0]
except (ImportError, IndexError):
    odbcdriver = None

from sqlalchemy.engine import URL

configfilename = '.dwclibrc'
configfile = Path(f'~/{configfilename}').expanduser()

if configfile.exists():
    config = ConfigParser()
    config.read(configfile)

    dwcuri = URL.create(
        'mssql+pyodbc',
        username=config.get('dwc', 'user'),
        password=config.get('dwc', 'pass'),
        host=config.get('dwc', 'host'),
        database=config.get('dwc', 'dbname'),
        query={'driver': config.get('dwc', 'driver', fallback=odbcdriver)},
    )

    pguri = URL.create(
        'postgresql',
        username=config.get('dwcmeta', 'user'),
        password=config.get('dwcmeta', 'pass'),
        host=config.get('dwcmeta', 'host'),
        database=config.get('dwcmeta', 'dbname'),
    )
else:
    dwcuri = None
    pguri = None
