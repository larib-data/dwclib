from configparser import ConfigParser
from pathlib import Path
from warnings import warn

from platformdirs import user_config_dir

try:
    import pyodbc

    pyodbc.pooling = False
    odbcdriver = pyodbc.drivers()[0]
except (ImportError, IndexError):
    odbcdriver = None

from sqlalchemy.engine import URL

config_dir = Path(user_config_dir(appname="larib-data", appauthor="larib-data"))
configfile = config_dir / "config.ini"


if configfile.exists():
    config = ConfigParser()
    config.read(configfile)
    dwcdriver = config.get('dwc', 'driver', fallback=odbcdriver)
    if dwcdriver is None:
        warn('No ODBC driver found for DWC', RuntimeWarning)

    dwcuri = URL.create(
        'mssql+pyodbc',
        username=config.get('dwc', 'user'),
        password=config.get('dwc', 'pass'),
        host=config.get('dwc', 'host'),
        database=config.get('dwc', 'dbname'),
        query={
            "driver": dwcdriver,
            "Encrypt": "no",
        },
    )

    pguri = URL.create(
        'postgresql',
        username=config.get('dwcmeta', 'user'),
        password=config.get('dwcmeta', 'pass'),
        host=config.get('dwcmeta', 'host'),
        database=config.get('dwcmeta', 'dbname'),
    )
else:
    warn(f"Configuration file {configfile} does not exist.", RuntimeWarning)
    dwcuri = None
    pguri = None
