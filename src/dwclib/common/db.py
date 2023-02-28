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

config_dir = Path(user_config_dir(appname="dwclib", appauthor="larib-data"))
config_dir.mkdir(parents=True, exist_ok=True)
configfile = config_dir / "config.ini"


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
        query={
            "driver": dwcdriver,
            "Encrypt": "no",
        },
    )

    pguri = URL.create(
        'postgresql',
        username=config.get('dwclib', 'dwcmeta_user'),
        password=config.get('dwclib', 'dwcmeta_pass'),
        host=config.get('dwclib', 'dwcmeta_host'),
        database=config.get('dwclib', 'dwcmeta_dbname'),
    )
else:
    warn(f"Configuration file {configfile} does not exist.", RuntimeWarning)
    dwcuri = None
    pguri = None
