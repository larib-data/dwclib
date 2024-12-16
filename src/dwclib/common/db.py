import configparser
from pathlib import Path
from warnings import warn
from platformdirs import user_config_dir
from sqlalchemy.engine import URL

config_dir = Path(user_config_dir(appname="larib-data", appauthor="larib-data"))
configfile = config_dir / "config.ini"
dwcuri = None
pguri = None

if configfile.exists():
    config = configparser.ConfigParser()
    config.read(configfile)

    try:
        dwcuri = URL.create(
            "mssql+pymssql",
            username=config.get("dwc", "user"),
            password=config.get("dwc", "pass"),
            host=config.get("dwc", "host"),
            database=config.get("dwc", "dbname"),
        )
    except configparser.NoOptionError:
        warn(f"No valid DWC configuration in {configfile}.", RuntimeWarning)

    try:
        pguri = URL.create(
            "postgresql+psycopg",
            username=config.get("dwcmeta", "user"),
            password=config.get("dwcmeta", "pass"),
            host=config.get("dwcmeta", "host"),
            database=config.get("dwcmeta", "dbname"),
        )
    except configparser.NoOptionError:
        warn(f"No valid DWCmeta configuration in {configfile}.", RuntimeWarning)
else:
    warn(f"Configuration file {configfile} does not exist.", RuntimeWarning)
