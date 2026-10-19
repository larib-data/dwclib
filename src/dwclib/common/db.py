import configparser
from pathlib import Path
from warnings import warn

from platformdirs import user_config_dir
from sqlalchemy.engine import URL

dwcuri: URL | None = None
pguri: URL | None = None
config_dir = Path(
    user_config_dir(appname="larib-data", appauthor="larib-data", roaming=True)
)
configfile = config_dir / "config.ini"


def update_config(newconfig):
    config = configparser.ConfigParser()
    config.read_string(newconfig)
    configfile.parent.mkdir(parents=True, exist_ok=True)
    with open(configfile, "w") as fd:
        config.write(fd)
    load_config()


def load_config():
    global dwcuri, pguri
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
    except configparser.Error:
        warn(f"No valid DWC configuration in {configfile}.", RuntimeWarning)

    try:
        pguri = URL.create(
            "postgresql+psycopg",
            username=config.get("dwcmeta", "user"),
            password=config.get("dwcmeta", "pass"),
            host=config.get("dwcmeta", "host"),
            database=config.get("dwcmeta", "dbname"),
        )
    except configparser.Error:
        warn(f"No valid DWCmeta configuration in {configfile}.", RuntimeWarning)


def get_dwcuri() -> URL:
    """Return the DWC (MSSQL) URI from the config, read at call time."""
    if dwcuri is None:
        raise RuntimeError(f"No valid DWC configuration in {configfile}.")
    return dwcuri


def get_pguri() -> URL:
    """Return the DWCmeta (Postgres) URI from the config, read at call time."""
    if pguri is None:
        raise RuntimeError(f"No valid DWCmeta configuration in {configfile}.")
    return pguri


if configfile.exists():
    load_config()
else:
    warn(f"Configuration file {configfile} does not exist.", RuntimeWarning)
