from configparser import ConfigParser
from pathlib import Path

try:
    import pyodbc

    pyodbc.pooling = False
    odbcdriver = pyodbc.drivers()[0]
except (ImportError, IndexError):
    odbcdriver = None

from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Text
from sqlalchemy import cast
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy.engine import URL

configfilename = '.dwclibrc'
configfile = Path(f'~/{configfilename}').expanduser()
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

pgdb = create_engine(pguri)
pgmeta = MetaData(bind=pgdb)

patientst = Table('patients', pgmeta, autoload_with=pgdb)
plabelst = Table('patientlabels', pgmeta, autoload_with=pgdb)

p = patientst.c
pl = plabelst.c
plpatientst = select([patientst, pl.numericlabels, pl.wavelabels]).select_from(
    patientst.join(plabelst, cast(pl.patientid, Text) == p.patientid, isouter=True)
)
