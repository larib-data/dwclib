from configparser import ConfigParser
from pathlib import Path

from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Text
from sqlalchemy import cast
from sqlalchemy import column
from sqlalchemy import create_engine
from sqlalchemy import select

configfilename = '.dwclibrc'
configfile = Path(f'~/{configfilename}').expanduser()
config = ConfigParser()
config.read(configfile)

pguri = config.get('db', 'pguri', fallback=None)
assert pguri is not None
dwcuri = config.get('db', 'dwcuri', fallback=None)
assert dwcuri is not None

# db = create_engine(uri, echo=True)
pgdb = create_engine(pguri)
pgmeta = MetaData(bind=pgdb)

# dwcdb = create_engine(dwcuri)
# dwcmeta = MetaData(bind=dwcdb)

patientst = Table('patients', pgmeta, autoload_with=pgdb)
plabelst = Table('patientlabels', pgmeta, autoload_with=pgdb)

p = patientst.c
pl = plabelst.c
plpatientst = select([patientst, pl.numericlabels, pl.wavelabels]).select_from(
    patientst.join(
        plabelst, cast(pl.patientid, Text) == p.patientid, isouter=True
    )
)
