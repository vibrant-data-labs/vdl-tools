from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy_utils import create_database, database_exists, drop_database

from vdl_tools.shared_tools.tools.config_utils import get_configuration
from vdl_tools.shared_tools.database_cache.database_models.base import Base

from vdl_tools.shared_tools.tools.config_utils import get_configuration
config = get_configuration()


CONN_PARAMS = dict(
    host=config["postgres"]["host"],
    port=config["postgres"]["port"],
    user=config["postgres"]["user"],
    password=config["postgres"]["password"],
)


def get_url(
    host,
    port,
    user,
    password,
    database,
):
    return URL.create(
        'postgresql',
        username=user,
        password=password,
        port=port,
        host=host,
        database=database
    )


def create_engine_from_cfg(config=None):
    config = config or get_configuration()
    url = get_url(
        host=config["postgres"]["host"],
        port=config["postgres"]["port"],
        user=config["postgres"]["user"],
        password=config["postgres"]["password"],
        database=config["postgres"]["database"],
    )
    return create_engine(url)

engine = create_engine_from_cfg()
Session = sessionmaker(bind=engine)



def _create_db(
    host,
    port,
    user,
    password,
    database,
):
    db_url = get_url(host, port, user, password, database)
    if database_exists(db_url):
        drop_database(db_url)
    create_database(db_url)


def recreate_db(config=None):
    config = config or get_configuration()
    _create_db(
        host=config["postgres"]["host"],
        port=config["postgres"]["port"],
        user=config["postgres"]["user"],
        password=config["postgres"]["password"],
        database=config["postgres"]["database"],
    )
    Base.metadata.create_all(bind=engine)


@contextmanager
def get_session(config=None, session=None):
    """Provide a transactional scope around a series of operations."""
    config = config or get_configuration()

    if not session:
        session = Session(bind=create_engine_from_cfg(config))
    try:
        yield session

        session.commit()
    except Exception as ex:
        print(ex)
        session.rollback()
        raise
    finally:
        session.close()
