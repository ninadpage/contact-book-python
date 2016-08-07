# encoding=utf-8
# Author: ninadpage

import logging
import logging.config
import sys

from .db import db_init as _db_init
from .db import ContactBookDB


__all__ = ['__version__', 'init_contactbook', 'ContactBookDB']

__version__ = '0.0.1'


def init_contactbook(*, sqlite_db_path=None, db_connection_string=None, logger=None):
    """
    Initializes Contact Book library (database connections, logging, etc).
    All parameters must be names explicitly. Only one of sqlite_db_path and db_connection_string must be provided.

    If the sqlite database given by the path doesn't exist, it is created. The path can be relative or absolute,
    in which case it must start with a /.

    If you want to use any other database engine, you can specify appropriate db_connection_string.
    Do not use an existing database, you might lose existing tables!

    If logger is not provided, it creates a logger which emits to stdout at DEBUG level.

    :param sqlite_db_path: Path to sqlite database file
    :type sqlite_db_path: str
    :param db_connection_string: A SQLAlchemy Database URL (see
                                 http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls)
    :type db_connection_string: str
    :param logger: Logger object which will be used by this package for all logging
    :type logger: logging.Logger
    :return: None
    """

    if logger:
        cb_logger = logger
    else:
        # Create a logger which emits to stdout, with log level DEBUG
        logging_config = {
            'version': 1,
            'formatters': {
                'extended': {
                    'format': '[%(asctime)s] [%(name)s] [%(levelname)s]: %(message)s',
                },
            },
            'handlers': {
                'stdout': {
                    'level': 'DEBUG',
                    'class': 'logging.StreamHandler',
                    'formatter': 'extended',
                    'stream': sys.stdout,
                },
            },
            'loggers': {
                'cb_logger': {
                    'handlers': ['stdout'],
                    'level': 'DEBUG',
                },
            },
        }

        logging.config.dictConfig(logging_config)
        cb_logger = logging.getLogger('cb_logger')

    _db_init(db_logger=cb_logger, sqlite_db_path=sqlite_db_path, db_connection_string=db_connection_string)
