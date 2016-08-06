# encoding=utf-8
# Author: ninadpage

import unittest
import logging
import logging.config
import sys
import os


from src.db import db_init


class TestContactBook(unittest.TestCase):

    TEST_DB_PATH = '../test.db'

    def setUp(self):
        self.tearDown()

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

        db_init(db_logger=cb_logger, sqlite_db_path=self.TEST_DB_PATH)

    def test_success(self):
        self.assertEqual(1, 1)

    def tearDown(self):
        if os.path.exists(self.TEST_DB_PATH):
            os.remove(self.TEST_DB_PATH)


if __name__ == '__main__':
    unittest.main()
