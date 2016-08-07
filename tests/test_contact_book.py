# encoding=utf-8
# Author: ninadpage

import unittest
import logging
import logging.config
import sys
import os

import models
from db import db_init, fast_trie_lookup, ContactBookDB
from exceptions import NoSuchObjectFound

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
        'cb_test_logger': {
            'handlers': ['stdout'],
            'level': 'DEBUG',
        },
    },
}

logging.config.dictConfig(logging_config)
logger = logging.getLogger('cb_test_logger')


class TestContactBook(unittest.TestCase):

    TEST_DB_PATH = 'test.db'

    def setUp(self):
        db_init(db_logger=logger, sqlite_db_path=self.TEST_DB_PATH)

    def test_trie_initialization(self):
        # Prepare initial state
        cb = ContactBookDB()
        p1 = cb.create_person(first_name='Abc', last_name='Def')
        p2 = cb.create_person(first_name='Tuv', last_name='Xyz')

        # Reinitialize contact book
        fast_trie_lookup.trie.clear()
        db_init(db_logger=logger, sqlite_db_path=self.TEST_DB_PATH)

        # Test if trie is initialized properly
        r1 = cb.find_person_details_by_prefix('')
        self.assertEqual(len(r1), 2)

        r2 = cb.find_person_details_by_prefix('ab')
        self.assertEqual(len(r2), 1)
        self.assertEqual(r2[0].id, p1.id)

        r3 = cb.find_person_details_by_prefix('xyz')
        self.assertEqual(len(r3), 1)
        self.assertEqual(r3[0].id, p2.id)

    def test_create_person(self):
        cb = ContactBookDB()

        p1 = cb.create_person(first_name='Abc', last_name='Def')
        self.assertIsInstance(p1, models.Person)
        self.assertEqual(p1.first_name, 'Abc')
        self.assertEqual(p1.last_name, 'Def')
        self.assertEqual(p1.middle_name, None)
        self.assertEqual(p1.groups, [])
        self.assertEqual(p1.phone_numbers, [])

        g1 = cb.create_group('G1')
        p2 = cb.create_person(first_name='Tuv', last_name='Xyz', phone_number='+31600012345', phone_label='Mobile',
                              email_address='abc@example.com', email_label='Personal', group_id=g1.id)
        self.assertIsInstance(p2, models.Person)

        self.assertEqual(len(p2.phone_numbers), 1)
        self.assertEqual((p2.phone_numbers[0].phone, p2.phone_numbers[0].label), ('+31600012345', 'Mobile'))

        self.assertEqual(len(p2.email_addresses), 1)
        self.assertEqual((p2.email_addresses[0].email, p2.email_addresses[0].label), ('abc@example.com', 'Personal'))

        self.assertEqual(len(p2.groups), 1)
        self.assertEqual((p2.groups[0].id, p2.groups[0].name), (g1.id, g1.name))

        self.assertEqual(len(p2.addresses), 0)

        nonexistant_group_id = g1.id + 2
        with self.assertRaises(NoSuchObjectFound):
            cb.create_person(first_name='Tuv', last_name='Xyz', group_id=nonexistant_group_id)

    def tearDown(self):
        fast_trie_lookup.trie.clear()
        if os.path.exists(self.TEST_DB_PATH):
            os.remove(self.TEST_DB_PATH)


if __name__ == '__main__':
    unittest.main()
