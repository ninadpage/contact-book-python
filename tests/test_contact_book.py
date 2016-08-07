# encoding=utf-8
# Author: ninadpage

import unittest
import logging
import logging.config
import sys
import os

from contactbook import init_contactbook, ContactBookDB
from contactbook import models
from contactbook.db import fast_trie_lookup
from contactbook.exceptions import NoSuchObjectFound


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
        init_contactbook(sqlite_db_path=self.TEST_DB_PATH, logger=logger)

    def test_trie_initialization(self):
        # Prepare initial state
        cb = ContactBookDB()
        p1 = cb.create_person(first_name='Abc', last_name='Def')
        p2 = cb.create_person(first_name='Tuv', last_name='Xyz')

        # Reinitialize contact book
        fast_trie_lookup.trie.clear()
        init_contactbook(sqlite_db_path=self.TEST_DB_PATH, logger=logger)

        # Test if trie is initialized properly
        r1 = cb.find_person_details_by_name('')
        self.assertEqual(len(r1), 2)

        r2 = cb.find_person_details_by_name('ab')
        self.assertEqual(len(r2), 1)
        self.assertEqual(r2[0].id, p1.id)

        r3 = cb.find_person_details_by_name('xyz')
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

        with self.assertRaises(ValueError):
            cb.create_person()

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

    def test_groups(self):
        cb = ContactBookDB()
        g1 = cb.create_group('G1')
        g2 = cb.create_group('G2')
        self.assertIsInstance(g1, models.Group)
        self.assertIsInstance(g2, models.Group)

        p1 = cb.create_person(first_name='P1', group_id=g1.id)
        p2 = cb.create_person(first_name='P2', group_id=g1.id)
        p3 = cb.create_person(first_name='P3')
        cb.add_group_to_person(p3.id, g1.id)
        p4 = cb.create_person(first_name='P4', group_id=g2.id)
        p5 = cb.create_person(first_name='P5')
        cb.add_group_to_person(p5.id, g2.id)
        p6 = cb.create_person(first_name='P6', group_id=g1.id)
        cb.add_group_to_person(p6.id, g2.id)

        self.assertEqual(len(p1.groups), 1)
        self.assertEqual(len(p4.groups), 1)
        self.assertEqual(len(p6.groups), 2)

        self.assertEqual(p1.groups[0].id, g1.id)
        self.assertEqual(p2.groups[0].id, g1.id)
        self.assertEqual(p3.groups[0].id, g1.id)
        self.assertEqual(p4.groups[0].id, g2.id)
        self.assertEqual(p5.groups[0].id, g2.id)
        self.assertEqual(p6.groups[0].id, g1.id)
        self.assertEqual(p6.groups[1].id, g2.id)

    def test_get_all_persons(self):
        cb = ContactBookDB()
        g1 = cb.create_group('G1')
        g2 = cb.create_group('G2')

        p1 = cb.create_person(first_name='P1', group_id=g1.id)
        p2 = cb.create_person(first_name='P2', group_id=g1.id)
        p3 = cb.create_person(first_name='P3')
        cb.add_group_to_person(p3.id, g1.id)
        p4 = cb.create_person(first_name='P4', group_id=g2.id)
        p5 = cb.create_person(first_name='P5')
        cb.add_group_to_person(p5.id, g2.id)
        p6 = cb.create_person(first_name='P6', group_id=g1.id)
        cb.add_group_to_person(p6.id, g2.id)

        res = cb.get_all_persons()
        self.assertEqual(len(res), 6)
        self.assertEqual(set(map(lambda p: p.id, res)), {p1.id, p2.id, p3.id, p4.id, p5.id, p6.id})

        res = cb.get_all_persons(group_id=g1.id)
        self.assertEqual(len(res), 4)
        self.assertEqual(set(map(lambda p: p.id, res)), {p1.id, p2.id, p3.id, p6.id})

        res = cb.get_all_persons(g2.id)
        self.assertEqual(len(res), 3)
        self.assertSetEqual(set(map(lambda p: p.id, res)), {p4.id, p5.id, p6.id})

    def test_add_fields(self):
        cb = ContactBookDB()
        p = cb.create_person(first_name='P', last_name='L')

        ph1 = cb.add_phone_number(p.id, '+31600012345')
        ph2 = cb.add_phone_number(p.id, '+31600012346', 'Work')
        self.assertEqual(len(p.phone_numbers), 2)
        self.assertEqual(set(map(lambda ph: ph.id, p.phone_numbers)), {ph1.id, ph2.id})
        for ph in p.phone_numbers:
            if ph.id == ph2.id:
                self.assertEqual(ph.phone, '+31600012346')
                self.assertEqual(ph.label, 'Work')

        em1 = cb.add_email_address(p.id, 'abc@example.com')
        em2 = cb.add_email_address(p.id, 'abc@work.com', 'Work')
        self.assertEqual(len(p.email_addresses), 2)
        self.assertSetEqual(set(map(lambda em: em.id, p.email_addresses)), {em1.id, em2.id})
        for em in p.email_addresses:
            if em.id == em2.id:
                self.assertEqual(em.email, 'abc@work.com')
                self.assertEqual(em.label, 'Work')

        ad1 = cb.add_address(p.id, house_number='100', street_name='Welington')
        ad2 = cb.add_address(p.id, house_number='102', street_name='Kiwi', address_line_1='AL1',
                             address_line_2='AL2', city='Auckland', postal_code='10236', country='NZ',
                             address_label='Work')
        self.assertEqual(len(p.addresses), 2)
        self.assertSetEqual(set(map(lambda ad: ad.id, p.addresses)), {ad1.id, ad2.id})
        for ad in p.addresses:
            if ad.id == ad2.id:
                self.assertEqual(ad.house_number, '102')
                self.assertEqual(ad.country, 'NZ')
                self.assertEqual(ad.label, 'Work')

        str_repr = "<Person> P L\n" \
            "Phone numbers: [<PhoneNumber> No label: +31600012345, <PhoneNumber> Work: +31600012346]\n" \
            "Email addresses: [<EmailAddress> No label: abc@example.com, <EmailAddress> Work: abc@work.com]\n" \
            "Addresses: [<Address> No label: Welington, 100, " \
            "<Address> Work: Kiwi, 102, AL1, AL2, 10236, Auckland, NZ]\n" \
            "Groups: []"
        self.assertEqual(str(p), str_repr)

    def test_trie_lookup(self):
        cb = ContactBookDB()
        g1 = cb.create_group('G1')
        p1 = cb.create_person(first_name='Abcd', last_name='Abxy', phone_number='+31600012345', phone_label='Mobile',
                              email_address='abc@example.com', email_label='Personal', group_id=g1.id)
        p2 = cb.create_person(first_name='Xyz', middle_name='Abcd', last_name='Def')
        p3 = cb.create_person(first_name='Tuf', last_name='Def')

        res = cb.find_person_details_by_name('')
        self.assertEqual(len(res), 3)
        res = cb.find_person_details_by_name('Abcd')
        self.assertEqual(len(res), 2)
        self.assertSetEqual({(res[0].id, res[0].full_name), (res[1].id, res[1].full_name)},
                            {(p1.id, 'Abcd Abxy'), (p2.id, 'Xyz Abcd Def')})

        # Test fetching whole record after lookup
        for r in res:
            if r.id == p1.id:
                p = cb.get_person_by_id(r.id)
                self.assertEqual(p.full_name, p1.full_name)
                self.assertEqual(p.email_addresses, p1.email_addresses)

        # Test updating trie after deletion of persons
        res = cb.find_person_details_by_name('Def')
        self.assertEqual(len(res), 2)
        self.assertSetEqual({res[0].id, res[1].id}, {p2.id, p3.id})
        cb.delete_person(p3.id)
        res = cb.find_person_details_by_name('Def')
        self.assertEqual(len(res), 1)
        self.assertSetEqual({res[0].id}, {p2.id})

        # Test updating trie after updating persons
        res = cb.find_person_details_by_name('ab')
        self.assertEqual(len(res), 2)
        self.assertSetEqual({res[0].id, res[1].id}, {p1.id, p2.id})

        p1.first_name = 'Xyzzy'
        cb.save_object(p1)
        # Still last name starts with ab
        res = cb.find_person_details_by_name('ab')
        self.assertEqual(len(res), 2)
        self.assertSetEqual({res[0].id, res[1].id}, {p1.id, p2.id})

        p1.last_name = 'Spoon'
        cb.save_object(p1)
        res = cb.find_person_details_by_name('ab')
        self.assertEqual(len(res), 1)
        self.assertSetEqual({res[0].id}, {p2.id})

        # Test non-duplicate results (e.g. when both first name & last name match a prefix)
        p4 = cb.create_person(first_name='Pqrs', last_name='Pqr')
        p5 = cb.create_person(first_name='Xy', last_name='Pqr')
        res = cb.find_person_details_by_name('pq')
        self.assertEqual(len(res), 2)
        self.assertSetEqual({res[0].id, res[1].id}, {p4.id, p5.id})

    def test_trie_lookup_multi_word(self):
        cb = ContactBookDB()
        p1 = cb.create_person(first_name='Abcd', last_name='Hijk')
        p2 = cb.create_person(first_name='Cdef', last_name='Abc')
        p3 = cb.create_person(first_name='Abef', last_name='Hijk')

        res = cb.find_person_details_by_name('ab hi')
        self.assertEqual(len(res), 2)
        self.assertSetEqual({res[0].id, res[1].id}, {p1.id, p3.id})

        res = cb.find_person_details_by_name('abe hi')
        self.assertEqual(len(res), 1)
        self.assertSetEqual({res[0].id}, {p3.id})

        res = cb.find_person_details_by_name('ab hi c')
        self.assertEqual(len(res), 0)

    def test_get_persons_by_email(self):
        cb = ContactBookDB()
        p1 = cb.create_person(first_name='P1', email_address='abc@example.com')
        p2 = cb.create_person(first_name='P1', email_address='xyz@example.com')
        p3 = cb.create_person(first_name='P1', email_address='abc@badexample.com')

        res = cb.get_persons_by_email('abc@example.com')
        self.assertEqual(len(res), 1)
        self.assertSetEqual({res[0].id}, {p1.id})

        res = cb.get_persons_by_email('abc')
        self.assertEqual(len(res), 2)
        self.assertSetEqual({res[0].id, res[1].id}, {p1.id, p3.id})

        cb.add_email_address(p2.id, 'abc@example.com')
        res = cb.get_persons_by_email('abc@example.com')
        self.assertEqual(len(res), 2)
        self.assertSetEqual({res[0].id, res[1].id}, {p1.id, p2.id})

    def test_persistence(self):
        cb = ContactBookDB()
        g1 = cb.create_group('G1')
        p1 = cb.create_person(first_name='Tuv', last_name='Xyz', phone_number='+31600012345', phone_label='Mobile',
                              email_address='abc@example.com', email_label='Personal', group_id=g1.id)
        p1.first_name = 'Abc'
        cb.save_object(p1)

        cb2 = ContactBookDB()
        res = cb2.get_all_persons()
        self.assertEqual(len(res), 1)
        self.assertEqual((res[0].first_name, res[0].last_name, res[0].phone_numbers[0].phone,
                          res[0].email_addresses[0].email, res[0].groups[0].id),
                         ('Abc', 'Xyz', '+31600012345', 'abc@example.com', g1.id))

    def tearDown(self):
        fast_trie_lookup.trie.clear()
        if os.path.exists(self.TEST_DB_PATH):
            os.remove(self.TEST_DB_PATH)


if __name__ == '__main__':
    unittest.main()
