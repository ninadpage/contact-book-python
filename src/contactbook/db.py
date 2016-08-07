# encoding=utf-8
# Author: ninadpage

import functools
import traceback
import sqlite3
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from .exceptions import NoSuchObjectFound
from .models import Base, Person, Address, PhoneNumber, EmailAddress, Group
from .fast_lookup import FastTrieLookup, FastLookupValue


sqlalchemy_database_url = None
sqlalchemy_engine = None
sqlalchemy_sessionmaker = None
logger = None

fast_trie_lookup = FastTrieLookup()


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    # Enable Foreign Key support for sqlite
    if isinstance(dbapi_connection, sqlite3.Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


def db_init(*, db_logger, sqlite_db_path=None, db_connection_string=None):
    global sqlalchemy_database_url, sqlalchemy_engine, sqlalchemy_sessionmaker, logger

    logger = db_logger

    if sqlite_db_path and db_connection_string:
        raise ValueError('Only one of sqlite_db_path and connection_string must be provided')

    if sqlite_db_path:
        import sqlite3
        # Create given sqlite database if it doesn't exist
        sqlite3.connect(sqlite_db_path)
        _db_connection_string = 'sqlite:///{}'.format(sqlite_db_path)
    elif db_connection_string:
        _db_connection_string = db_connection_string
    else:
        raise ValueError('One of sqlite_db_path and connection_string must be provided')

    sqlalchemy_database_url = _db_connection_string
    sqlalchemy_engine = create_engine(sqlalchemy_database_url)
    sqlalchemy_sessionmaker = sessionmaker(bind=sqlalchemy_engine)

    Base.metadata.create_all(sqlalchemy_engine)
    init_lookup_trie_with_existing_persons()


def init_lookup_trie_with_existing_persons():
    """
    Feeds existing person records into lookup trie.

    :return: None
    """
    cb = ContactBookDB()
    persons = cb.get_all_persons()
    for person in persons:
        fast_trie_lookup.add_person(person)


def sqlalchemy_session(commit=False, expunge=False, close_session=False):
    """
    Decorator for ContactBookDB methods, which creates a SQLAlchemy session, rolls back in case of any exception and
    disposes it off after use. The session is available as `self.session` inside the method which adds this decorator.

    One problem with this decorator is, since it overwrites self.session every time it is invoked,
    you cannot call one decorated method from other decorated method, as the latter invocation will
    overwrite prior invocation's session.
    So what if you want to cleanly manipulate two objects from same method, but without letting their sessions
    mix? You can use `extra_sessions` parameter. It'll create a list of sessions of given length, available
    as `self.sessions`. Note that these sessions are read-only, they cannot be committed.
    """
    global sqlalchemy_sessionmaker

    def sqlalchemy_session_decorator(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            self = args[0]
            if self.session is None:
                self.session = sqlalchemy_sessionmaker()

            try:
                ret_value = func(*args, **kwargs)
            except SQLAlchemyError as e:
                self.session.rollback()
                logger.error('Exception in SQLAlchemy session: {}\n{}'.format(e, traceback.format_exc()))
                raise
            else:
                if commit:
                    self.session.commit()
                if expunge:
                    self.session.expunge_all()
            finally:
                if close_session:
                    self.session.close()
                    self.session = None

            return ret_value
        return wrapper
    return sqlalchemy_session_decorator


class ContactBookDB(object):

    def __init__(self):
        self.session = None

    @sqlalchemy_session(commit=True)
    def create_person(self, title=None, first_name=None, middle_name=None, last_name=None, *, suffix=None,
                      phone_number=None, phone_label=None, email_address=None, email_label=None,
                      group_id=None):
        """
        Adds a person and optionally, associated details (phone number, email, group). The group must exist.
        Street address can be later added manually. Addition details such as other phone numbers
        or other groups can also be later added manually.

        :param title: Title
        :type title: str
        :param first_name: First name
        :type first_name: str
        :param middle_name: Middle name
        :type middle_name: str
        :param last_name: Last name
        :type last_name: str
        :param suffix: Suffix
        :type suffix: str
        :param phone_number: Phone number
        :type phone_number: str
        :param phone_label: Label for phone number
        :type phone_label: str
        :param email_address: Email address
        :type email_address: str
        :param email_label: Label for email address
        :type email_label: str
        :param group_id: id of the group this person belongs to
        :type group_id: int
        :return: Person created
        :rtype: models.Person
        """

        if any([first_name, middle_name, last_name]):
            person = Person(title=title, first_name=first_name, middle_name=middle_name, last_name=last_name,
                            suffix=suffix)
            self.session.add(person)
            if phone_number:
                phone = PhoneNumber(person=person, phone=phone_number, label=phone_label)
                self.session.add(phone)
            if email_address:
                email = EmailAddress(person=person, email=email_address, label=email_label)
                self.session.add(email)
            if group_id:
                group = self.session.query(Group).get(group_id)
                if group is None:
                    raise NoSuchObjectFound('Group', group_id)
                person.groups.append(group)

            # Intermediary commit to refresh the person object so that it gets updated with id
            self.session.commit()
            fast_trie_lookup.add_person(person)
            return person
        else:
            raise ValueError('At least one of first_name, middle_name and last_name must be specified')

    @sqlalchemy_session(commit=True)
    def create_group(self, name):
        """
        Adds a new group.

        :param name: Name of the group
        :type name: str
        :return: Group created
        :rtype: models.Group
        """
        if not name:
            raise ValueError('Group name must be specified')
        group = Group(name=name)
        self.session.add(group)

        return group

    @sqlalchemy_session()
    def get_person_by_id(self, person_id):
        """
        Returns a Person object with given id.

        :param person_id: id of the person
        :type person_id: int
        :return: Person object
        :rtype: models.Person
        """
        person = self.session.query(Person).get(person_id)
        if person is None:
            raise NoSuchObjectFound('Person', person_id)
        return person

    @sqlalchemy_session()
    def get_group_by_id(self, group_id):
        """
        Returns a Group object with given id.

        :param group_id: id of the group
        :type group_id: int
        :return: Group object
        :rtype: models.Group
        """
        group = self.session.query(Group).get(group_id)
        if group is None:
            raise NoSuchObjectFound('Group', group_id)
        return group

    @sqlalchemy_session(commit=True)
    def add_group_to_person(self, person_id, group_id):
        """
        Adds group specified by group_id to person specified by person_id.

        :param person_id: id of the person
        :type person_id: int
        :param group_id: id of the group
        :type group_id: int
        :return: None
        """
        person = self.session.query(Person).get(person_id)
        if person is None:
            raise NoSuchObjectFound('Person', person_id)
        group = self.session.query(Group).get(group_id)
        if group is None:
            raise NoSuchObjectFound('Group', group_id)
        person.groups.append(group)

    @sqlalchemy_session(commit=True)
    def add_phone_number(self, person_id, phone_number, phone_label=None):
        """
        Adds a new phone number to person specified by person_id.

        :param person_id: id of the person
        :type person_id: int
        :param phone_number: Phone number
        :type phone_number: str
        :param phone_label: Label for phone number
        :type phone_label: str
        :return: Phone number created
        :rtype: models.PhoneNumber
        """
        if not phone_number:
            raise ValueError('Phone number must be specified')
        person = self.session.query(Person).get(person_id)
        if person is None:
            raise NoSuchObjectFound('Person', person_id)
        phone = PhoneNumber(person=person, phone=phone_number, label=phone_label)
        self.session.add(phone)
        return phone

    @sqlalchemy_session(commit=True)
    def add_email_address(self, person_id, email_address, email_label=None):
        """
        Adds a new email address to person specified by person_id.

        :param person_id: id of the person
        :type person_id: int
        :param email_address: Email address
        :type email_address: str
        :param email_label: Label for email address
        :type email_label: str
        :return: Email address created
        :rtype: models.EmailAddress
        """
        if not email_address:
            raise ValueError('Email address must be specified')
        person = self.session.query(Person).get(person_id)
        if person is None:
            raise NoSuchObjectFound('Person', person_id)
        email = EmailAddress(person=person, email=email_address, label=email_label)
        self.session.add(email)
        return email

    @sqlalchemy_session(commit=True)
    def add_address(self, person_id, house_number=None, street_name=None, address_line_1=None,
                    address_line_2=None, city=None, postal_code=None, country=None, address_label=None):
        """
        Adds a new address to person specified by person_id.

        :param person_id: id of the person
        :type person_id: int
        :param house_number: House number
        :type house_number: str
        :param street_name: Street name
        :type street_name: str
        :param address_line_1: Address line 1
        :type address_line_1: str
        :param address_line_2: Address line 2
        :type address_line_2: str
        :param city: City
        :type city: str
        :param postal_code: Postal code
        :type postal_code: str
        :param country: Country
        :type country: str
        :param address_label: Label for address
        :type address_label: str
        :return: Address created
        :rtype: models.Address
        """

        if not any([house_number, street_name, address_line_1, address_line_2, city, postal_code, country]):
            raise ValueError('At least one address field must be specified')
        person = self.session.query(Person).get(person_id)
        if person is None:
            raise NoSuchObjectFound('Person', person_id)
        address = Address(person=person, house_number=house_number, street_name=street_name,
                          address_line_1=address_line_1, address_line_2=address_line_2, city=city,
                          postal_code=postal_code, country=country, label=address_label)
        self.session.add(address)
        return address

    @sqlalchemy_session(commit=True)
    def delete_person(self, person_id):
        """
        Deletes a person and all associated fields (phone numbers, email addresses, etc).
        The person is also removed from the groups he/she was part of.

        :param person_id: id of the person to delete
        :type person_id: int
        :return: None
        """
        person = self.session.query(Person).get(person_id)
        if person is None:
            raise NoSuchObjectFound('Person', person_id)
        # Also delete the person from lookup trie
        fast_trie_lookup.remove_person(person)
        # cascade clause used in defining relationships in models will take care of deleting associated
        # rows from other tables (phone_numbers, addresses, person_group_associations, etc).
        self.session.delete(person)

    @sqlalchemy_session(commit=True)
    def delete_group(self, group_id):
        """
        Deletes a group. The group is also removed from the persons who were part of this group.

        :param group_id: id of the group to delete
        :type group_id: int
        :return: None
        """
        group = self.session.query(Group).get(group_id)
        if group is None:
            raise NoSuchObjectFound('Group', group_id)
        # cascade clause used in defining relationships in models will take care of deleting associated rows
        # from person_group_associations table.
        self.session.delete(group)

    @sqlalchemy_session(commit=True)
    def delete_phone_number(self, phone_number_id):
        """
        Deletes a phone number.

        :param phone_number_id: id of the phone number to delete
        :type phone_number_id: int
        :return: None
        """
        phone = self.session.query(PhoneNumber).get(phone_number_id)
        if phone is None:
            raise NoSuchObjectFound('PhoneNumber', phone_number_id)
        self.session.delete(phone)

    @sqlalchemy_session(commit=True)
    def delete_email_address(self, email_address_id):
        """
        Deletes an email address.

        :param email_address_id: id of the email address to delete
        :type email_address_id: int
        :return: None
        """
        email = self.session.query(EmailAddress).get(email_address_id)
        if email is None:
            raise NoSuchObjectFound('EmailAddress', email_address_id)
        self.session.delete(email)

    @sqlalchemy_session(commit=True)
    def delete_address(self, address_id):
        """
        Deletes an address.

        :param address_id: id of the address to delete
        :type address_id: int
        :return: None
        """
        address = self.session.query(Address).get(address_id)
        if address is None:
            raise NoSuchObjectFound('Address', address_id)
        self.session.delete(address)

    @sqlalchemy_session(commit=True)
    def save_object(self, obj):
        """
        Writes in-memory changes done to an SQLAlchemy object to database.

        :param obj: Any Contact Book object (Person, Address, PhoneNumber, EmailAddress, Group)
        :type obj: sqlalchemy.ext.declarative.api.Base
        :return: Updated object
        :rtype: sqlalchemy.ext.declarative.api.Base
        """
        # Updating Person object is tricky, because we also need to update lookup trie,
        # but for that we need old name attributes of Person.
        if isinstance(obj, Person):
            # Using new session to read existing (before updating) data of person
            global sqlalchemy_sessionmaker
            session = sqlalchemy_sessionmaker()
            old_person = session.query(Person).get(obj.id)
            if old_person is None:
                raise NoSuchObjectFound('Person', obj.id)
            fast_trie_lookup.remove_person(old_person)

        self.session.merge(obj)
        if isinstance(obj, Person):
            fast_trie_lookup.add_person(obj)
        return obj

    @sqlalchemy_session()
    def get_all_persons(self, group_id=None):
        """
        Returns all persons. Optionally filters based on given group_id.

        :param group_id: Group id to filter persons on
        :type group_id: int
        :return: List of persons
        :rtype: <list (models.Person)>
        """
        query = self.session.query(Person)
        if group_id:
            query = query.filter(Person.groups.any(id=group_id))
        result = query.all()
        # This query result is detached from session by the decorator, so that
        # the caller can safely manipulate it
        return result

    @sqlalchemy_session()
    def get_persons_by_email(self, email):
        """
        Returns all persons with matching email address. `email` can be prefix of the address.
        e.g., for a person with email abc@example.com, it will return that person for both
        email='abc@example.com' and email='abc'.

        Since we use SQL LIKE query to find matching persons, it's fairly straightforward to
        extend it to find match anywhere (instead of just as prefix).
        i.e., ``SELECT when email LIKE '%<substr>%';``

        However, such queries (patterns beginning with wildcards) need to do full-table scan
        as indices won't help for that kind of queries. It can be sped up by using FULLTEXT
        index in MySQL or using CONTAINS instead of LIKE in SQL Server (which again uses
        a full-text index). However, expect the performance gain to be limited as full-
        text indices typically split words.

        But in a contact book, looking up by email isn't typically expected to be an instant
        operation like looking up by name or phone number is, so any of the above solutions
        would do just fine.

        :param email: Email address to filter persons on (can be a prexix)
        :type email: str
        :return: List of persons
        :rtype: <list (models.Person)>
        """
        return self.session.query(Person).filter(Person.email_addresses.any(
            EmailAddress.email.like('{}%'.format(email)))).all()

    @staticmethod
    def _find_person_details_by_prefix(prefix):
        """
        Helper method for find_person_details_by_name which only works for a single word `prefix`.
        """
        result = fast_trie_lookup.get_persons_by_prefix(prefix)
        # result is a list of dicts, will possible duplicates as there may be multiple paths to a single
        # person (e.g. via common prefix of first name & last name). We need to merge all results to remove
        # duplicates.
        merged = {}
        for d in result:
            merged.update(d)
        # Create a list of namedtuples <FastLookupValue> from merged result
        return list(map(lambda kv: FastLookupValue(*kv), merged.items()))

    @classmethod
    def find_person_details_by_name(cls, name):
        """
        Returns short details of a person (person id and full name) given a name. This name can be
        of any of person's attributes (i.e., first_name, last_name, etc) and can even be a prefix.
        Lookup is case-insensitive.

        If name is space separated, each word is searched individually, and results matching _both_
        are returned. This can be used to search for persons with matching first name & last name (both).

        This lookup is extremely fast as it is performed on an in-memory trie which caches these details.
        So this method can be used to implement Auto-Complete for a Contacts app, where you call this
        every time a character is entered by the user, show the user list of matching full names which
        is updated instantly, and when the user picks one, use the id to fetch full details of the person.

        :Example:

        Assume you have persons with following persons (id: full name)::

        >>> 1: Abcd Hijk
        >>> 2: Cdef Abc
        >>> 3: Abef Hijk

        Then

        >>> find_person_details_by_name('ab')

        will return ``[(1, Abcd Hijk), (2, Abef Abc), (3, Abef Hijk)]``

        >>> find_person_details_by_name('abc')

        will return ``[(1, Abcd Hijk), (2, Abef Abc)]``

        >>> find_person_details_by_name('abcd')

        will return ``[(1, Abcd Hijk)]``

        >>> find_person_details_by_name('ab hi')

        will return ``[(1, Abcd Hijk), (3, Abef Hijk)]``

        >>> find_person_details_by_name('abe hi')

        will return ``[(3, Abef Hijk)]``

        :param name: Prefix of any name attribute to lookup
        :type name: str
        :return: List of short persons' details (which are namedtuples with fields id and full_name)
        :rtype: <list (fast_lookup.FastLookupValue)>
        """
        if not name or len(name.split()) == 1:
            return cls._find_person_details_by_prefix(name)
        else:
            result_sets = []
            for word in name.split():
                result_sets.append(set(cls._find_person_details_by_prefix(word)))
            # Since we only want results which match with all words in name, we need to take intersection
            # of merged_results (FastLookupValue overrides __eq__ so that the comparison works on id)
            return list(set.intersection(*result_sets))
