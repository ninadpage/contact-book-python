# encoding=utf-8
# Author: ninadpage

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import relationship

Base = declarative_base()


class PersonGroupAssociation(Base):
    """
    There is many-to-many relationship between persons & groups. This table is used to define such relationships.
    """
    __tablename__ = 'person_group_associations'

    # (person_id, group_id) is the composite primary key for this table.
    person_id = Column(Integer, ForeignKey('persons.id'), primary_key=True)
    group_id = Column(Integer, ForeignKey('groups.id'), primary_key=True)


class Person(Base):
    __tablename__ = 'persons'

    id = Column(Integer, primary_key=True)
    title = Column(String(length=16))
    first_name = Column(String(length=256))
    middle_name = Column(String(length=256))
    last_name = Column(String(length=256))
    suffix = Column(String(length=256))

    addresses = relationship("Address", back_populates="person", lazy="joined", cascade="all, delete-orphan")
    phone_numbers = relationship("PhoneNumber", back_populates="person", lazy="joined", cascade="all, delete-orphan")
    email_addresses = relationship("EmailAddress", back_populates="person", lazy="joined",
                                   cascade="all, delete-orphan")

    groups = relationship("Group", secondary="person_group_associations", lazy="joined", back_populates="persons")

    @property
    def full_name(self):
        return '{}{}'.format(' '.join([s for s in [self.title, self.first_name, self.middle_name, self.last_name]
                                      if s is not None]),
                             ', {}'.format(self.suffix) if self.suffix else '')

    def __str__(self):
        return '<Person> {}\nPhone numbers: {}\nEmail addresses: {}\nAddresses: {}\nGroups: {}'.format(
            self.full_name, self.phone_numbers, self.email_addresses, self.addresses, self.groups)

    __repr__ = __str__


class Group(Base):
    __tablename__ = 'groups'

    id = Column(Integer, primary_key=True)
    name = Column(String(length=256))

    persons = relationship("Person", secondary='person_group_associations', lazy="joined", back_populates="groups")

    def __str__(self):
        return '<Group> {}'.format(self.name)

    __repr__ = __str__


class AbstractField(Base):
    """
    Abstract class for all fields in a contact (phone numbers, emails, addresses, notes, etc).
    All fields belong to (have a many-to-one relationship with) Person and all have a label (e.g. 'Home' number,
    'Mobile' number, 'Word' address, etc).
    """
    __abstract__ = True

    id = Column(Integer, primary_key=True)
    label = Column(String(length=255))

    # noinspection PyMethodParameters
    @declared_attr
    def person_id(cls):
        return Column(Integer, ForeignKey('persons.id'))


class Address(AbstractField):
    __tablename__ = 'addresses'

    house_number = Column(String(length=32))
    street_name = Column(String(length=256))
    address_line_1 = Column(String(length=1024))
    address_line_2 = Column(String(length=1024))
    city = Column(String(length=256))
    postal_code = Column(String(length=32))
    country = Column(String(length=256))

    person = relationship("Person", back_populates="addresses", single_parent=True, lazy="joined",
                          cascade="all, delete-orphan")

    def __str__(self):
        return '<Address> {}: {}'.format(self.label if self.label else 'No label',
                                         ', '.join([s for s in [self.street_name, self.house_number,
                                                                self.address_line_1, self.address_line_2,
                                                                self.postal_code, self.city, self.country]
                                                    if s is not None]))

    __repr__ = __str__


class PhoneNumber(AbstractField):
    __tablename__ = 'phone_numbers'

    phone = Column(String(length=256), nullable=False)

    person = relationship("Person", back_populates="phone_numbers", single_parent=True, lazy="joined",
                          cascade="all, delete-orphan")

    def __str__(self):
        return '<PhoneNumber> {}: {}'.format(self.label if self.label else 'No label', self.phone)

    __repr__ = __str__


class EmailAddress(AbstractField):
    __tablename__ = 'email_addresses'

    email = Column(String(length=256), nullable=False)

    person = relationship("Person", back_populates="email_addresses", single_parent=True, lazy="joined",
                          cascade="all, delete-orphan")

    def __str__(self):
        return '<EmailAddress> {}: {}'.format(self.label if self.label else 'No label', self.email)

    __repr__ = __str__
