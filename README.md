# contact-book-python
An implementation of Android-like Contact book API, written in Python.

## Prerequisites
1. Tested with Python 3.4+.
2. [virtualenv](https://virtualenv.pypa.io/en/stable/) is recommended.

## Setup
1. Activate virtualenv.
2. This library depends on [SQLAlchemy](https://pypi.python.org/pypi/SQLAlchemy), whose latest version
   often cannot be built on older setuptools. Update your setuptools using
   `pip install --upgrade pip setuptools`
3. Install this library using `pip install git+https://github.com/ninadpage/contact-book-python.git`

## Tests
Testsuite can be executed using `python setup.py test`

## Example

```
>>> import contactbook
>>> contactbook.init_contactbook(sqlite_db_path='test.db')
>>> cb = contactbook.ContactBookDB()
>>> p = cb.create_person(first_name='First', last_name='Last')
>>> cb.get_all_persons()
[<Person> First Last
Phone numbers: []
Email addresses: []
Addresses: []
Groups: []]
```

Full API documentation can be found [here](http://contact-book-python.readthedocs.io/?).

## Dependencies
1. [SQLAlchemy](http://docs.sqlalchemy.org/en/latest/intro.html)
2. [PyTrie](https://pypi.python.org/pypi/PyTrie)

## Persistence
This library needs a database to store all data. Simplest option is to provide a SQLite db file, like above example.
But it may work with any major database engine. See
[init_contact](http://contact-book-python.readthedocs.io/?#contactbook.init_contactbook) for more details.

