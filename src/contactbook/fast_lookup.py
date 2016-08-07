# encoding=utf-8
# Author: ninadpage

from collections import namedtuple
from pytrie import SortedStringTrie


# noinspection PyClassHasNoInit
class FastLookupValue(namedtuple('FastLookupValueNT', ['id', 'full_name'])):

    # Settings __slots__ to an empty tuple to keep memory requirements
    # low by preventing the creation of instance dictionaries
    __slots__ = ()

    # Override __eq__ so that value comparison only considers the id.
    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)


class FastTrieLookup(object):

    def __init__(self):
        self.trie = SortedStringTrie()

    def _add_name(self, name, value_dict):
        """
        Adds a name and associated value dict to trie.
        value_dict should be a dict {person.id: person.full_name}
        """
        name = name.lower()
        if name in self.trie:
            # Name already exists (maybe some other person has same first/last name)
            self.trie[name].update(value_dict)
        else:
            self.trie[name] = value_dict

    def _delete_value_for_name(self, name, value_dict_key):
        """
        Deletes a name's associated value_dict from trie. If given name doesn't have any more values left,
        it is deleted as well. `value_dict_key` should be person.id.
        """
        name = name.lower()
        if name in self.trie:
            values = self.trie[name]
            # Remove element from `values` (which is a dict) with given `value_dict_key`
            values.pop(value_dict_key, None)

            # Check if values is empty. Delete Trie node if yes, otherwise store remaining values for given name
            if values:
                self.trie[name] = values
            else:
                del self.trie[name]
        else:
            raise KeyError(name)

    def add_person(self, person):
        """
        Adds a person's all name attributes to trie and associates them with person's value dict {id: full_name}.
        """
        value_dict = {person.id: person.full_name}

        if person.title:
            self._add_name(person.title, value_dict)
        if person.first_name:
            self._add_name(person.first_name, value_dict)
        if person.middle_name:
            self._add_name(person.middle_name, value_dict)
        if person.last_name:
            self._add_name(person.last_name, value_dict)
        if person.suffix:
            self._add_name(person.suffix, value_dict)

    def remove_person(self, person):
        """
        Removes a persons's all name attributes and associated value dicts from trie.
        """
        if person.title:
            self._delete_value_for_name(person.title, person.id)
        if person.first_name:
            self._delete_value_for_name(person.first_name, person.id)
        if person.middle_name:
            self._delete_value_for_name(person.middle_name, person.id)
        if person.last_name:
            self._delete_value_for_name(person.last_name, person.id)
        if person.suffix:
            self._delete_value_for_name(person.suffix, person.id)

    def get_persons_by_prefix(self, prefix):
        """
        Returns all value dicts which are associated with a name with given prefix.
        """
        prefix = prefix.lower()
        return self.trie.values(prefix)
