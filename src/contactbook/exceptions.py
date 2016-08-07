# encoding=utf-8
# Author: ninadpage


class NoSuchObjectFound(Exception):
    """
    Exception class which is raised if object with given id is not found in the database.
    """

    def __init__(self, object_type, object_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.object_type = object_type
        self.object_id = object_id

    def __str__(self):
        return 'No such object of type {} found for id={}'.format(self.object_type, self.object_id)

    __repr__ = __str__
