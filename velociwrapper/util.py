def all_subclasses(cls):
    """
    Generator that recursively yields all the subclasses of the passed in class
    Args:
        cls: class
    Returns: generator
    """
    for subclass in cls.__subclasses__():
        yield subclass
        for sub_subclass in all_subclasses(subclass):
            yield sub_subclass


class VWUnset(object):
    """
    An empty value.

    Used when None and False are valid choices to verify 
    the value is not set by the user

    Thanks to WTForms for this idea
    """

    def __repr__(self):
        return '<VWUnset>'

    def __bool__(self):
        return False

    def __nonzero__(self):
        return False


unset = VWUnset()
