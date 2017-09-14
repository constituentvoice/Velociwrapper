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
