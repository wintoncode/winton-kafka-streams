"""
Identity serialiser (default)

"""


class IdentitySerde:
    """
    Identity serialiser that makes no changes to values
    during serialisation deserialisation
    """
    def serialise(self, value):
        return value
    def deserialise(self, value):
        return value
