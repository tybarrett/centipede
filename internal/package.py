"""
Package.py - defines a class that holds
"""

class Package(object):
    """
    A simple placeholder class meant to store abstract data, dependent on
    the specific needs of the application.

    Stages attach what they produce as attributes and read what earlier stages
    left the same way. Because the set of fields is whatever the pipeline has put
    there, a package can say what it is carrying.
    """

    def __init__(self):
        """
        The "linked resources" for this data package refers to a list of URLs
        that are pushed onto the ingestion queue.
        """
        self.linked_resources = []

    def get(self, name, default=None):
        """
        Returns a field an earlier stage attached, or default if no stage did.

        Use this for a field an earlier stage attaches only sometimes; reach for
        the attribute directly when the stage before is meant to guarantee it.
        """
        return getattr(self, name, default)

    def fields(self):
        """
        The names of everything the package is currently carrying.
        """
        return sorted(self.__dict__)

    def get_linked_resources(self):
        """
        Returns the resources that should be pushed onto the ingestion queue.
        """
        return self.linked_resources

    def __getattr__(self, name):
        # Reached only when normal lookup has already failed. Dunder lookups are
        # left alone: pickle asks about several before a package has any state,
        # and answering those with a report about pipeline fields would be wrong
        # as well as unhelpful.
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)

        carried = ", ".join(sorted(self.__dict__)) or "nothing"
        raise AttributeError(
            "the package carries no %r, only: %s" % (name, carried)
        )

    def __repr__(self):
        return "<Package carrying %s>" % (", ".join(self.fields()) or "nothing")
