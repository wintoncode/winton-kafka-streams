class InMemoryKeyValueStore:
    def __init__(self, name):
        self.name = name
        self.dict = {}

    def initialise(self, context, root):
        pass
        # TODO: register with context, passing restore callback

    def __setitem__(self, key, value):
        self.dict[key] = value

    def __getitem__(self, key):
        return self.dict[key]

    def get(self, key, default=None):
        return self.dict.get(key, default)

    def __delitem__(self, key):
        v = self.dict[key]
        del self.dict[key]
        return v
