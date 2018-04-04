from .store_change_logger import StoreChangeLogger


class ChangeLoggingKeyValueStore:
    def __init__(self, name, inner):
        self.inner = inner(name)
        self.change_logger = None

    def initialise(self, context, root):
        self.inner.initialise(context, root)
        self.change_logger = StoreChangeLogger(self.inner.name, context)

    def __setitem__(self, key, value):
        self.inner.__setitem__(key, value)
        self.change_logger.log_change(key, value)

    def __getitem__(self, key):
        return self.inner.__getitem__(key)

    def get(self, key, default=None):
        return self.inner.get(key, default)

    def __delitem__(self, key):
        v = self.inner.__delitem__(key)
        self.change_logger.log_change(key, None)
        return v
