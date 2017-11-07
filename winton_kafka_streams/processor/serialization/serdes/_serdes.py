import importlib
import inspect


def serde_from_string(serde_name):
    module_name, class_name = serde_name.rsplit(".", 1)
    module = importlib.import_module(module_name)
    SerdeClass = getattr(module, class_name)
    return SerdeClass()


def serde_as_string(serde):
    module_name = serde.__module__
    class_name = serde.__name__ if inspect.isclass(serde) else serde.__class__.__name__
    return module_name + "." + class_name
