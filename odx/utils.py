import datetime

from . import config


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, force=False, **kwargs):
        if force:
            cls._instances[cls] = super(Singleton, cls).__call__(
                *args, **kwargs
            )
        try:
            return cls._instances[cls]
        except KeyError:
            cls._instances[cls] = super(Singleton, cls).__call__(
                *args, **kwargs
            )
        return cls._instances[cls]


def ddict2dict(d):
    for k, v in d.items():
        if isinstance(v, dict):
            d[k] = ddict2dict(v)
    return dict(d)
