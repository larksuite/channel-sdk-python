from .v1.version import V1
from lark_channel.core.model import Config


class ImService(object):
    def __init__(self, config: Config) -> None:
        self.v1: V1 = V1(config)
