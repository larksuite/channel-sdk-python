from lark_channel.core.model import Config
from .v1.version import V1


class CardkitService(object):
    def __init__(self, config: Config) -> None:
        self.v1: V1 = V1(config)
