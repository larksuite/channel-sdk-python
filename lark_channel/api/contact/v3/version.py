from lark_channel.core.model import Config

from .resource import User


class V3(object):
    def __init__(self, config: Config) -> None:
        self.user: User = User(config)
