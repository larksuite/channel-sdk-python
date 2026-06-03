from typing import Any, List, Optional

from lark_channel.core import AppType, LogLevel
from lark_channel.core.cache import ICache
from lark_channel.core.const import FEISHU_DOMAIN


class Config(object):
    def __init__(self) -> None:
        self.app_id: Optional[str] = None
        self.app_secret: Optional[str] = None
        self.domain: str = FEISHU_DOMAIN  # Domain; defaults to https://open.feishu.cn
        self.timeout: Optional[float] = 30  # client timeout in seconds (default 30s); override via ClientBuilder.timeout()
        self.app_type: AppType = AppType.SELF  # App type; defaults to a self-built app. When set to ISV, configure tenant_key in request_option
        self.enable_set_token: bool = False  # Whether manual token setting is allowed; disabled by default. When enabled, configure the token in request_option
        self.cache: Optional[ICache] = None  # Custom cache; defaults to the built-in local cache
        self.log_level: LogLevel = LogLevel.WARNING  # Log level; defaults to WARNING
        self.source: Optional[str] = None  # caller identifier, appended to UA as `source/<name>`
        self.proxy_url: Optional[str] = None
        self.trust_env_proxy: Optional[bool] = None
        # Internal: sub-modules (e.g. channel) append bare UA tags from here.
        self.extra_ua_tags: Optional[List[str]] = None
        self.security: Optional[Any] = None
