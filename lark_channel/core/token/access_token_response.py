from typing import Optional

from lark_channel.core.construct import init
from lark_channel.core.model import BaseResponse


class AccessTokenResponse(BaseResponse):
    _types = {}

    def __init__(self, d):
        super().__init__(d)
        self.tenant_access_token: Optional[str] = None
        self.app_access_token: Optional[str] = None
        self.expire: Optional[int] = None
        init(self, d, self._types)
