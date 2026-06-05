from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.construct import init
from lark_channel.core.model import BaseResponse
from .get_user_response_body import GetUserResponseBody


class GetUserResponse(BaseResponse):
    _types = {
        "data": GetUserResponseBody
    }

    def __init__(self, d=None):
        super().__init__(d)
        self.data: Optional[GetUserResponseBody] = None
        init(self, d, self._types)
