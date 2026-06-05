from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.construct import init
from lark_channel.core.model import BaseResponse
from .list_user_response_body import ListUserResponseBody


class ListUserResponse(BaseResponse):
    _types = {
        "data": ListUserResponseBody
    }

    def __init__(self, d=None):
        super().__init__(d)
        self.data: Optional[ListUserResponseBody] = None
        init(self, d, self._types)
