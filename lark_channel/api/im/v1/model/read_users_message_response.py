from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.construct import init
from lark_channel.core.model import BaseResponse
from .read_users_message_response_body import ReadUsersMessageResponseBody


class ReadUsersMessageResponse(BaseResponse):
    _types = {
        "data": ReadUsersMessageResponseBody
    }

    def __init__(self, d=None):
        super().__init__(d)
        self.data: Optional[ReadUsersMessageResponseBody] = None
        init(self, d, self._types)
