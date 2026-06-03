from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.construct import init
from lark_channel.core.model import BaseResponse
from .get_message_response_body import GetMessageResponseBody


class GetMessageResponse(BaseResponse):
    _types = {
        "data": GetMessageResponseBody
    }

    def __init__(self, d=None):
        super().__init__(d)
        self.data: Optional[GetMessageResponseBody] = None
        init(self, d, self._types)
