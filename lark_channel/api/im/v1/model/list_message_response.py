from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.construct import init
from lark_channel.core.model import BaseResponse
from .list_message_response_body import ListMessageResponseBody


class ListMessageResponse(BaseResponse):
    _types = {
        "data": ListMessageResponseBody
    }

    def __init__(self, d=None):
        super().__init__(d)
        self.data: Optional[ListMessageResponseBody] = None
        init(self, d, self._types)
