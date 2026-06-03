from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.construct import init
from lark_channel.core.model import BaseResponse
from .list_chat_response_body import ListChatResponseBody


class ListChatResponse(BaseResponse):
    _types = {
        "data": ListChatResponseBody
    }

    def __init__(self, d=None):
        super().__init__(d)
        self.data: Optional[ListChatResponseBody] = None
        init(self, d, self._types)
