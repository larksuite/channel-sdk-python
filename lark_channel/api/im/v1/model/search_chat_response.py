from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.construct import init
from lark_channel.core.model import BaseResponse
from .search_chat_response_body import SearchChatResponseBody


class SearchChatResponse(BaseResponse):
    _types = {
        "data": SearchChatResponseBody
    }

    def __init__(self, d=None):
        super().__init__(d)
        self.data: Optional[SearchChatResponseBody] = None
        init(self, d, self._types)
