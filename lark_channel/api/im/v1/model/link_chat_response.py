from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.construct import init
from lark_channel.core.model import BaseResponse
from .link_chat_response_body import LinkChatResponseBody


class LinkChatResponse(BaseResponse):
    _types = {
        "data": LinkChatResponseBody
    }

    def __init__(self, d=None):
        super().__init__(d)
        self.data: Optional[LinkChatResponseBody] = None
        init(self, d, self._types)
