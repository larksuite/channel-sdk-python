from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.construct import init
from lark_channel.core.model import BaseResponse
from .create_chat_response_body import CreateChatResponseBody


class CreateChatResponse(BaseResponse):
    _types = {
        "data": CreateChatResponseBody
    }

    def __init__(self, d=None):
        super().__init__(d)
        self.data: Optional[CreateChatResponseBody] = None
        init(self, d, self._types)
