from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.construct import init
from lark_channel.core.model import BaseResponse
from .create_message_response_body import CreateMessageResponseBody


class CreateMessageResponse(BaseResponse):
    _types = {
        "data": CreateMessageResponseBody
    }

    def __init__(self, d=None):
        super().__init__(d)
        self.data: Optional[CreateMessageResponseBody] = None
        init(self, d, self._types)
