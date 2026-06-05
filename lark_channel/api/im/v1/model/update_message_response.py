from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.construct import init
from lark_channel.core.model import BaseResponse
from .update_message_response_body import UpdateMessageResponseBody


class UpdateMessageResponse(BaseResponse):
    _types = {
        "data": UpdateMessageResponseBody
    }

    def __init__(self, d=None):
        super().__init__(d)
        self.data: Optional[UpdateMessageResponseBody] = None
        init(self, d, self._types)
