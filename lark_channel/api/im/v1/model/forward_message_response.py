from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.construct import init
from lark_channel.core.model import BaseResponse
from .forward_message_response_body import ForwardMessageResponseBody


class ForwardMessageResponse(BaseResponse):
    _types = {
        "data": ForwardMessageResponseBody
    }

    def __init__(self, d=None):
        super().__init__(d)
        self.data: Optional[ForwardMessageResponseBody] = None
        init(self, d, self._types)
