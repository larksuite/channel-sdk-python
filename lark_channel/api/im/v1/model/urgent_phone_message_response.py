from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.construct import init
from lark_channel.core.model import BaseResponse
from .urgent_phone_message_response_body import UrgentPhoneMessageResponseBody


class UrgentPhoneMessageResponse(BaseResponse):
    _types = {
        "data": UrgentPhoneMessageResponseBody
    }

    def __init__(self, d=None):
        super().__init__(d)
        self.data: Optional[UrgentPhoneMessageResponseBody] = None
        init(self, d, self._types)
