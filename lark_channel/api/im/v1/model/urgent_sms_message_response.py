from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.construct import init
from lark_channel.core.model import BaseResponse
from .urgent_sms_message_response_body import UrgentSmsMessageResponseBody


class UrgentSmsMessageResponse(BaseResponse):
    _types = {
        "data": UrgentSmsMessageResponseBody
    }

    def __init__(self, d=None):
        super().__init__(d)
        self.data: Optional[UrgentSmsMessageResponseBody] = None
        init(self, d, self._types)
