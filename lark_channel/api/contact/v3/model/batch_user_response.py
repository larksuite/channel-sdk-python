from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.construct import init
from lark_channel.core.model import BaseResponse
from .batch_user_response_body import BatchUserResponseBody


class BatchUserResponse(BaseResponse):
    _types = {
        "data": BatchUserResponseBody
    }

    def __init__(self, d=None):
        super().__init__(d)
        self.data: Optional[BatchUserResponseBody] = None
        init(self, d, self._types)
