from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.construct import init
from lark_channel.core.model import BaseResponse
from .create_file_response_body import CreateFileResponseBody


class CreateFileResponse(BaseResponse):
    _types = {
        "data": CreateFileResponseBody
    }

    def __init__(self, d=None):
        super().__init__(d)
        self.data: Optional[CreateFileResponseBody] = None
        init(self, d, self._types)
