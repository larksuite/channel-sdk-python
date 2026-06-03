from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.construct import init
from lark_channel.core.model import BaseResponse
from .create_image_response_body import CreateImageResponseBody


class CreateImageResponse(BaseResponse):
    _types = {
        "data": CreateImageResponseBody
    }

    def __init__(self, d=None):
        super().__init__(d)
        self.data: Optional[CreateImageResponseBody] = None
        init(self, d, self._types)
