from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.construct import init
from lark_channel.core.model import BaseResponse
from .patch_user_response_body import PatchUserResponseBody


class PatchUserResponse(BaseResponse):
    _types = {
        "data": PatchUserResponseBody
    }

    def __init__(self, d=None):
        super().__init__(d)
        self.data: Optional[PatchUserResponseBody] = None
        init(self, d, self._types)
