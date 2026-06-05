from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.construct import init
from lark_channel.core.model import BaseResponse


class UpdateCardElementResponse(BaseResponse):
    _types = {

    }

    def __init__(self, d=None):
        super().__init__(d)

        init(self, d, self._types)
