import io
from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.const import UTF_8, CONTENT_TYPE, APPLICATION_JSON
from lark_channel.core import JSON
from lark_channel.core.token import verify
from lark_channel.core.http import Transport
from lark_channel.core.model import Config, RequestOption, RawResponse
from lark_channel.core.utils import Files
from requests_toolbelt import MultipartEncoder
from ..model.get_message_resource_request import GetMessageResourceRequest
from ..model.get_message_resource_response import GetMessageResourceResponse


class MessageResource(object):
    def __init__(self, config: Config) -> None:
        self.config: Config = config

    def get(self, request: GetMessageResourceRequest,
            option: Optional[RequestOption] = None) -> GetMessageResourceResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Add content-type
        if request.body is not None:
            option.headers[CONTENT_TYPE] = f"{APPLICATION_JSON}; charset=utf-8"

        # Send the request
        resp: RawResponse = Transport.execute(self.config, request, option)

        # Handle the binary stream
        content_type = resp.headers.get(CONTENT_TYPE)
        response: GetMessageResourceResponse = GetMessageResourceResponse()
        if 200 <= resp.status_code < 300:
            response.code = 0
            response.file = io.BytesIO(resp.content)
            response.file_name = Files.parse_file_name(resp.headers)
        elif content_type is not None and content_type.startswith(APPLICATION_JSON):
            response = JSON.unmarshal(str(resp.content, UTF_8), GetMessageResourceResponse)

        response.raw = resp
        return response

    async def aget(self, request: GetMessageResourceRequest,
                   option: Optional[RequestOption] = None) -> GetMessageResourceResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Send the request
        resp: RawResponse = await Transport.aexecute(self.config, request, option)

        # Handle the binary stream
        content_type = resp.headers.get(CONTENT_TYPE)
        response: GetMessageResourceResponse = GetMessageResourceResponse()
        if 200 <= resp.status_code < 300:
            response.code = 0
            response.file = io.BytesIO(resp.content)
            response.file_name = Files.parse_file_name(resp.headers)
        elif content_type is not None and content_type.startswith(APPLICATION_JSON):
            response = JSON.unmarshal(str(resp.content, UTF_8), GetMessageResourceResponse)

        response.raw = resp
        return response
