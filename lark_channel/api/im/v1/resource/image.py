import io
from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.const import UTF_8, CONTENT_TYPE, APPLICATION_JSON
from lark_channel.core import JSON
from lark_channel.core.token import verify
from lark_channel.core.http import Transport
from lark_channel.core.model import Config, RequestOption, RawResponse
from lark_channel.core.utils import Files
from requests_toolbelt import MultipartEncoder
from ..model.create_image_request import CreateImageRequest
from ..model.create_image_response import CreateImageResponse
from ..model.get_image_request import GetImageRequest
from ..model.get_image_response import GetImageResponse


class Image(object):
    def __init__(self, config: Config) -> None:
        self.config: Config = config

    def create(self, request: CreateImageRequest, option: Optional[RequestOption] = None) -> CreateImageResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Add content-type
        if request.body is not None:
            form_data = MultipartEncoder(Files.parse_form_data(request.body))
            request.body = form_data
            option.headers[CONTENT_TYPE] = form_data.content_type

        # Send the request
        resp: RawResponse = Transport.execute(self.config, request, option)

        # Deserialize the response
        response: CreateImageResponse = JSON.unmarshal(str(resp.content, UTF_8), CreateImageResponse)
        response.raw = resp

        return response

    async def acreate(self, request: CreateImageRequest, option: Optional[RequestOption] = None) -> CreateImageResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Parse the file
        request.files = Files.extract_files(request.body)

        # Send the request
        resp: RawResponse = await Transport.aexecute(self.config, request, option)

        # Deserialize the response
        response: CreateImageResponse = JSON.unmarshal(str(resp.content, UTF_8), CreateImageResponse)
        response.raw = resp

        return response

    def get(self, request: GetImageRequest, option: Optional[RequestOption] = None) -> GetImageResponse:
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
        response: GetImageResponse = GetImageResponse()
        if 200 <= resp.status_code < 300:
            response.code = 0
            response.file = io.BytesIO(resp.content)
            response.file_name = Files.parse_file_name(resp.headers)
        elif content_type is not None and content_type.startswith(APPLICATION_JSON):
            response = JSON.unmarshal(str(resp.content, UTF_8), GetImageResponse)

        response.raw = resp
        return response

    async def aget(self, request: GetImageRequest, option: Optional[RequestOption] = None) -> GetImageResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Send the request
        resp: RawResponse = await Transport.aexecute(self.config, request, option)

        # Handle the binary stream
        content_type = resp.headers.get(CONTENT_TYPE)
        response: GetImageResponse = GetImageResponse()
        if 200 <= resp.status_code < 300:
            response.code = 0
            response.file = io.BytesIO(resp.content)
            response.file_name = Files.parse_file_name(resp.headers)
        elif content_type is not None and content_type.startswith(APPLICATION_JSON):
            response = JSON.unmarshal(str(resp.content, UTF_8), GetImageResponse)

        response.raw = resp
        return response
