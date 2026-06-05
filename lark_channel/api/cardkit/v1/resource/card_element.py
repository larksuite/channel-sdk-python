import io
from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.const import UTF_8, CONTENT_TYPE, APPLICATION_JSON
from lark_channel.core import JSON
from lark_channel.core.token import verify
from lark_channel.core.http import Transport
from lark_channel.core.model import Config, RequestOption, RawResponse
from lark_channel.core.utils import Files
from requests_toolbelt import MultipartEncoder
from ..model.content_card_element_request import ContentCardElementRequest
from ..model.content_card_element_response import ContentCardElementResponse
from ..model.create_card_element_request import CreateCardElementRequest
from ..model.create_card_element_response import CreateCardElementResponse
from ..model.delete_card_element_request import DeleteCardElementRequest
from ..model.delete_card_element_response import DeleteCardElementResponse
from ..model.patch_card_element_request import PatchCardElementRequest
from ..model.patch_card_element_response import PatchCardElementResponse
from ..model.update_card_element_request import UpdateCardElementRequest
from ..model.update_card_element_response import UpdateCardElementResponse


class CardElement(object):
    def __init__(self, config: Config) -> None:
        self.config: Config = config

    def content(self, request: ContentCardElementRequest,
                option: Optional[RequestOption] = None) -> ContentCardElementResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Add content-type
        if request.body is not None:
            option.headers[CONTENT_TYPE] = f"{APPLICATION_JSON}; charset=utf-8"

        # Send the request
        resp: RawResponse = Transport.execute(self.config, request, option)

        # Deserialize the response
        response: ContentCardElementResponse = JSON.unmarshal(str(resp.content, UTF_8), ContentCardElementResponse)
        response.raw = resp

        return response

    async def acontent(self, request: ContentCardElementRequest,
                       option: Optional[RequestOption] = None) -> ContentCardElementResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Send the request
        resp: RawResponse = await Transport.aexecute(self.config, request, option)

        # Deserialize the response
        response: ContentCardElementResponse = JSON.unmarshal(str(resp.content, UTF_8), ContentCardElementResponse)
        response.raw = resp

        return response

    def create(self, request: CreateCardElementRequest,
               option: Optional[RequestOption] = None) -> CreateCardElementResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Add content-type
        if request.body is not None:
            option.headers[CONTENT_TYPE] = f"{APPLICATION_JSON}; charset=utf-8"

        # Send the request
        resp: RawResponse = Transport.execute(self.config, request, option)

        # Deserialize the response
        response: CreateCardElementResponse = JSON.unmarshal(str(resp.content, UTF_8), CreateCardElementResponse)
        response.raw = resp

        return response

    async def acreate(self, request: CreateCardElementRequest,
                      option: Optional[RequestOption] = None) -> CreateCardElementResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Send the request
        resp: RawResponse = await Transport.aexecute(self.config, request, option)

        # Deserialize the response
        response: CreateCardElementResponse = JSON.unmarshal(str(resp.content, UTF_8), CreateCardElementResponse)
        response.raw = resp

        return response

    def delete(self, request: DeleteCardElementRequest,
               option: Optional[RequestOption] = None) -> DeleteCardElementResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Add content-type
        if request.body is not None:
            option.headers[CONTENT_TYPE] = f"{APPLICATION_JSON}; charset=utf-8"

        # Send the request
        resp: RawResponse = Transport.execute(self.config, request, option)

        # Deserialize the response
        response: DeleteCardElementResponse = JSON.unmarshal(str(resp.content, UTF_8), DeleteCardElementResponse)
        response.raw = resp

        return response

    async def adelete(self, request: DeleteCardElementRequest,
                      option: Optional[RequestOption] = None) -> DeleteCardElementResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Send the request
        resp: RawResponse = await Transport.aexecute(self.config, request, option)

        # Deserialize the response
        response: DeleteCardElementResponse = JSON.unmarshal(str(resp.content, UTF_8), DeleteCardElementResponse)
        response.raw = resp

        return response

    def patch(self, request: PatchCardElementRequest,
              option: Optional[RequestOption] = None) -> PatchCardElementResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Add content-type
        if request.body is not None:
            option.headers[CONTENT_TYPE] = f"{APPLICATION_JSON}; charset=utf-8"

        # Send the request
        resp: RawResponse = Transport.execute(self.config, request, option)

        # Deserialize the response
        response: PatchCardElementResponse = JSON.unmarshal(str(resp.content, UTF_8), PatchCardElementResponse)
        response.raw = resp

        return response

    async def apatch(self, request: PatchCardElementRequest,
                     option: Optional[RequestOption] = None) -> PatchCardElementResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Send the request
        resp: RawResponse = await Transport.aexecute(self.config, request, option)

        # Deserialize the response
        response: PatchCardElementResponse = JSON.unmarshal(str(resp.content, UTF_8), PatchCardElementResponse)
        response.raw = resp

        return response

    def update(self, request: UpdateCardElementRequest,
               option: Optional[RequestOption] = None) -> UpdateCardElementResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Add content-type
        if request.body is not None:
            option.headers[CONTENT_TYPE] = f"{APPLICATION_JSON}; charset=utf-8"

        # Send the request
        resp: RawResponse = Transport.execute(self.config, request, option)

        # Deserialize the response
        response: UpdateCardElementResponse = JSON.unmarshal(str(resp.content, UTF_8), UpdateCardElementResponse)
        response.raw = resp

        return response

    async def aupdate(self, request: UpdateCardElementRequest,
                      option: Optional[RequestOption] = None) -> UpdateCardElementResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Send the request
        resp: RawResponse = await Transport.aexecute(self.config, request, option)

        # Deserialize the response
        response: UpdateCardElementResponse = JSON.unmarshal(str(resp.content, UTF_8), UpdateCardElementResponse)
        response.raw = resp

        return response
