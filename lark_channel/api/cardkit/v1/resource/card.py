import io
from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.const import UTF_8, CONTENT_TYPE, APPLICATION_JSON
from lark_channel.core import JSON
from lark_channel.core.token import verify
from lark_channel.core.http import Transport
from lark_channel.core.model import Config, RequestOption, RawResponse
from lark_channel.core.utils import Files
from requests_toolbelt import MultipartEncoder
from ..model.batch_update_card_request import BatchUpdateCardRequest
from ..model.batch_update_card_response import BatchUpdateCardResponse
from ..model.create_card_request import CreateCardRequest
from ..model.create_card_response import CreateCardResponse
from ..model.id_convert_card_request import IdConvertCardRequest
from ..model.id_convert_card_response import IdConvertCardResponse
from ..model.settings_card_request import SettingsCardRequest
from ..model.settings_card_response import SettingsCardResponse
from ..model.update_card_request import UpdateCardRequest
from ..model.update_card_response import UpdateCardResponse


class Card(object):
    def __init__(self, config: Config) -> None:
        self.config: Config = config

    def batch_update(self, request: BatchUpdateCardRequest,
                     option: Optional[RequestOption] = None) -> BatchUpdateCardResponse:
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
        response: BatchUpdateCardResponse = JSON.unmarshal(str(resp.content, UTF_8), BatchUpdateCardResponse)
        response.raw = resp

        return response

    async def abatch_update(self, request: BatchUpdateCardRequest,
                            option: Optional[RequestOption] = None) -> BatchUpdateCardResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Send the request
        resp: RawResponse = await Transport.aexecute(self.config, request, option)

        # Deserialize the response
        response: BatchUpdateCardResponse = JSON.unmarshal(str(resp.content, UTF_8), BatchUpdateCardResponse)
        response.raw = resp

        return response

    def create(self, request: CreateCardRequest, option: Optional[RequestOption] = None) -> CreateCardResponse:
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
        response: CreateCardResponse = JSON.unmarshal(str(resp.content, UTF_8), CreateCardResponse)
        response.raw = resp

        return response

    async def acreate(self, request: CreateCardRequest, option: Optional[RequestOption] = None) -> CreateCardResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Send the request
        resp: RawResponse = await Transport.aexecute(self.config, request, option)

        # Deserialize the response
        response: CreateCardResponse = JSON.unmarshal(str(resp.content, UTF_8), CreateCardResponse)
        response.raw = resp

        return response

    def id_convert(self, request: IdConvertCardRequest,
                   option: Optional[RequestOption] = None) -> IdConvertCardResponse:
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
        response: IdConvertCardResponse = JSON.unmarshal(str(resp.content, UTF_8), IdConvertCardResponse)
        response.raw = resp

        return response

    async def aid_convert(self, request: IdConvertCardRequest,
                          option: Optional[RequestOption] = None) -> IdConvertCardResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Send the request
        resp: RawResponse = await Transport.aexecute(self.config, request, option)

        # Deserialize the response
        response: IdConvertCardResponse = JSON.unmarshal(str(resp.content, UTF_8), IdConvertCardResponse)
        response.raw = resp

        return response

    def settings(self, request: SettingsCardRequest, option: Optional[RequestOption] = None) -> SettingsCardResponse:
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
        response: SettingsCardResponse = JSON.unmarshal(str(resp.content, UTF_8), SettingsCardResponse)
        response.raw = resp

        return response

    async def asettings(self, request: SettingsCardRequest,
                        option: Optional[RequestOption] = None) -> SettingsCardResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Send the request
        resp: RawResponse = await Transport.aexecute(self.config, request, option)

        # Deserialize the response
        response: SettingsCardResponse = JSON.unmarshal(str(resp.content, UTF_8), SettingsCardResponse)
        response.raw = resp

        return response

    def update(self, request: UpdateCardRequest, option: Optional[RequestOption] = None) -> UpdateCardResponse:
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
        response: UpdateCardResponse = JSON.unmarshal(str(resp.content, UTF_8), UpdateCardResponse)
        response.raw = resp

        return response

    async def aupdate(self, request: UpdateCardRequest, option: Optional[RequestOption] = None) -> UpdateCardResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Send the request
        resp: RawResponse = await Transport.aexecute(self.config, request, option)

        # Deserialize the response
        response: UpdateCardResponse = JSON.unmarshal(str(resp.content, UTF_8), UpdateCardResponse)
        response.raw = resp

        return response
