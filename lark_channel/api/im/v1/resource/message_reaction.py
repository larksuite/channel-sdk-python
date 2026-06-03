import io
from typing import Any, Optional, Union, Dict, List, Set, IO, Callable, Type
from lark_channel.core.const import UTF_8, CONTENT_TYPE, APPLICATION_JSON
from lark_channel.core import JSON
from lark_channel.core.token import verify
from lark_channel.core.http import Transport
from lark_channel.core.model import Config, RequestOption, RawResponse
from lark_channel.core.utils import Files
from requests_toolbelt import MultipartEncoder
from ..model.create_message_reaction_request import CreateMessageReactionRequest
from ..model.create_message_reaction_response import CreateMessageReactionResponse
from ..model.delete_message_reaction_request import DeleteMessageReactionRequest
from ..model.delete_message_reaction_response import DeleteMessageReactionResponse
from ..model.list_message_reaction_request import ListMessageReactionRequest
from ..model.list_message_reaction_response import ListMessageReactionResponse


class MessageReaction(object):
    def __init__(self, config: Config) -> None:
        self.config: Config = config

    def create(self, request: CreateMessageReactionRequest,
               option: Optional[RequestOption] = None) -> CreateMessageReactionResponse:
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
        response: CreateMessageReactionResponse = JSON.unmarshal(str(resp.content, UTF_8),
                                                                 CreateMessageReactionResponse)
        response.raw = resp

        return response

    async def acreate(self, request: CreateMessageReactionRequest,
                      option: Optional[RequestOption] = None) -> CreateMessageReactionResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Send the request
        resp: RawResponse = await Transport.aexecute(self.config, request, option)

        # Deserialize the response
        response: CreateMessageReactionResponse = JSON.unmarshal(str(resp.content, UTF_8),
                                                                 CreateMessageReactionResponse)
        response.raw = resp

        return response

    def delete(self, request: DeleteMessageReactionRequest,
               option: Optional[RequestOption] = None) -> DeleteMessageReactionResponse:
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
        response: DeleteMessageReactionResponse = JSON.unmarshal(str(resp.content, UTF_8),
                                                                 DeleteMessageReactionResponse)
        response.raw = resp

        return response

    async def adelete(self, request: DeleteMessageReactionRequest,
                      option: Optional[RequestOption] = None) -> DeleteMessageReactionResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Send the request
        resp: RawResponse = await Transport.aexecute(self.config, request, option)

        # Deserialize the response
        response: DeleteMessageReactionResponse = JSON.unmarshal(str(resp.content, UTF_8),
                                                                 DeleteMessageReactionResponse)
        response.raw = resp

        return response

    def list(self, request: ListMessageReactionRequest,
             option: Optional[RequestOption] = None) -> ListMessageReactionResponse:
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
        response: ListMessageReactionResponse = JSON.unmarshal(str(resp.content, UTF_8), ListMessageReactionResponse)
        response.raw = resp

        return response

    async def alist(self, request: ListMessageReactionRequest,
                    option: Optional[RequestOption] = None) -> ListMessageReactionResponse:
        if option is None:
            option = RequestOption()

        # Authenticate and obtain a token
        verify(self.config, request, option)

        # Send the request
        resp: RawResponse = await Transport.aexecute(self.config, request, option)

        # Deserialize the response
        response: ListMessageReactionResponse = JSON.unmarshal(str(resp.content, UTF_8), ListMessageReactionResponse)
        response.raw = resp

        return response
