from lark_channel.core.enum import AccessTokenType, HttpMethod
from lark_channel.core.model import BaseRequest


def build_wiki_node_get_request(*, token: str) -> BaseRequest:
    req = BaseRequest()
    req.http_method = HttpMethod.GET
    req.uri = "/open-apis/wiki/v2/spaces/get_node"
    req.token_types = {AccessTokenType.TENANT, AccessTokenType.USER}
    req.add_query("token", token)
    return req
