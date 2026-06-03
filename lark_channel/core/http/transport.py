import json
import logging
import urllib.parse
import inspect
from typing import Dict, Optional

import httpx
import requests
from requests_toolbelt import MultipartEncoder

from lark_channel.core.const import *
from lark_channel.core.json import JSON
from lark_channel.core.log import (
    logger,
    redact_files_for_log,
    redact_for_log,
    redact_query_params_for_log,
)
from lark_channel.core.model import *
from lark_channel.core.utils.user_agent import build_user_agent


class Transport(object):

    @staticmethod
    def execute(conf: Config, req: BaseRequest, option: Optional[RequestOption] = None) -> RawResponse:
        if option is None:
            option = RequestOption()

        # Build the URL
        url: str = _build_url(conf.domain, req.uri, req.paths)

        # Assemble the headers
        headers: Dict[str, str] = _build_header(req, option, conf)

        data = req.body
        if data is not None and not isinstance(data, MultipartEncoder):
            data = JSON.marshal(req.body).encode(UTF_8)

        request, proxy_kwargs = _sync_requester(conf)
        response = request(
            str(req.http_method.name),
            url,
            headers=req.headers,
            params=req.queries,
            data=data,
            timeout=conf.timeout,
            **proxy_kwargs,
        )

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"{str(req.http_method.name)} {url} {response.status_code}, "
                         f"headers: {JSON.marshal(redact_for_log(headers))}, "
                         f"params: {JSON.marshal(redact_query_params_for_log(req.queries))}, "
                         f"body: {JSON.marshal(redact_for_log(data))}")

        resp = RawResponse()
        resp.status_code = response.status_code
        resp.headers = dict(response.headers)
        resp.content = response.content

        return resp

    @staticmethod
    async def aexecute(conf: Config, req: BaseRequest, option: Optional[RequestOption] = None) -> RawResponse:
        if option is None:
            option = RequestOption()

        # Build the URL
        url: str = _build_url(conf.domain, req.uri, req.paths)

        # Assemble the headers
        headers: Dict[str, str] = _build_header(req, option, conf)

        json_, files, data = None, None, None
        if req.files:
            # multipart/form-data
            files = req.files
            if req.body is not None:
                data = json.loads(JSON.marshal(req.body))
        elif req.body is not None:
            # application/json
            json_ = json.loads(JSON.marshal(req.body))

        async with httpx.AsyncClient(**_httpx_client_kwargs(conf)) as client:
            response = await client.request(
                str(req.http_method.name),
                url,
                headers=req.headers,
                params=req.queries,
                json=json_,
                data=data,
                files=files,
                timeout=conf.timeout,
            )

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"{str(req.http_method.name)} {url} {response.status_code}"
                    f"{f', headers: {JSON.marshal(redact_for_log(headers))}' if headers else ''}"
                    f"{f', params: {JSON.marshal(redact_query_params_for_log(req.queries))}' if req.queries else ''}"
                    f"{f', body: {JSON.marshal(_redact_body_for_log(json_, files, data))}' if json_ or files or data else ''}"
                )

            resp = RawResponse()
            resp.status_code = response.status_code
            resp.headers = dict(response.headers)
            resp.content = response.content

            return resp


def _build_url(domain: str, uri: str, paths: Dict[str, str]) -> str:
    if paths is None:
        paths = {}
    for key in paths:
        # Path params must be URL-encoded; safe='' prevents '/', '?', '#'
        # from passing through unencoded (path traversal / query injection).
        value = paths[key]
        if value is None:
            value = ""
        encoded = urllib.parse.quote(str(value), safe="")
        uri = uri.replace(":" + key, encoded)

    return domain + uri


def _proxy_kwargs(proxy_url: Optional[str]) -> Dict[str, object]:
    if not proxy_url:
        return {}
    return {"proxies": {"http": proxy_url, "https": proxy_url}}


def _sync_requester(conf: Config):
    proxy_url = getattr(conf, "proxy_url", None)
    trust_env = getattr(conf, "trust_env_proxy", None)
    if trust_env is False:
        session = requests.Session()
        session.trust_env = False
        return session.request, _proxy_kwargs(proxy_url)
    return requests.request, _proxy_kwargs(proxy_url)


def _httpx_client_kwargs(conf: Config) -> Dict[str, object]:
    proxy_url = getattr(conf, "proxy_url", None)
    trust_env = getattr(conf, "trust_env_proxy", None)
    kwargs: Dict[str, object] = {}
    if proxy_url:
        params = inspect.signature(httpx.AsyncClient).parameters
        if "proxy" in params:
            kwargs["proxy"] = proxy_url
        elif "proxies" in params:
            kwargs["proxies"] = proxy_url
        else:
            raise RuntimeError("installed httpx does not support explicit proxy configuration")
    if trust_env is not None:
        kwargs["trust_env"] = trust_env
    return kwargs


def _redact_body_for_log(json_, files, data):
    return _merge_dicts(
        redact_for_log(json_),
        redact_files_for_log(files),
        redact_for_log(data),
    )


def _build_header(request: BaseRequest, option: RequestOption, conf: Optional[Config] = None) -> Dict[str, str]:
    headers = request.headers

    # Add the User-Agent
    source = getattr(conf, "source", None) if conf is not None else None
    extra_tags = getattr(conf, "extra_ua_tags", None) if conf is not None else None
    headers[USER_AGENT] = build_user_agent(source=source, extra_tags=extra_tags)

    # Append extra headers
    if option.headers is not None:
        for key in option.headers:
            headers[key] = option.headers[key]

    # Add the token
    for token_type in request.token_types:
        if AccessTokenType.TENANT == token_type:
            headers[AUTHORIZATION] = f"Bearer {option.tenant_access_token}"
        elif AccessTokenType.APP == token_type:
            headers[AUTHORIZATION] = f"Bearer {option.app_access_token}"
        elif AccessTokenType.USER == token_type:
            headers[AUTHORIZATION] = f"Bearer {option.user_access_token}"

    return headers


def _merge_dicts(*dicts):
    res = {}
    for d in dicts:
        if d is not None:
            res.update(d)
    return res
