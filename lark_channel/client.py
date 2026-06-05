import urllib.parse
from typing import Optional

from .api.cardkit.service import CardkitService
from .api.contact.service import ContactService
from .api.im.service import ImService
from .core import JSON, APPLICATION_JSON, CONTENT_TYPE, UTF_8, logger
from .core.cache import ICache
from .core.enum import AppType, LogLevel
from .core.http import Transport
from .core.model import BaseRequest, BaseResponse, Config, RequestOption
from .core.token import TokenManager, verify
from .core.utils.files import Files


def _validate_domain(domain: str) -> None:
    """Reject domains that could redirect credential-bearing API traffic."""
    if not isinstance(domain, str) or not domain:
        raise ValueError("domain must be a non-empty https URL")
    parsed = urllib.parse.urlsplit(domain)
    if parsed.scheme == "http":
        host = (parsed.hostname or "").lower()
        if host not in ("127.0.0.1", "localhost", "::1"):
            raise ValueError(f"domain must use https scheme (got {domain!r})")
    elif parsed.scheme != "https":
        raise ValueError(f"domain must use https scheme (got {domain!r})")
    if not parsed.netloc:
        raise ValueError(f"domain missing host (got {domain!r})")
    if "@" in parsed.netloc:
        raise ValueError(f"domain must not embed credentials (got {domain!r})")
    if parsed.path or parsed.query or parsed.fragment:
        raise ValueError(
            "domain must be an origin (scheme + host[:port]) without "
            f"path/query/fragment (got {domain!r})"
        )


class Client(object):
    def __init__(self) -> None:
        self._config: Optional[Config] = None
        self.cardkit: Optional[CardkitService] = None
        self.contact: Optional[ContactService] = None
        self.im: Optional[ImService] = None

    @staticmethod
    def builder() -> "ClientBuilder":
        return ClientBuilder()

    @property
    def config(self) -> Optional[Config]:
        return self._config

    def request(
        self, request: BaseRequest, option: Optional[RequestOption] = None
    ) -> BaseResponse:
        if option is None:
            option = RequestOption()

        verify(self._config, request, option)
        raw_resp = Transport.execute(self._config, request, option)

        resp = BaseResponse()
        content_type = raw_resp.headers.get(CONTENT_TYPE)
        if content_type is not None and content_type.startswith(APPLICATION_JSON):
            resp = JSON.unmarshal(str(raw_resp.content, UTF_8), BaseResponse)
        elif 200 <= raw_resp.status_code < 300:
            resp.code = 0
        resp.raw = raw_resp
        return resp

    async def arequest(
        self, request: BaseRequest, option: Optional[RequestOption] = None
    ) -> BaseResponse:
        if option is None:
            option = RequestOption()

        verify(self._config, request, option)
        request.files = Files.extract_files(request.body)
        raw_resp = await Transport.aexecute(self._config, request, option)

        resp = BaseResponse()
        content_type = raw_resp.headers.get(CONTENT_TYPE)
        if content_type is not None and content_type.startswith(APPLICATION_JSON):
            resp = JSON.unmarshal(str(raw_resp.content, UTF_8), BaseResponse)
        elif 200 <= raw_resp.status_code < 300:
            resp.code = 0
        resp.raw = raw_resp
        return resp


class ClientBuilder(object):
    def __init__(self) -> None:
        self._config = Config()

    def app_id(self, app_id: str) -> "ClientBuilder":
        self._config.app_id = app_id
        return self

    def app_secret(self, app_secret: str) -> "ClientBuilder":
        self._config.app_secret = app_secret
        return self

    def domain(self, domain: str) -> "ClientBuilder":
        _validate_domain(domain)
        self._config.domain = domain
        return self

    def timeout(self, timeout: float) -> "ClientBuilder":
        self._config.timeout = timeout
        return self

    def proxy_url(self, proxy_url: Optional[str]) -> "ClientBuilder":
        self._config.proxy_url = proxy_url
        return self

    def trust_env_proxy(self, trust_env_proxy: Optional[bool]) -> "ClientBuilder":
        self._config.trust_env_proxy = trust_env_proxy
        return self

    def app_type(self, app_type: AppType) -> "ClientBuilder":
        self._config.app_type = app_type
        return self

    def app_ticket(self, app_ticket: str) -> "ClientBuilder":
        self._config.app_ticket = app_ticket
        return self

    def enable_set_token(self, enable_set_token: bool) -> "ClientBuilder":
        self._config.enable_set_token = enable_set_token
        return self

    def cache(self, cache: ICache) -> "ClientBuilder":
        self._config.cache = cache
        return self

    def log_level(self, log_level: LogLevel) -> "ClientBuilder":
        self._config.log_level = log_level
        return self

    def source(self, source: str) -> "ClientBuilder":
        self._config.source = source
        return self

    def build(self) -> Client:
        client = Client()
        client._config = self._config

        self._init_cache()
        self._init_logger()

        client.cardkit = CardkitService(self._config)
        client.contact = ContactService(self._config)
        client.im = ImService(self._config)
        return client

    def _init_cache(self) -> None:
        if self._config.cache is not None:
            TokenManager.cache = self._config.cache

    def _init_logger(self) -> None:
        logger.setLevel(int(self._config.log_level.value))
