import asyncio
import base64
import http
import inspect
import ipaddress
import random
import time
from typing import Any, Callable, Dict, Mapping, Optional, TYPE_CHECKING
from urllib.parse import urlparse, parse_qs

import requests
import websockets
from websockets.exceptions import InvalidHandshake

from lark_channel.core.cache import ExpiringCache
from lark_channel.core.const import UTF_8, FEISHU_DOMAIN, USER_AGENT
from lark_channel.core.enum import LogLevel
from lark_channel.core.json import JSON
from lark_channel.core.log import logger
from lark_channel.core.utils import Strings
from lark_channel.core.utils.user_agent import build_user_agent
from lark_channel.event.dispatcher_handler import EventDispatcherHandler
from lark_channel.event.security import (
    REASON_WS_FRAGMENT_LIMIT,
    REASON_WS_INSECURE_SCHEME,
    REASON_WS_INVALID_TIMING,
    should_record_security_audit,
)
from lark_channel.ws.const import *
from lark_channel.ws.enum import FrameType, MessageType
from lark_channel.ws.exception import *
from lark_channel.ws.model import *
from lark_channel.ws.pb.google.protobuf.internal.containers import RepeatedCompositeFieldContainer
from lark_channel.ws.pb.pbbp2_pb2 import Frame

if TYPE_CHECKING:
    from lark_channel.channel.config import SecurityConfig


try:
    loop = asyncio.get_event_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)


def _get_by_key(headers: RepeatedCompositeFieldContainer, key: str) -> str:
    for header in headers:
        if header.key == key:
            return header.value

    raise HeaderNotFoundException(key)


def _new_ping_frame(service_id: int) -> Frame:
    frame = Frame()
    header = frame.headers.add()
    header.key = HEADER_TYPE
    header.value = MessageType.PING.value
    frame.service = service_id
    frame.method = FrameType.CONTROL.value
    frame.SeqID = 0
    frame.LogID = 0

    return frame


def _ordinal(n: int):
    suffixes = {1: 'st', 2: 'nd', 3: 'rd'}
    if 10 <= n <= 20:
        suffix = 'th'
    else:
        suffix = suffixes.get(n % 10, 'th')
    return str(n) + suffix


async def _select():
    while True:
        await asyncio.sleep(3600)


def _ws_connect_kwargs(proxy_url: Optional[str] = None, trust_env_proxy: Optional[bool] = None):
    params = inspect.signature(websockets.connect).parameters
    if "proxy" not in params:
        if proxy_url:
            raise RuntimeError("installed websockets does not support explicit proxy configuration")
        return {}
    if proxy_url:
        return {"proxy": proxy_url}
    if trust_env_proxy is True:
        return {}
    # websockets 15 enables environment proxy discovery by default. The SDK
    # historically connected directly, so preserve that behavior when the
    # parameter exists.
    return {"proxy": None}


def _endpoint_proxy_kwargs(proxy_url: Optional[str]) -> Dict[str, object]:
    if proxy_url:
        return {"proxies": {"http": proxy_url, "https": proxy_url}}
    return {}


def _post_endpoint(
    url: str,
    *,
    headers: Dict[str, str],
    body: Dict[str, str],
    timeout: Optional[float],
    proxy_url: Optional[str],
    trust_env_proxy: Optional[bool],
):
    kwargs = {}
    if timeout is not None:
        kwargs["timeout"] = timeout
    proxy_kwargs = _endpoint_proxy_kwargs(proxy_url)
    if trust_env_proxy is False:
        session = requests.Session()
        session.trust_env = False
        return session.post(url, headers=headers, json=body, **proxy_kwargs, **kwargs)
    return requests.post(
        url,
        headers=headers,
        json=body,
        **proxy_kwargs,
        **kwargs,
    )


def _get_ws_conn_exception_headers(e):
    headers = getattr(e, "headers", None)
    if headers is not None:
        return headers

    response = getattr(e, "response", None)
    if response is None:
        return None
    return getattr(response, "headers", None)


def _parse_ws_conn_exception(e):
    headers = _get_ws_conn_exception_headers(e)
    if headers is None:
        raise e

    code = headers.get(HEADER_HANDSHAKE_STATUS)
    msg = headers.get(HEADER_HANDSHAKE_MSG)
    if code is None or msg is None:
        raise e

    code = int(code)
    if code == AUTH_FAILED:
        auth_code = headers.get(HEADER_HANDSHAKE_AUTH_ERRCODE)
        if int(auth_code) == EXCEED_CONN_LIMIT:
            raise ClientException(code, msg)
        else:
            raise ServerException(code, msg)
    elif code == FORBIDDEN:
        raise ClientException(code, msg)
    else:
        raise ServerException(code, msg)


class Client(object):
    def __init__(self,
                 app_id: str,
                 app_secret,
                 log_level: LogLevel = LogLevel.INFO,
                 event_handler: EventDispatcherHandler = None,
                 domain: str = FEISHU_DOMAIN,
                 auto_reconnect: bool = True,
                 source: Optional[str] = None,
                 extra_ua_tags: Optional[list] = None,
                 headers: Optional[Mapping[str, str]] = None,
                 proxy_url: Optional[str] = None,
                 trust_env_proxy: Optional[bool] = None,
                 handshake_timeout: Optional[float] = None,
                 security: Optional["SecurityConfig"] = None) -> None:
        self._app_id: str = app_id
        self._app_secret: str = app_secret
        self._log_level: LogLevel = log_level
        self._event_handler: EventDispatcherHandler = event_handler
        self._auto_reconnect: bool = auto_reconnect
        self._domain: str = domain
        self._headers: Dict[str, str] = dict(headers or {})
        self._proxy_url = proxy_url
        self._trust_env_proxy = trust_env_proxy
        self._handshake_timeout = handshake_timeout
        self._security = security or _default_security_config()
        # UA used on the endpoint-discovery POST (and any future HTTP/WS
        # handshakes from this client). ``extra_ua_tags`` is internal; higher
        # level modules may pass a short product tag here.
        self._user_agent: str = build_user_agent(source=source, extra_tags=extra_ua_tags)
        self._conn: Optional[websockets.WebSocketClientProtocol] = None
        self._conn_url: str = ""
        self._service_id: str = ""
        self._conn_id: str = ""
        self._last_activity_at: float = 0.0
        self._pong_waiter: Optional[asyncio.Future[bool]] = None
        self._loop = loop
        self._reconnect_task = None
        # Local defaults; the Feishu WS endpoint authoritatively replaces these
        # via _configure() on every handshake (and may push updates mid-session
        # via CONTROL frames). User-facing overrides are intentionally not
        # exposed because the server replaces them.
        self._reconnect_nonce: int = 30
        self._reconnect_count: int = -1
        self._reconnect_interval: int = 120
        self._ping_interval: int = 120
        self._cache: ExpiringCache = ExpiringCache(clear_interval=30)
        self._lock = asyncio.Lock()
        self._reconnect_lock = asyncio.Lock()
        self._handler_semaphore = (
            asyncio.Semaphore(self._security.max_concurrent_ws_handlers)
            if self._security.max_concurrent_ws_handlers is not None
            else None
        )
        # Observer hooks for higher-level wrappers (e.g. FeishuChannel) to
        # react to reconnect lifecycle. ``on_reconnecting`` fires when the
        # client decides a connection was lost and starts retrying;
        # ``on_reconnected`` fires on the first successful re-establishment.
        # Both default to no-op so existing callers see no behaviour change.
        self.on_reconnecting: Callable[[], None] = lambda: None
        self.on_reconnected: Callable[[], None] = lambda: None
        logger.setLevel(log_level.value)

    @property
    def last_activity_at(self) -> float:
        """最近一次收到任意 WS 帧的 monotonic 时间戳；0 表示尚未收到。"""
        return self._last_activity_at

    async def probe_live(self, *, timeout: float) -> bool:
        """True 当 ping/pong 往返在 timeout 内完成；连接缺失/超时返回 False。"""
        if self._conn is None:
            return False
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[bool] = loop.create_future()
        self._pong_waiter = fut
        try:
            frame = _new_ping_frame(int(self._service_id))
            await self._write_message(frame.SerializeToString())
            await asyncio.wait_for(asyncio.shield(fut), timeout)
            return True
        except asyncio.TimeoutError:
            logger.warning(self._fmt_log("probe_live timed out after %ss", timeout))
            return False
        except Exception as e:  # noqa: BLE001 — probe 必须失败闭合
            logger.warning(self._fmt_log("probe_live failed, err: {}", e))
            return False
        finally:
            self._pong_waiter = None

    def start(self) -> None:
        try:
            loop.run_until_complete(self._connect())
        except ClientException as e:
            logger.error(self._fmt_log("connect failed, err: {}", e))
            raise e
        except Exception as e:
            logger.error(self._fmt_log("connect failed, err: {}", e))
            if self._auto_reconnect:
                loop.run_until_complete(self._disconnect_and_reconnect())
            else:
                loop.run_until_complete(self._disconnect())
                raise e

        loop.create_task(self._ping_loop())
        loop.run_until_complete(_select())

    async def _ping_loop(self):
        while True:
            try:
                if self._conn is not None:
                    frame = _new_ping_frame(int(self._service_id))
                    await self._write_message(frame.SerializeToString())
                    logger.debug(self._fmt_log("ping success"))
            except Exception as e:
                logger.warn(self._fmt_log("ping failed, err: {}", e))
            finally:
                await asyncio.sleep(self._ping_interval)

    async def _connect(self) -> None:
        async with self._lock:
            if self._conn is not None:
                return
            try:
                if self._handshake_timeout is None:
                    conn_url = self._get_conn_url()
                else:
                    conn_url = self._get_conn_url(timeout=self._handshake_timeout)
                u = urlparse(conn_url)
                self._validate_conn_url(u, conn_url)
                q = parse_qs(u.query)
                conn_id = q[DEVICE_ID][0]
                service_id = q[SERVICE_ID][0]

                connect_kwargs = _ws_connect_kwargs(self._proxy_url, self._trust_env_proxy)
                if self._handshake_timeout is not None:
                    connect_kwargs["open_timeout"] = self._handshake_timeout
                conn = await websockets.connect(conn_url, **connect_kwargs)
                self._conn = conn
                self._conn_url = conn_url
                self._conn_id = conn_id
                self._service_id = service_id

                logger.info(self._fmt_log("connected to {}", conn_url))
                loop.create_task(self._receive_message_loop(conn))
            except InvalidHandshake as e:
                _parse_ws_conn_exception(e)

    async def _receive_message_loop(self, conn):
        try:
            while True:
                msg = await conn.recv()
                await self._schedule_handle_message(msg)
        except Exception as e:
            logger.error(self._fmt_log("receive message loop exit, err: {}", e))
            if self._auto_reconnect:
                await self._disconnect_and_reconnect(expected_conn=conn)
            else:
                await self._disconnect(expected_conn=conn)
                raise e

    async def _schedule_handle_message(self, msg) -> None:
        if self._handler_semaphore is None:
            loop.create_task(self._handle_message(msg))
            return
        await self._handler_semaphore.acquire()
        loop.create_task(self._handle_message_with_limit(msg))

    async def _handle_message_with_limit(self, msg) -> None:
        try:
            await self._handle_message(msg)
        finally:
            self._handler_semaphore.release()

    def _get_conn_url(self, *, timeout: Optional[float] = None) -> str:
        if Strings.is_empty(self._app_id) or Strings.is_empty(self._app_secret):
            raise ClientException(NO_CREDENTIAL, "app_id or app_secret is null")

        headers = dict(self._headers)
        headers.update({
            "locale": "zh",
            USER_AGENT: self._user_agent,
        })
        response = _post_endpoint(
            self._domain + GEN_ENDPOINT_URI,
            headers=headers,
            body={
                "AppID": self._app_id,
                "AppSecret": self._app_secret,
            },
            timeout=timeout,
            proxy_url=self._proxy_url,
            trust_env_proxy=self._trust_env_proxy,
        )
        if response.status_code != http.HTTPStatus.OK:
            raise ServerException(response.status_code, "system busy")

        resp = JSON.unmarshal(str(response.content, UTF_8), EndpointResp)
        if resp.code == OK:
            pass
        elif resp.code == SYSTEM_BUSY:
            raise ServerException(resp.code, "system busy")
        elif resp.code == INTERNAL_ERROR:
            raise ServerException(resp.code, resp.msg)
        else:
            raise ClientException(resp.code, resp.msg)

        data = resp.data
        if data.ClientConfig is not None:
            self._configure(data.ClientConfig)

        return data.URL

    def _validate_conn_url(self, parsed, conn_url: str) -> None:
        scheme = (parsed.scheme or "").lower()
        if scheme == "wss":
            return
        if scheme != "ws":
            raise ClientException(FORBIDDEN, f"invalid websocket endpoint scheme: {scheme}")
        if self._is_local_insecure_ws(parsed) and self._security.allow_local_insecure_ws:
            return
        self._record_security_audit(
            REASON_WS_INSECURE_SCHEME,
            action=(
                "allow"
                if self._security.is_strict and self._security.allow_insecure_ws
                else "block"
                if self._security.is_strict
                else "legacy_flow"
            ),
            details={"url": conn_url},
        )
        if self._security.is_strict and not self._security.allow_insecure_ws:
            raise ClientException(FORBIDDEN, "insecure websocket endpoint")

    def _is_local_insecure_ws(self, parsed) -> bool:
        hostname = parsed.hostname
        if hostname is None:
            return False
        if hostname.lower() == "localhost":
            return True
        try:
            return ipaddress.ip_address(hostname).is_loopback
        except ValueError:
            return False

    def _set_loop(self, loop_) -> None:
        self._loop = loop_
        self._reconnect_task = None

    def probe_endpoint(self, *, timeout: float) -> bool:
        try:
            self._get_conn_url(timeout=timeout)
            return True
        except Exception as e:
            logger.warning(self._fmt_log("endpoint probe failed, err: {}", e))
            return False

    def request_reconnect(self) -> None:
        active_loop = getattr(self, "_loop", None)
        if active_loop is None or active_loop.is_closed() or not active_loop.is_running():
            return
        active_loop.call_soon_threadsafe(self._ensure_reconnect_task)

    def _ensure_reconnect_task(self) -> None:
        task = getattr(self, "_reconnect_task", None)
        if task is not None and not task.done():
            return
        self._reconnect_task = asyncio.create_task(self._request_reconnect_once())
        self._reconnect_task.add_done_callback(self._log_reconnect_task)

    def _log_reconnect_task(self, task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.warning(self._fmt_log("requested reconnect failed, err: {}", exc))

    async def _request_reconnect_once(self) -> None:
        await self._disconnect_and_reconnect()

    async def _handle_message(self, msg: bytes) -> None:
        try:
            frame = Frame()
            frame.ParseFromString(msg)
            ft = FrameType(frame.method)

            if ft == FrameType.CONTROL:
                await self._handle_control_frame(frame)
            elif ft == FrameType.DATA:
                await self._handle_data_frame(frame)
        except Exception as e:
            logger.error(self._fmt_log("handle message failed, err: {}", e))

    async def _handle_control_frame(self, frame: Frame):
        self._last_activity_at = time.monotonic()
        hs = frame.headers
        type_ = _get_by_key(hs, HEADER_TYPE)
        message_type = MessageType(type_)

        if message_type == MessageType.PING:
            return
        elif message_type == MessageType.PONG:
            logger.debug(self._fmt_log("receive pong"))
            waiter = self._pong_waiter
            if waiter is not None and not waiter.done():
                waiter.set_result(True)
            if not frame.payload:
                return
            conf = JSON.unmarshal(str(frame.payload, UTF_8), ClientConfig)
            self._configure(conf)

    async def _handle_data_frame(self, frame: Frame):
        self._last_activity_at = time.monotonic()
        hs = frame.headers
        msg_id = _get_by_key(hs, HEADER_MESSAGE_ID)
        trace_id = _get_by_key(hs, HEADER_TRACE_ID)
        sum_ = _get_by_key(hs, HEADER_SUM)
        seq = _get_by_key(hs, HEADER_SEQ)
        type_ = _get_by_key(hs, HEADER_TYPE)

        pl = frame.payload
        if int(sum_) > 1:
            # Reassemble the multi-part packet
            pl = self._combine(msg_id, int(sum_), int(seq), pl)
            if pl is None:
                return

        message_type = MessageType(type_)
        logger.info(self._fmt_log("receive message, message_type: {}, message_id: {}, trace_id: {}, payload_len: {}",
                                  message_type.value, msg_id, trace_id, len(pl)))

        resp = Response(code=http.HTTPStatus.OK)
        try:
            start = int(round(time.time() * 1000))
            if message_type == MessageType.EVENT:
                result = self._event_handler._do_without_validation(pl)
            elif message_type == MessageType.CARD:
                return
            else:
                return
            end = int(round(time.time() * 1000))
            header = hs.add()
            header.key = HEADER_BIZ_RT
            header.value = str(end - start)
            if result is not None:
                resp.data = base64.b64encode(JSON.marshal(result).encode(UTF_8))
        except Exception as e:
            logger.error(
                self._fmt_log("handle message failed, message_type: {}, message_id: {}, trace_id: {}, err: {}",
                              message_type.value, msg_id, trace_id, e))
            resp = Response(code=http.HTTPStatus.INTERNAL_SERVER_ERROR)

        frame.payload = JSON.marshal(resp).encode(UTF_8)
        await self._write_message(frame.SerializeToString())

    async def _reconnect(self):
        async with self._reconnect_lock:
            await self._reconnect_locked()

    async def _disconnect_and_reconnect(self, *, expected_conn=None):
        async with self._reconnect_lock:
            disconnected = await self._disconnect(expected_conn=expected_conn)
            if expected_conn is not None and not disconnected:
                return False
            await self._reconnect_locked()
            return True

    async def _reconnect_locked(self):
        # Notify subscribers that we're about to try reconnecting. Wrapped in
        # try/except so a misbehaving observer can never derail reconnect.
        try:
            self.on_reconnecting()
        except Exception as e:  # pragma: no cover - defensive
            logger.warning(self._fmt_log("on_reconnecting callback raised: {}", e))

        # Random jitter before the first reconnect
        if self._reconnect_nonce > 0:
            nonce = random.random() * self._reconnect_nonce
            await asyncio.sleep(nonce)

        # Reconnect
        if self._reconnect_count >= 0:
            for i in range(self._reconnect_count):
                if await self._try_connect(i):
                    self._fire_on_reconnected()
                    return
                await asyncio.sleep(self._reconnect_interval)
            raise ServerUnreachableException(
                f"unable to connect to the server after trying {self._reconnect_count} times")
        else:
            i = 0
            while True:
                if await self._try_connect(i):
                    self._fire_on_reconnected()
                    return
                await asyncio.sleep(self._reconnect_interval)
                i += 1

    def _fire_on_reconnected(self) -> None:
        try:
            self.on_reconnected()
        except Exception as e:  # pragma: no cover - defensive
            logger.warning(self._fmt_log("on_reconnected callback raised: {}", e))

    async def _try_connect(self, cnt: int) -> bool:
        logger.info(self._fmt_log("trying to reconnect for the {} time", _ordinal(cnt + 1)))
        try:
            await self._connect()
            return True
        except ClientException as e:
            logger.error(self._fmt_log("connect failed, err: {}", e))
            raise e
        except Exception as e:
            logger.error(self._fmt_log("connect failed, err: {}", e))
            return False

    async def _disconnect(self, *, expected_conn=None) -> bool:
        async with self._lock:
            if self._conn is None:
                return False
            if expected_conn is not None and self._conn is not expected_conn:
                return False
            try:
                await self._conn.close()
                logger.info(self._fmt_log("disconnected to {}", self._conn_url))
            finally:
                self._conn = None
                self._conn_url = ""
                self._conn_id = ""
                self._service_id = ""
            return True

    async def _write_message(self, data: bytes):
        async with self._lock:
            if self._conn is None:
                raise ConnectionClosedException("connection is closed, write message failed")
            await self._conn.send(data)

    def _combine(self, msg_id: str, sum_: int, seq: int, bs: bytes) -> Optional[bytes]:
        if sum_ <= 1:
            return bs
        if seq < 0 or seq >= sum_:
            self._record_security_audit(
                REASON_WS_FRAGMENT_LIMIT,
                action="drop",
                details={"message_id": msg_id, "sum": sum_, "seq": seq},
            )
            return None
        if self._fragment_parts_limit_exceeded(sum_):
            self._record_security_audit(
                REASON_WS_FRAGMENT_LIMIT,
                action="drop" if self._should_drop_resource_overflow() else "would_block",
                details={"message_id": msg_id, "sum": sum_, "seq": seq, "bytes": len(bs)},
            )
            if self._should_drop_resource_overflow():
                return None

        val = self._cache.get(msg_id)
        if not isinstance(val, dict) or val.get("sum") != sum_:
            val = {"sum": sum_, "parts": {}, "total_bytes": 0}

        parts = val["parts"]
        old = parts.get(seq)
        if old is None:
            val["total_bytes"] += len(bs)
        else:
            val["total_bytes"] += len(bs) - len(old)
        if self._fragment_bytes_limit_exceeded(val["total_bytes"]):
            self._record_security_audit(
                REASON_WS_FRAGMENT_LIMIT,
                action="drop" if self._should_drop_resource_overflow() else "would_block",
                details={
                    "message_id": msg_id,
                    "sum": sum_,
                    "seq": seq,
                    "bytes": val["total_bytes"],
                },
            )
            if self._should_drop_resource_overflow():
                return None
        parts[seq] = bs
        if len(parts) < sum_:
            self._cache.set(msg_id, val, 5)
            return None

        pl = b"".join(parts[i] for i in range(sum_))
        return pl

    def _configure(self, conf: ClientConfig) -> None:
        self._reconnect_count = self._normalize_reconnect_count(conf.ReconnectCount)
        self._reconnect_interval = self._normalize_interval(
            conf.ReconnectInterval,
            default=120,
            field_name="ReconnectInterval",
        )
        self._reconnect_nonce = self._normalize_interval(
            conf.ReconnectNonce,
            default=30,
            field_name="ReconnectNonce",
        )
        self._ping_interval = self._normalize_interval(
            conf.PingInterval,
            default=120,
            field_name="PingInterval",
        )

    def _fragment_parts_limit_exceeded(self, parts: int) -> bool:
        max_parts = self._security.max_ws_fragment_parts
        return max_parts is not None and parts > max_parts

    def _fragment_bytes_limit_exceeded(self, total_bytes: int) -> bool:
        max_bytes = self._security.max_ws_fragment_bytes
        return max_bytes is not None and total_bytes > max_bytes

    def _should_drop_resource_overflow(self) -> bool:
        return self._security.is_strict or self._security.resource_overflow_policy == "drop"

    def _normalize_interval(
        self,
        value: Any,
        *,
        default: int,
        field_name: str,
    ) -> int:
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0 or value > 86400:
            self._record_security_audit(
                REASON_WS_INVALID_TIMING,
                action="fallback",
                details={"field": field_name, "value": value, "default": default},
            )
            return default
        return value

    def _normalize_reconnect_count(self, value: Any) -> int:
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value < -1
            or value > 10000
        ):
            self._record_security_audit(
                REASON_WS_INVALID_TIMING,
                action="fallback",
                details={"field": "ReconnectCount", "value": value, "default": -1},
            )
            return -1
        return value

    def _record_security_audit(
        self,
        reason: str,
        *,
        action: str,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        if not should_record_security_audit(self._security):
            return
        self._security.audit_recorder.record(
            reason,
            mode=self._security.mode,
            action=action,
            details=details,
        )

    def _fmt_log(self, fmt: str, *args) -> str:
        log = fmt.format(*args)
        if self._conn_id != "":
            log += f' [conn_id={self._conn_id}]'

        return log


def _default_security_config():
    from lark_channel.channel.config import SecurityConfig

    return SecurityConfig()
