"""Executing meeting requests, and reporting what came back.

The result object exists because two callers need the platform's error code
rather than an exception: the liveness probe has to tell ``120004`` (the bot
is not a participant) from ``120003`` (a *user* is not) and from everything
else, and membership accounting has to tell "the meeting is gone" from "we do
not know". Raising would flatten all of that into one class.

Credential handling, in one place:

* every call builds a **fresh** ``BaseRequest`` and ``RequestOption``. The
  transport writes ``Authorization`` back onto the request object it was given,
  so a reused request carries the previous call's identity into the next one
  and keeps a token reachable on a long-lived object.
* the original transport exception is never stored or re-raised — it keeps the
  outgoing headers reachable. Only its type name survives, for diagnosis.
* the request and option are **scrubbed after the call**. A raised error carries
  ``__traceback__``, and every frame in it exposes its locals — so a request
  object still holding ``Authorization`` or a meeting password is reachable from
  the error object that a crash reporter walks, even though ``repr()`` is clean.
"""

import asyncio
from typing import Any, Dict, Optional

from lark_channel.core.http.transport import Transport
from lark_channel.core.json import JSON
from lark_channel.core.model import RequestOption
from lark_channel.core.token import auth as _token_auth

from ..errors import FeishuChannelError
from .errors import _first_console_url, build_api_error


#: Request-body fields that are credentials in their own right.
_CREDENTIAL_BODY_FIELDS = ("password",)
#: Every credential slot on a ``RequestOption``.
_CREDENTIAL_OPTION_FIELDS = (
    "user_access_token",
    "tenant_access_token",
    "app_access_token",
    "app_ticket",
)


def _scrub_credentials(request: Any, option: Any) -> None:
    """Remove credentials from a spent request and its options.

    Both objects are single-use, and both are locals of the frames an error
    unwinds through — which means they stay reachable through the raised
    error's ``__traceback__``. Clearing them here is the one place that covers
    every caller, including the ones that raise.
    """
    headers = getattr(request, "headers", None)
    if isinstance(headers, dict):
        for key in list(headers):
            if key.lower() == "authorization":
                headers.pop(key, None)
    body = getattr(request, "body", None)
    if isinstance(body, dict):
        for field in _CREDENTIAL_BODY_FIELDS:
            if field in body:
                body[field] = None
    # Every token slot, not just the user one: `core.token.auth.verify` writes
    # the freshly minted tenant token onto the option too, and both live on an
    # object that stays reachable from a raised error's frames.
    for slot in _CREDENTIAL_OPTION_FIELDS:
        if getattr(option, slot, None):
            setattr(option, slot, None)


class ApiResult:
    """What one meeting request produced.

    ``transport_error`` means the request never got an answer at all, which is
    the case that must never be read as "the bot is not in the meeting".
    """

    __slots__ = (
        "ok",
        "status",
        "feishu_code",
        "msg",
        "data",
        "transport_error",
        "console_url",
    )

    def __init__(
        self,
        *,
        ok: bool,
        status: Optional[int] = None,
        feishu_code: Optional[int] = None,
        msg: str = "",
        data: Optional[Dict[str, Any]] = None,
        transport_error: bool = False,
        console_url: Optional[str] = None,
    ) -> None:
        self.ok = ok
        self.status = status
        self.feishu_code = feishu_code
        self.msg = msg
        self.data = data or {}
        self.transport_error = transport_error
        # Only the already-validated link is carried, never the response body:
        # some meeting responses include a plaintext password, and a body kept
        # on a result object ends up wherever that object ends up.
        self.console_url = console_url

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "<ApiResult ok=%s status=%s code=%s>" % (
            self.ok,
            self.status,
            self.feishu_code,
        )


class MeetingApi:
    """Runs meeting requests against the channel's client."""

    def __init__(self, client: Any, *, timeout_seconds: float = 30.0) -> None:
        self._client = client
        self._timeout = timeout_seconds or 30.0

    async def call(
        self,
        request: Any,
        *,
        user_access_token: Optional[str] = None,
        what: str = "meeting request",
    ) -> ApiResult:
        """Execute ``request``; never raises for an API-level failure.

        ``user_access_token`` is attached to a fresh option for this one call
        and referenced nowhere else. It only takes effect because the channel
        builds its client with ``enable_set_token(True)`` and because the
        request declares exactly one token type.
        """
        option = RequestOption()
        if user_access_token:
            option.user_access_token = user_access_token
        try:
            resp = await asyncio.wait_for(
                self._run(request, option), timeout=self._timeout
            )
        except FeishuChannelError:
            raise
        except Exception as exc:
            # Deliberately drops `exc`: an httpx exception keeps the request's
            # Authorization header reachable through attribute walks.
            return ApiResult(
                ok=False, msg=type(exc).__name__, transport_error=True
            )
        finally:
            _scrub_credentials(request, option)
        return self._interpret(resp)

    async def _run(self, request: Any, option: RequestOption) -> Any:
        loop = asyncio.get_running_loop()
        # Token verification may fetch or refresh a tenant token over the
        # network; keep it off the event loop.
        await loop.run_in_executor(
            None, _token_auth.verify, self._client.config, request, option
        )
        return await Transport.aexecute(self._client.config, request, option)

    @staticmethod
    def _interpret(resp: Any) -> ApiResult:
        status = getattr(resp, "status_code", None)
        raw = getattr(resp, "content", None)
        body: Dict[str, Any] = {}
        if raw:
            try:
                parsed = JSON.unmarshal(raw.decode("utf-8"), dict)
                if isinstance(parsed, dict):
                    body = parsed
            except Exception:
                body = {}
        code = body.get("code")
        feishu_code = int(code) if isinstance(code, int) and code else None
        msg = body.get("msg") or ""
        http_ok = status is None or 200 <= int(status) < 300
        if feishu_code is None and http_ok:
            data = body.get("data")
            return ApiResult(
                ok=True,
                status=status,
                msg=msg,
                data=data if isinstance(data, dict) else {},
            )
        return ApiResult(
            ok=False,
            status=status,
            feishu_code=feishu_code,
            msg=msg,
            console_url=_first_console_url(body),
        )

    @staticmethod
    def error_for(result: ApiResult, *, what: str) -> FeishuChannelError:
        """The error to hand a caller, with credentials already stripped."""
        body: Dict[str, Any] = {}
        if result.feishu_code is not None:
            body["code"] = result.feishu_code
        if result.msg:
            body["msg"] = result.msg
        if result.console_url:
            body["console_url"] = result.console_url
        return build_api_error(
            what=what,
            status=result.status,
            body=body,
            transport_error=RuntimeError() if result.transport_error else None,
        )


__all__ = ["ApiResult", "MeetingApi"]
