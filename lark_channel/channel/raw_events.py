"""Subscribing to Feishu event types the channel has not wrapped.

Without this there is no legitimate way to receive one: reaching into the
dispatcher's private tables breaks on the next release, and opening a second
socket for the same app is worse — Feishu splits delivery between connections,
so the channel's own message path starts losing events.

Two properties of the underlying dispatcher shape everything here.

**There are two tables, and the callback one is consulted first.** Events whose
return value goes back to Feishu (a card button, a link preview) live in a
separate map. A callback type registered into the plain map is never reached —
silently, with no error and no log line — and, worse, the callback answers with
whatever the plain path returns, which leaves the button dead in the user's
client.

**The whole table is rebuilt on every ``start()``.** A subscription that only
exists on the current dispatcher instance goes quiet after one restart. So
subscriptions live here, in a registry the channel owns, and are replayed onto
each new dispatcher.

Replay groups by event type and installs **one** processor per type. Registering
per handler would hit the dispatcher's duplicate-key guard on the second
handler for a type, and that exception would escape the rebuild and take
``start()`` down — the message path with it.

Security, stated because it cannot be inferred: a raw handler runs **after**
signature verification and decryption, so the payload is authentic. But it sits
**outside** the channel's safety pipeline — no policy gate, no dedup, no
processing lock, no loop guard. Subscribing to a type the channel already
handles therefore opens an unpoliced path into that type. That is what an
escape hatch is; it is deliberate, and it is pinned by a test so it stays a
documented property.
"""

import inspect
from typing import Any, Callable, Dict, List, Optional, Tuple

from lark_channel.core.log import logger

from . import _coerce
from .meeting.errors import sanitize_for_log
from .errors import FeishuChannelError, FeishuChannelErrorCode

Unsubscribe = Callable[[], None]

#: Event types whose return value is sent back to Feishu, with the builder
#: method that installs them and the empty response each one needs.
_CALLBACK_TYPES: Dict[str, Tuple[str, str]] = {
    "card.action.trigger": (
        "register_p2_card_action_trigger",
        "P2CardActionTriggerResponse",
    ),
    "url.preview.get": ("register_p2_url_preview_get", "P2URLPreviewGetResponse"),
}


def _empty_response(name: str) -> Any:
    from lark_channel.event.callback.model.p2_card_action_trigger import (
        P2CardActionTriggerResponse,
    )
    from lark_channel.event.callback.model.p2_url_preview_get import (
        P2URLPreviewGetResponse,
    )

    return {
        "P2CardActionTriggerResponse": P2CardActionTriggerResponse,
        "P2URLPreviewGetResponse": P2URLPreviewGetResponse,
    }[name]({})


class _RawEventProcessor:
    """Runs the built-in processor, if any, then the raw handlers.

    The built-in result is the one returned. A raw subscriber must not be able
    to change what Feishu is told — the return value of a card callback decides
    whether the button the user clicked does anything.
    """

    def __init__(self, *, inner: Any, dispatch: Callable[[Any], None], fallback_response=None):
        self._inner = inner
        self._dispatch = dispatch
        self._fallback_response = fallback_response

    def type(self):
        if self._inner is not None:
            return self._inner.type()
        from lark_channel.event.custom import CustomizedEvent

        return CustomizedEvent

    def do(self, data: Any) -> Any:
        result = None
        if self._inner is not None:
            result = self._inner.do(data)
        self._dispatch(data)
        if result is not None:
            return result
        if self._fallback_response is not None:
            return self._fallback_response()
        return None


def _builtin_under(existing: Any) -> Any:
    """The genuine built-in processor beneath ``existing``, if any.

    A previous installation of ours must be **replaced**, not wrapped: wrapping
    it would run its dispatch and the new one's, calling every handler once per
    layer. Only a processor we did not create counts as the built-in.
    """
    if isinstance(existing, _RawEventProcessor):
        return existing._inner
    return existing


class RawEventRegistry:
    """The channel's own record of raw subscriptions, replayed on each rebuild."""

    def __init__(self, *, schedule: Callable[[Any], Any], report: Callable[[BaseException], Any]):
        self._handlers: Dict[str, List[Callable]] = {}
        self._schedule = schedule
        self._report = report

    def subscribe(self, event_type: str, handler: Callable) -> Unsubscribe:
        if not isinstance(event_type, str) or not event_type:
            raise FeishuChannelError(
                FeishuChannelErrorCode.FORMAT_ERROR,
                "on_raw_event needs a Feishu event type",
            )
        if event_type.startswith("p1.") or event_type.startswith("p2."):
            # The schema prefix is added internally. Accepting one here would
            # build a key like `p2.p2.x`, which no incoming event can match —
            # and nothing would report it.
            raise FeishuChannelError(
                FeishuChannelErrorCode.FORMAT_ERROR,
                "on_raw_event takes the event type without a schema prefix, "
                "for example 'im.message.receive_v1'",
            )
        self._handlers.setdefault(event_type, []).append(handler)

        def unsubscribe() -> None:
            handlers = self._handlers.get(event_type)
            if not handlers:
                return
            try:
                handlers.remove(handler)
            except ValueError:
                return
            # The key stays even when empty. Removing the processor would make
            # the dispatcher raise for every later event of this type, which on
            # the socket path prints a full traceback each time.

        return unsubscribe

    @property
    def event_types(self) -> List[str]:
        return list(self._handlers)

    def install(self, dispatcher: Any) -> Optional[str]:
        """Install every subscription onto a **built** dispatcher, in place.

        The builder and the built handler keep the two processor tables under
        the same attribute names, so the same installation logic serves both.
        Used when a subscription arrives while a dispatcher is already running.
        """
        return self.apply(dispatcher)

    def apply(self, builder: Any) -> Optional[str]:
        """Install every subscription onto ``builder``.

        Returns a description of the first failure, or ``None``. Failures are
        reported rather than raised: one broken subscription must not stop the
        channel from starting.
        """
        problem = None
        for event_type in list(self._handlers):
            try:
                self._install(builder, event_type)
            except Exception as exc:
                detail = "%s: %s" % (event_type, type(exc).__name__)
                logger.warning(
                    "channel: could not install raw subscription for %s (%s)",
                    sanitize_for_log(event_type),
                    type(exc).__name__,
                )
                problem = problem or detail
        return problem

    def _install(self, target: Any, event_type: str) -> None:
        """Install one event type's processor onto a builder or a built handler.

        Both shapes keep the two processor tables under the same attribute
        names, so the maps are written directly. That also side-steps the
        builder's duplicate-key guard, which this needs to do: replaying is
        expected to overwrite, and merging every handler for a type into a
        single processor is the whole point — registering per handler is what
        would trip that guard and take the rebuild down with it.
        """
        dispatch = self._dispatcher_for(event_type)
        key = "p2.%s" % event_type
        callback_map = target._callback_processor_map
        event_map = target._processorMap

        if event_type in _CALLBACK_TYPES:
            _method, response_name = _CALLBACK_TYPES[event_type]
            callback_map[key] = _RawEventProcessor(
                inner=_builtin_under(callback_map.get(key)),
                dispatch=dispatch,
                fallback_response=lambda name=response_name: _empty_response(name),
            )
            return
        if key in callback_map:
            callback_map[key] = _RawEventProcessor(
                inner=_builtin_under(callback_map.get(key)), dispatch=dispatch
            )
            return
        event_map[key] = _RawEventProcessor(
            inner=_builtin_under(event_map.get(key)), dispatch=dispatch
        )

    def _dispatcher_for(self, event_type: str) -> Callable[[Any], None]:
        def dispatch(data: Any) -> None:
            handlers = list(self._handlers.get(event_type) or ())
            if not handlers:
                return
            payload = _coerce.obj_to_dict(data) or {}
            self._schedule(self._run(handlers, payload))

        return dispatch

    async def _run(self, handlers: List[Callable], payload: Dict[str, Any]) -> None:
        for handler in handlers:
            try:
                result = handler(payload)
                if inspect.isawaitable(result):
                    await result
            except Exception as exc:
                # A raw handler is application code on an escape hatch; its
                # failure must not touch the built-in path.
                outcome = self._report(exc)
                if inspect.isawaitable(outcome):
                    await outcome


__all__ = ["RawEventRegistry"]
