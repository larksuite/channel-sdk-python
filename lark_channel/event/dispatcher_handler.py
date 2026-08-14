import hmac
import json
import logging
import warnings
from typing import Any, Callable, Dict, Optional, TYPE_CHECKING, Type

from lark_channel.api.im.v1.processor import (
    P2ImChatMemberBotAddedV1Processor,
    P2ImChatMemberBotDeletedV1Processor,
    P2ImMessageMessageReadV1Processor,
    P2ImMessageReactionCreatedV1Processor,
    P2ImMessageReactionDeletedV1Processor,
    P2ImMessageReceiveV1Processor,
)
from lark_channel.api.im.v1.model.p2_im_chat_member_bot_added_v1 import (
    P2ImChatMemberBotAddedV1,
)
from lark_channel.api.im.v1.model.p2_im_chat_member_bot_deleted_v1 import (
    P2ImChatMemberBotDeletedV1,
)
from lark_channel.api.im.v1.model.p2_im_message_message_read_v1 import (
    P2ImMessageMessageReadV1,
)
from lark_channel.api.im.v1.model.p2_im_message_reaction_created_v1 import (
    P2ImMessageReactionCreatedV1,
)
from lark_channel.api.im.v1.model.p2_im_message_reaction_deleted_v1 import (
    P2ImMessageReactionDeletedV1,
)
from lark_channel.api.im.v1.model.p2_im_message_receive_v1 import (
    P2ImMessageReceiveV1,
)
from lark_channel.core.const import (
    APPLICATION_JSON,
    LARK_REQUEST_NONCE,
    LARK_REQUEST_SIGNATURE,
    LARK_REQUEST_TIMESTAMP,
    UTF_8,
    URL_VERIFICATION,
    X_REQUEST_ID,
)
from lark_channel.core.enum import LogLevel
from lark_channel.core.exception import (
    AccessDeniedException,
    EventException,
    InvalidArgsException,
    NoAuthorizationException,
)
from lark_channel.core.http import HttpHandler
from lark_channel.core.json import JSON
from lark_channel.core.log import logger, redact_for_log
from lark_channel.core.model import RawRequest, RawResponse
from lark_channel.core.utils import AESCipher, Strings
from .callback.model.p2_card_action_trigger import (
    P2CardActionTrigger,
    P2CardActionTriggerResponse,
)
from .callback.model.p2_url_preview_get import (
    P2URLPreviewGet,
    P2URLPreviewGetResponse,
)
from .callback.processor import (
    P2CardActionTriggerProcessor,
    P2URLPreviewGetProcessor,
)
from .context import EventContext
from .custom import CustomizedEvent, CustomizedEventProcessor
from .processor import ICallBackProcessor, IEventProcessor
from .security import (
    REASON_WEBHOOK_SIGNATURE_INVALID,
    REASON_WEBHOOK_SIGNATURE_MISSING,
    build_error_response_content,
    should_record_security_audit,
)
from lark_channel.core.webhook_signature import (
    ReplayGuard,
    verify_webhook_signature,
)

if TYPE_CHECKING:
    from lark_channel.channel.config import SecurityConfig


class EventDispatcherHandler(HttpHandler):
    def __init__(self, security: Optional["SecurityConfig"] = None) -> None:
        self._processorMap: Dict[str, IEventProcessor] = {}
        self._callback_processor_map: Dict[str, ICallBackProcessor] = {}
        self._encrypt_key: Optional[str] = None
        self._verification_token: Optional[str] = None
        self._security = security or _default_security_config()
        self._replay_guard_instance: Optional[ReplayGuard] = None

    def do(self, req: RawRequest) -> RawResponse:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "event access, uri: %s, headers: %s, body: %s",
                req.uri,
                JSON.marshal(redact_for_log(req.headers)),
                JSON.marshal(redact_for_log(req.body)) if req.body is not None else None,
            )

        resp = RawResponse()
        resp.status_code = 200
        resp.set_content_type(f"{APPLICATION_JSON}; charset=utf-8")

        try:
            if req.body is None:
                raise InvalidArgsException("request body is null")

            signature_preverified = False
            if self._is_encrypted_payload(req.body):
                signature_preverified = self._preverify_encrypted_request(req)

            plaintext = self._decrypt(req.body)
            context = self._parse_context(plaintext)

            if Strings.is_not_empty(self._verification_token):
                if context.token is None or not hmac.compare_digest(
                    self._verification_token, context.token
                ):
                    raise AccessDeniedException("invalid verification_token")

            if URL_VERIFICATION == context.type:
                resp.content = JSON.marshal({"challenge": context.challenge}).encode(
                    UTF_8
                )
                return resp

            if not signature_preverified:
                self._verify_sign(req)
            result = self._dispatch(plaintext, context)
            if result is _NO_CALLBACK_RESULT:
                resp.content = b'{"msg":"success"}'
            else:
                resp.content = JSON.marshal(result).encode(UTF_8)
            return resp

        except Exception as e:
            if self._security.enforce_strict_error_response:
                logger.error(
                    "handle event failed, uri: %s, request_id: %s, err: %s",
                    req.uri,
                    req.headers.get(X_REQUEST_ID),
                    redact_for_log(str(e)),
                )
            else:
                logger.exception(
                    "handle event failed, uri: %s, request_id: %s, err: %s",
                    req.uri,
                    req.headers.get(X_REQUEST_ID),
                    e,
                )
            resp.status_code = 500
            resp.content = build_error_response_content(e, security=self._security)
            return resp

    def _do_without_validation(self, payload: bytes) -> Any:
        plaintext = payload.decode(UTF_8)
        context = self._parse_context(plaintext)
        result = self._dispatch(plaintext, context)
        if result is _NO_CALLBACK_RESULT:
            return None
        return result

    def do_without_validation(self, payload: bytes) -> Any:
        warnings.warn(
            "EventDispatcherHandler.do_without_validation() is deprecated and "
            "kept only for backward compatibility. Use do(req) for HTTP "
            "callbacks; WebSocket dispatch is handled internally by the SDK.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._do_without_validation(payload)

    def _decrypt(self, content: bytes) -> str:
        encrypt = json.loads(content).get("encrypt")
        if Strings.is_not_empty(encrypt):
            if Strings.is_empty(self._encrypt_key):
                raise NoAuthorizationException("encrypt_key not found")
            return AESCipher(self._encrypt_key).decrypt_str(encrypt)
        return str(content, UTF_8)

    def _is_encrypted_payload(self, content: bytes) -> bool:
        try:
            encrypt = json.loads(content).get("encrypt")
        except (TypeError, ValueError, AttributeError):
            return False
        return Strings.is_not_empty(encrypt)

    def _preverify_encrypted_request(self, request: RawRequest) -> bool:
        if Strings.is_empty(self._encrypt_key):
            return False
        if not self._has_signature_headers(request):
            action = "legacy_flow"
            if self._security.is_strict:
                action = (
                    "allow"
                    if self._security.allow_unsigned_encrypted_webhook
                    else "block"
                )
            self._record_security_audit(
                REASON_WEBHOOK_SIGNATURE_MISSING,
                action=action,
                request=request,
            )
            if (
                self._security.is_strict
                and not self._security.allow_unsigned_encrypted_webhook
            ):
                raise AccessDeniedException("signature verification failed")
            return True
        try:
            self._verify_sign(request)
        except Exception:
            self._record_security_audit(
                REASON_WEBHOOK_SIGNATURE_INVALID,
                action="block",
                request=request,
            )
            raise
        return True

    def _has_signature_headers(self, request: RawRequest) -> bool:
        return (
            Strings.is_not_empty(request.headers.get(LARK_REQUEST_TIMESTAMP))
            and Strings.is_not_empty(request.headers.get(LARK_REQUEST_NONCE))
            and Strings.is_not_empty(request.headers.get(LARK_REQUEST_SIGNATURE))
        )

    def _record_security_audit(
        self,
        reason: str,
        *,
        action: str,
        request: RawRequest,
    ) -> None:
        if not should_record_security_audit(self._security):
            return
        self._security.audit_recorder.record(
            reason,
            mode=self._security.mode,
            action=action,
            details={
                "uri": request.uri,
                "request_id": request.headers.get(X_REQUEST_ID),
            },
        )

    def _verify_sign(self, request: RawRequest) -> None:
        verify_webhook_signature(
            request,
            secret=self._encrypt_key,
            algorithm="sha256",
            security=self._security,
            record_audit=lambda reason, action: self._record_security_audit(
                reason, action=action, request=request
            ),
            warn=lambda msg: logger.warning("%s", msg),
            replay_guard=self._replay_guard(),
        )

    def _replay_guard(self) -> Optional[ReplayGuard]:
        ttl = self._security.replay_protection_seconds
        if ttl is None:
            return None
        if self._replay_guard_instance is None:
            self._replay_guard_instance = ReplayGuard(ttl)
        return self._replay_guard_instance

    def _parse_context(self, plaintext: str) -> EventContext:
        context = JSON.unmarshal(plaintext, EventContext)
        if Strings.is_not_empty(context.schema):
            context.schema = "p2"
            context.type = context.header.event_type
            context.token = context.header.token
        elif Strings.is_not_empty(context.uuid):
            context.schema = "p1"
            context.type = context.event.get("type")
        return context

    def _dispatch(self, plaintext: str, context: EventContext) -> Any:
        event_key = f"{context.schema}.{context.type}"
        if event_key in self._callback_processor_map:
            processor = self._callback_processor_map.get(event_key)
            if processor is None:
                raise EventException(f"callback processor not found, type: {context.type}")
            data = JSON.unmarshal(plaintext, processor.type())
            return processor.do(data)

        processor = self._processorMap.get(event_key)
        if processor is None:
            raise EventException(f"processor not found, type: {context.type}")
        data = JSON.unmarshal(plaintext, processor.type())
        processor.do(data)
        return _NO_CALLBACK_RESULT

    @staticmethod
    def builder(
        encrypt_key: str,
        verification_token: str,
        level: LogLevel = None,
        *,
        security: Optional["SecurityConfig"] = None,
    ) -> "EventDispatcherHandlerBuilder":
        if level is not None:
            logger.setLevel(int(level.value))
        return EventDispatcherHandlerBuilder(
            encrypt_key,
            verification_token,
            security=security,
        )


class _NoCallbackResult:
    pass


_NO_CALLBACK_RESULT = _NoCallbackResult()


class EventDispatcherHandlerBuilder(object):
    def __init__(
        self,
        encrypt_key: str,
        verification_token: str,
        *,
        security: Optional["SecurityConfig"] = None,
    ) -> None:
        self._encrypt_key = encrypt_key
        self._verification_token = verification_token
        self._security = security or _default_security_config()
        self._processorMap: Dict[str, IEventProcessor] = {}
        self._callback_processor_map: Dict[str, ICallBackProcessor] = {}

    def register_p1_customized_event(
        self, event_type: str, f: Callable[[CustomizedEvent], None]
    ) -> "EventDispatcherHandlerBuilder":
        return self._register_event(
            f"p1.{event_type}", CustomizedEventProcessor(f)
        )

    def register_p2_customized_event(
        self, event_type: str, f: Callable[[CustomizedEvent], None]
    ) -> "EventDispatcherHandlerBuilder":
        return self._register_event(
            f"p2.{event_type}", CustomizedEventProcessor(f)
        )

    def register_p2_card_action_trigger(
        self,
        f: Callable[[P2CardActionTrigger], P2CardActionTriggerResponse],
    ) -> "EventDispatcherHandlerBuilder":
        return self._register_callback(
            "p2.card.action.trigger", P2CardActionTriggerProcessor(f)
        )

    def register_p2_url_preview_get(
        self,
        f: Callable[[P2URLPreviewGet], P2URLPreviewGetResponse],
    ) -> "EventDispatcherHandlerBuilder":
        return self._register_callback(
            "p2.url.preview.get", P2URLPreviewGetProcessor(f)
        )

    def register_p2_im_message_receive_v1(
        self, f: Callable[[P2ImMessageReceiveV1], None]
    ) -> "EventDispatcherHandlerBuilder":
        return self._register_event(
            "p2.im.message.receive_v1", P2ImMessageReceiveV1Processor(f)
        )

    def register_p2_im_message_reaction_created_v1(
        self, f: Callable[[P2ImMessageReactionCreatedV1], None]
    ) -> "EventDispatcherHandlerBuilder":
        return self._register_event(
            "p2.im.message.reaction.created_v1",
            P2ImMessageReactionCreatedV1Processor(f),
        )

    def register_p2_im_message_reaction_deleted_v1(
        self, f: Callable[[P2ImMessageReactionDeletedV1], None]
    ) -> "EventDispatcherHandlerBuilder":
        return self._register_event(
            "p2.im.message.reaction.deleted_v1",
            P2ImMessageReactionDeletedV1Processor(f),
        )

    def register_p2_im_chat_member_bot_added_v1(
        self, f: Callable[[P2ImChatMemberBotAddedV1], None]
    ) -> "EventDispatcherHandlerBuilder":
        return self._register_event(
            "p2.im.chat.member.bot.added_v1",
            P2ImChatMemberBotAddedV1Processor(f),
        )

    def register_p2_im_chat_member_bot_deleted_v1(
        self, f: Callable[[P2ImChatMemberBotDeletedV1], None]
    ) -> "EventDispatcherHandlerBuilder":
        return self._register_event(
            "p2.im.chat.member.bot.deleted_v1",
            P2ImChatMemberBotDeletedV1Processor(f),
        )

    def register_p2_im_message_message_read_v1(
        self, f: Callable[[P2ImMessageMessageReadV1], None]
    ) -> "EventDispatcherHandlerBuilder":
        return self._register_event(
            "p2.im.message.message_read_v1",
            P2ImMessageMessageReadV1Processor(f),
        )

    def _register_event(
        self, key: str, processor: IEventProcessor
    ) -> "EventDispatcherHandlerBuilder":
        if key in self._processorMap:
            raise EventException(f"processor already registered, type: {key}")
        self._processorMap[key] = processor
        return self

    def _register_callback(
        self, key: str, processor: ICallBackProcessor
    ) -> "EventDispatcherHandlerBuilder":
        if key in self._callback_processor_map:
            raise EventException(f"processor already registered, type: {key}")
        self._callback_processor_map[key] = processor
        return self

    def build(self) -> EventDispatcherHandler:
        handler = EventDispatcherHandler(security=self._security)
        handler._encrypt_key = self._encrypt_key
        handler._verification_token = self._verification_token
        handler._processorMap = self._processorMap
        handler._callback_processor_map = self._callback_processor_map
        return handler


def _default_security_config():
    from lark_channel.channel.config import SecurityConfig

    return SecurityConfig()
