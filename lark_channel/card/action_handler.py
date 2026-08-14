import hashlib
import hmac
import json
import logging
from typing import Optional, Callable, Any, TYPE_CHECKING

from lark_channel.core.const import *
from lark_channel.core.enum import LogLevel
from lark_channel.core.exception import InvalidArgsException, AccessDeniedException, CardException, \
    NoAuthorizationException
from lark_channel.core.http import HttpHandler
from lark_channel.core.json import JSON
from lark_channel.core.log import logger, redact_for_log
from lark_channel.core.model import RawRequest, RawResponse
from lark_channel.core.utils import Strings, AESCipher
from lark_channel.event.security import (
    REASON_CARD_SIGNATURE_INVALID,
    REASON_CARD_SIGNATURE_MISSING,
    build_error_response_content,
    should_record_security_audit,
)
from .model import Card

if TYPE_CHECKING:
    from lark_channel.channel.config import SecurityConfig


class CardActionHandler(HttpHandler):

    def __init__(self, security: Optional["SecurityConfig"] = None) -> None:
        self._encrypt_key: Optional[str] = None
        self._verification_token: Optional[str] = None
        self._processor: Optional[Callable[[Card], Any]] = None
        self._security = security or _default_security_config()

    def do(self, req: RawRequest) -> RawResponse:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"card access, uri: {req.uri}, "
                         f"headers: {JSON.marshal(redact_for_log(req.headers))}, "
                         f"body: {JSON.marshal(redact_for_log(req.body)) if req.body is not None else None}")

        resp = RawResponse()
        resp.status_code = 200
        resp.set_content_type(f"{APPLICATION_JSON}; charset=utf-8")

        try:
            if req.body is None:
                raise InvalidArgsException("request body is null")

            signature_preverified = False
            if self._is_encrypted_payload(req.body):
                signature_preverified = self._preverify_encrypted_request(req)

            # Decrypt the message
            plaintext = self._decrypt(req.body)

            # Deserialize the response
            card = JSON.unmarshal(plaintext, Card)
            card.raw = req

            if URL_VERIFICATION == card.type:
                # URL verification: constant-time token compare.
                if self._verification_token is None or card.token is None or not hmac.compare_digest(
                    self._verification_token, card.token
                ):
                    raise AccessDeniedException("invalid verification_token")

                # Echo the challenge (JSON-escaped).
                resp.content = JSON.marshal({"challenge": card.challenge}).encode(UTF_8)
                return resp
            else:
                # Otherwise verify the signature
                if not signature_preverified:
                    self._verify_sign(req)

            if self._processor is None:
                raise CardException("processor not found")

            # Handle the business logic
            result: Any = self._processor(card)

            # Return the result
            if result is None:
                resp.content = "{\"msg\":\"success\"}".encode(UTF_8)
            elif isinstance(result, bytes):
                resp.content = result
            elif isinstance(result, str):
                resp.content = bytes(result, UTF_8)
            elif isinstance(result, RawResponse):
                resp = result
            else:
                resp.content = JSON.marshal(result).encode(UTF_8)

            return resp

        except Exception as e:
            if self._security.enforce_strict_error_response:
                logger.error(
                    "handle card failed, uri: %s, request_id: %s, err: %s",
                    req.uri,
                    req.headers.get(X_REQUEST_ID),
                    redact_for_log(str(e)),
                )
            else:
                logger.exception(
                    f"handle card failed, uri: {req.uri}, request_id: {req.headers.get(X_REQUEST_ID)}, err: {e}")
            resp.status_code = 500
            resp.content = build_error_response_content(e, security=self._security)

            return resp

    def _decrypt(self, content: bytes) -> str:
        plaintext: str
        encrypt = json.loads(content).get("encrypt")
        if Strings.is_not_empty(encrypt):
            if Strings.is_empty(self._encrypt_key):
                raise NoAuthorizationException("encrypt_key not found")
            plaintext = AESCipher(self._encrypt_key).decrypt_str(encrypt)
        else:
            plaintext = str(content, UTF_8)

        return plaintext

    def _is_encrypted_payload(self, content: bytes) -> bool:
        try:
            encrypt = json.loads(content).get("encrypt")
        except (TypeError, ValueError, AttributeError):
            return False
        return Strings.is_not_empty(encrypt)

    def _preverify_encrypted_request(self, request: RawRequest) -> bool:
        if Strings.is_empty(self._verification_token):
            if (
                self._security.is_strict
                and not self._security.allow_unsigned_encrypted_webhook
            ):
                self._record_security_audit(
                    REASON_CARD_SIGNATURE_MISSING,
                    action="block",
                    request=request,
                )
                raise AccessDeniedException("signature verification failed")
            if self._security.is_strict:
                self._record_security_audit(
                    REASON_CARD_SIGNATURE_MISSING,
                    action="allow",
                    request=request,
                )
            return False
        if not self._has_signature_headers(request):
            if self._is_url_verification_handshake(request):
                # The Feishu console's "save request URL" challenge is not
                # signed (platform behavior); it carries verification_token
                # as its own proof, checked below in do() like any other
                # card callback, so it does not need to pass signature
                # pre-verify.
                return True
            action = "legacy_flow"
            if self._security.is_strict:
                action = (
                    "allow"
                    if self._security.allow_unsigned_encrypted_webhook
                    else "block"
                )
            self._record_security_audit(
                REASON_CARD_SIGNATURE_MISSING,
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
                REASON_CARD_SIGNATURE_INVALID,
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

    def _is_url_verification_handshake(self, request: RawRequest) -> bool:
        try:
            card = JSON.unmarshal(self._decrypt(request.body), Card)
        except Exception:
            return False
        return URL_VERIFICATION == card.type

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
        if self._verification_token is None or self._verification_token == "":
            return
        timestamp = request.headers.get(LARK_REQUEST_TIMESTAMP)
        nonce = request.headers.get(LARK_REQUEST_NONCE)
        signature = request.headers.get(LARK_REQUEST_SIGNATURE)
        bs = (timestamp + nonce + self._verification_token).encode(UTF_8) + request.body
        h = hashlib.sha1(bs)
        if signature != h.hexdigest():
            raise AccessDeniedException("signature verification failed")

    @staticmethod
    def builder(
        encrypt_key: str,
        verification_token: str,
        level: LogLevel = None,
        *,
        security: Optional["SecurityConfig"] = None,
    ) -> "CardActionHandlerBuilder":
        if level is not None:
            logger.setLevel(int(level.value))
        return CardActionHandlerBuilder(
            encrypt_key,
            verification_token,
            security=security,
        )


class CardActionHandlerBuilder(object):
    def __init__(
        self,
        encrypt_key: str,
        verification_token: str,
        *,
        security: Optional["SecurityConfig"] = None,
    ) -> None:
        self._encrypt_key = encrypt_key
        self._verification_token = verification_token
        self._processor: Optional[Callable[[Card], Any]] = None
        self._security = security or _default_security_config()

    def register(self, f: Callable[[Card], Any]) -> "CardActionHandlerBuilder":
        self._processor = f
        return self

    def build(self) -> CardActionHandler:
        card_action_handler = CardActionHandler(security=self._security)
        card_action_handler._encrypt_key = self._encrypt_key
        card_action_handler._verification_token = self._verification_token
        card_action_handler._processor = self._processor
        return card_action_handler


def _default_security_config():
    from lark_channel.channel.config import SecurityConfig

    return SecurityConfig()
