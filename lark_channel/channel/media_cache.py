import hashlib
import mimetypes
import os
import tempfile
import time
from pathlib import Path

from .config import MediaCacheConfig
from .types import CachedResource, ResourceDescriptor


class MediaResourceCache:
    def __init__(self, config: MediaCacheConfig) -> None:
        self._config = config
        self._root = (
            Path(config.root_dir)
            if config.root_dir is not None
            else Path.home() / ".cache" / "lark-channel-sdk"
        )
        self._key_index = {}

    async def resolve(self, *, message_id: str, resource: ResourceDescriptor, downloader):
        if not self._config.enabled:
            return self._result(
                "skipped",
                reason="cache_disabled",
                message_id=message_id,
                resource=resource,
            )
        if resource.type not in ("image", "file"):
            return self._result(
                "rejected",
                reason="unsupported_resource_type",
                message_id=message_id,
                resource=resource,
            )

        if not self._can_store():
            return self._result(
                "skipped",
                reason="cache_limit",
                message_id=message_id,
                resource=resource,
            )

        cache_key = (message_id, resource.type, resource.file_key)
        cached = self._key_index.get(cache_key)
        if cached is not None and cached.path is not None:
            if (
                self._path_exists(cached.path)
                and self._is_fresh(cached.path)
                and self._is_valid_cache_file(
                    cached.path,
                    cached.sha256 or "",
                    cached.size if cached.size is not None else -1,
                )
            ):
                if self._touch(cached.path):
                    return cached
                self._key_index.pop(cache_key, None)
            else:
                self._safe_unlink(cached.path)
                self._key_index.pop(cache_key, None)

        body, meta = await downloader(
            message_id=message_id,
            file_key=resource.file_key,
            resource_type=resource.type,
        )
        if body is None:
            return self._result(
                "rejected",
                reason="download_failed",
                message_id=message_id,
                resource=resource,
            )

        size = len(body)
        mime_type = _normalize_mime_type(meta)
        if resource.type == "image" and mime_type == "application/octet-stream":
            mime_type = _sniff_image_mime_type(body) or mime_type
        if size > self._config.max_file_bytes:
            return self._result(
                "rejected",
                reason="max_file_bytes",
                message_id=message_id,
                resource=resource,
                mime_type=mime_type,
                size=size,
            )
        if size > self._config.max_bytes:
            return self._result(
                "rejected",
                reason="max_bytes",
                message_id=message_id,
                resource=resource,
                mime_type=mime_type,
                size=size,
            )
        if resource.type == "image" and size > self._config.image_max_bytes:
            return self._result(
                "rejected",
                reason="image_max_bytes",
                message_id=message_id,
                resource=resource,
                mime_type=mime_type,
                size=size,
            )
        if resource.type == "image" and mime_type not in {
            "image/png",
            "image/jpeg",
            "image/jpg",
            "image/gif",
            "image/webp",
        }:
            return self._result(
                "rejected",
                reason="unsupported_mime_type",
                message_id=message_id,
                resource=resource,
                mime_type=mime_type,
                size=size,
            )

        digest = hashlib.sha256(body).hexdigest()
        suffix = _suffix_for(mime_type)
        try:
            self._root.mkdir(parents=True, exist_ok=True)
        except OSError:
            return self._result(
                "rejected",
                reason="write_failed",
                message_id=message_id,
                resource=resource,
                mime_type=mime_type,
                size=size,
            )
        path = self._root / f"{digest}{suffix}"
        if self._path_exists(path) and not self._is_valid_cache_file(path, digest, size):
            self._safe_unlink(path)
        if not self._is_valid_cache_file(path, digest, size) and not self._write_atomic(path, body):
            return self._result(
                "rejected",
                reason="write_failed",
                message_id=message_id,
                resource=resource,
                mime_type=mime_type,
                size=size,
            )
        if not self._touch(path):
            return self._result(
                "rejected",
                reason="write_failed",
                message_id=message_id,
                resource=resource,
                mime_type=mime_type,
                size=size,
            )
        cached = self._result(
            "cached",
            message_id=message_id,
            resource=resource,
            path=path,
            sha256=digest,
            mime_type=mime_type,
            size=size,
        )
        self._key_index[cache_key] = cached
        self.gc(protect=path)
        if not self._path_exists(path):
            self._key_index.pop(cache_key, None)
            return self._result(
                "rejected",
                reason="cache_evicted",
                message_id=message_id,
                resource=resource,
                mime_type=mime_type,
                size=size,
            )
        return cached

    def gc(self, *, protect=None) -> None:
        if not self._path_exists(self._root):
            return
        now = time.time()
        ttl_seconds = self._config.ttl_seconds
        entries = self._entries()
        for path in entries:
            if path == protect:
                continue
            try:
                expired = ttl_seconds <= 0 or now - path.stat().st_mtime > ttl_seconds
            except OSError:
                expired = True
            if expired:
                self._safe_unlink(path)
        entries = self._entries()
        max_entries = max(self._config.max_entries, 0)
        while len(entries) > max_entries:
            victim = self._oldest_unprotected(entries, protect)
            if victim is None:
                break
            self._safe_unlink(victim)
            entries = [path for path in entries if path != victim]
        max_bytes = max(self._config.max_bytes, 0)
        while entries and self._total_size(entries) > max_bytes:
            victim = self._oldest_unprotected(entries, protect)
            if victim is None:
                break
            self._safe_unlink(victim)
            entries = [path for path in entries if path != victim]
        self._key_index = {
            key: value
            for key, value in self._key_index.items()
            if value.path is not None and self._path_exists(value.path)
        }

    def _entries(self):
        try:
            candidates = list(self._root.iterdir())
        except OSError:
            return []
        entries = []
        for path in candidates:
            try:
                if path.is_file() and not path.name.startswith("."):
                    path.stat()
                    entries.append(path)
            except OSError:
                continue
        return sorted(entries, key=lambda path: self._mtime(path))

    def _can_store(self) -> bool:
        return (
            self._config.ttl_seconds > 0
            and self._config.max_entries > 0
            and self._config.max_bytes > 0
        )

    def _is_fresh(self, path) -> bool:
        try:
            return time.time() - path.stat().st_mtime <= self._config.ttl_seconds
        except OSError:
            return False

    def _write_atomic(self, path, body: bytes) -> bool:
        tmp_path = None
        try:
            fd, tmp_name = tempfile.mkstemp(
                prefix=f".{path.stem}.",
                suffix=".tmp",
                dir=str(self._root),
            )
            tmp_path = Path(tmp_name)
            with os.fdopen(fd, "wb") as tmp:
                tmp.write(body)
            os.replace(tmp_path, path)
            return True
        except OSError:
            if tmp_path is not None:
                self._safe_unlink(tmp_path)
            return False

    def _is_valid_cache_file(self, path, digest: str, size: int) -> bool:
        try:
            if path.is_symlink() or not path.is_file():
                return False
            if path.stat().st_size != size:
                return False
        except OSError:
            return False
        return self._sha256_file(path) == digest

    def _touch(self, path) -> bool:
        try:
            os.utime(path, None)
            return True
        except OSError:
            return False

    @staticmethod
    def _path_exists(path) -> bool:
        try:
            return path.exists()
        except OSError:
            return False

    @staticmethod
    def _safe_unlink(path) -> bool:
        try:
            path.unlink(missing_ok=True)
            return True
        except OSError:
            return False

    @staticmethod
    def _sha256_file(path) -> str:
        digest = hashlib.sha256()
        try:
            with path.open("rb") as fp:
                for chunk in iter(lambda: fp.read(1024 * 1024), b""):
                    digest.update(chunk)
        except OSError:
            return ""
        return digest.hexdigest()

    @staticmethod
    def _mtime(path) -> float:
        try:
            return path.stat().st_mtime
        except OSError:
            return 0.0

    @staticmethod
    def _total_size(paths) -> int:
        total = 0
        for path in paths:
            try:
                total += path.stat().st_size
            except OSError:
                continue
        return total

    @staticmethod
    def _oldest_unprotected(paths, protect):
        for path in paths:
            if path != protect:
                return path
        return None

    @staticmethod
    def _result(
        decision: str,
        *,
        message_id: str,
        resource: ResourceDescriptor,
        reason=None,
        path=None,
        sha256=None,
        mime_type=None,
        size=None,
    ) -> CachedResource:
        return CachedResource(
            decision=decision,
            reason=reason,
            path=path,
            sha256=sha256,
            mime_type=mime_type,
            size=size,
            source_message_id=message_id,
            source_file_key=resource.file_key,
            resource_type=resource.type,
        )


def _normalize_mime_type(meta: str) -> str:
    if not meta:
        return "application/octet-stream"
    if "/" in meta:
        return meta.split(";", 1)[0].strip().lower()
    guessed, _ = mimetypes.guess_type(meta)
    return guessed or "application/octet-stream"


def _suffix_for(mime_type: str) -> str:
    if mime_type == "image/png":
        return ".png"
    if mime_type in ("image/jpeg", "image/jpg"):
        return ".jpg"
    if mime_type == "image/gif":
        return ".gif"
    if mime_type == "image/webp":
        return ".webp"
    return mimetypes.guess_extension(mime_type) or ".bin"


def _sniff_image_mime_type(body: bytes) -> str:
    if body.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if body.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if body.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if len(body) >= 12 and body[:4] == b"RIFF" and body[8:12] == b"WEBP":
        return "image/webp"
    return ""
