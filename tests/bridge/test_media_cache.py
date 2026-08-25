import asyncio
import io
import os
import time
from dataclasses import fields
from types import SimpleNamespace
from typing import List, get_type_hints

import pytest

from lark_channel import ChannelConfig, FeishuChannel, MediaCacheConfig
from lark_channel.channel import _api_helpers
from lark_channel.channel.types import CachedResource, ResourceDescriptor


def test_media_cache_config_is_appended_for_positional_compatibility():
    names = [field.name for field in fields(ChannelConfig)]

    # media_cache/security keep their positions; the bot-at-bot knobs
    # (resolve_sender_names / resolve_chat_members) were appended after them,
    # and the meeting config after those, so existing positional callers are
    # unaffected — every addition goes on the end.
    assert names[-3:] == [
        "resolve_sender_names",
        "resolve_chat_members",
        "meeting",
    ]
    assert names[names.index("security") - 1] == "media_cache"
    assert names[names.index("chat_mode_cache") + 1] == "policy"


@pytest.mark.asyncio
async def test_cache_api_writes_hash_named_file(tmp_path, monkeypatch):
    ch = FeishuChannel(
        app_id="cli_x",
        app_secret="s",
        config=ChannelConfig(media_cache=MediaCacheConfig(root_dir=tmp_path)),
    )

    async def fake_download(lark_client, *, message_id, file_key, resource_type):
        assert lark_client is ch.client
        assert (message_id, file_key, resource_type) == ("om_1", "img_1", "image")
        return b"abc", "image/png"

    monkeypatch.setattr("lark_channel.channel._api_helpers.download_media_with_meta", fake_download)

    result = await ch.resolve_resource_to_cache(
        message_id="om_1",
        resource=ResourceDescriptor(type="image", file_key="img_1"),
    )

    assert result.decision == "cached"
    assert result.sha256 == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    assert result.path.exists()
    assert result.path.read_bytes() == b"abc"


@pytest.mark.asyncio
async def test_cache_rejects_sticker_without_downloading(tmp_path, monkeypatch):
    ch = FeishuChannel(
        app_id="cli_x",
        app_secret="s",
        config=ChannelConfig(media_cache=MediaCacheConfig(root_dir=tmp_path)),
    )
    called = False

    async def fake_download(*args, **kwargs):
        nonlocal called
        called = True
        return b"abc", "image/png"

    monkeypatch.setattr("lark_channel.channel._api_helpers.download_media_with_meta", fake_download)

    result = await ch.resolve_resource_to_cache(
        message_id="om_1",
        resource=ResourceDescriptor(type="sticker", file_key="st_1"),
    )

    assert result.decision == "rejected"
    assert result.reason == "unsupported_resource_type"
    assert called is False


@pytest.mark.asyncio
async def test_old_download_resource_still_returns_bytes(monkeypatch):
    ch = FeishuChannel(app_id="cli_x", app_secret="s")

    async def fake_download(*args, **kwargs):
        return b"old"

    monkeypatch.setattr("lark_channel.channel._api_helpers.download_media", fake_download)

    assert await ch.download_resource("k", "image", "om_1") == b"old"


@pytest.mark.asyncio
async def test_cache_hit_does_not_download_again(tmp_path, monkeypatch):
    ch = FeishuChannel(
        app_id="cli_x",
        app_secret="s",
        config=ChannelConfig(media_cache=MediaCacheConfig(root_dir=tmp_path)),
    )
    calls = 0

    async def fake_download(lark_client, *, message_id, file_key, resource_type):
        nonlocal calls
        calls += 1
        return b"abc", "image/png"

    monkeypatch.setattr("lark_channel.channel._api_helpers.download_media_with_meta", fake_download)
    resource = ResourceDescriptor(type="image", file_key="img_1")

    first = await ch.resolve_resource_to_cache(message_id="om_1", resource=resource)
    second = await ch.resolve_resource_to_cache(message_id="om_1", resource=resource)

    assert first.path == second.path
    assert calls == 1


@pytest.mark.asyncio
async def test_cache_hit_revalidates_file_content(tmp_path, monkeypatch):
    ch = FeishuChannel(
        app_id="cli_x",
        app_secret="s",
        config=ChannelConfig(media_cache=MediaCacheConfig(root_dir=tmp_path)),
    )
    calls = 0

    async def fake_download(lark_client, *, message_id, file_key, resource_type):
        nonlocal calls
        calls += 1
        return b"abc", "image/png"

    monkeypatch.setattr("lark_channel.channel._api_helpers.download_media_with_meta", fake_download)
    resource = ResourceDescriptor(type="image", file_key="img_1")

    first = await ch.resolve_resource_to_cache(message_id="om_1", resource=resource)
    first.path.write_bytes(b"bad")
    second = await ch.resolve_resource_to_cache(message_id="om_1", resource=resource)

    assert calls == 2
    assert second.decision == "cached"
    assert second.path.read_bytes() == b"abc"


@pytest.mark.asyncio
async def test_zero_byte_cache_hit_is_valid(tmp_path, monkeypatch):
    ch = FeishuChannel(
        app_id="cli_x",
        app_secret="s",
        config=ChannelConfig(media_cache=MediaCacheConfig(root_dir=tmp_path)),
    )
    calls = 0

    async def fake_download(lark_client, *, message_id, file_key, resource_type):
        nonlocal calls
        calls += 1
        return b"", "application/octet-stream"

    monkeypatch.setattr("lark_channel.channel._api_helpers.download_media_with_meta", fake_download)
    resource = ResourceDescriptor(type="file", file_key="file_1")

    first = await ch.resolve_resource_to_cache(message_id="om_1", resource=resource)
    second = await ch.resolve_resource_to_cache(message_id="om_1", resource=resource)

    assert calls == 1
    assert first.path == second.path
    assert second.size == 0
    assert second.path.exists()


@pytest.mark.asyncio
async def test_expired_cache_hit_downloads_fresh_file(tmp_path, monkeypatch):
    ch = FeishuChannel(
        app_id="cli_x",
        app_secret="s",
        config=ChannelConfig(
            media_cache=MediaCacheConfig(root_dir=tmp_path, ttl_seconds=1)
        ),
    )
    bodies = [b"abc", b"def"]
    calls = 0

    async def fake_download(lark_client, *, message_id, file_key, resource_type):
        nonlocal calls
        body = bodies[calls]
        calls += 1
        return body, "image/png"

    monkeypatch.setattr("lark_channel.channel._api_helpers.download_media_with_meta", fake_download)
    resource = ResourceDescriptor(type="image", file_key="img_1")

    first = await ch.resolve_resource_to_cache(message_id="om_1", resource=resource)
    old_time = time.time() - 10
    os.utime(first.path, (old_time, old_time))
    second = await ch.resolve_resource_to_cache(message_id="om_1", resource=resource)

    assert calls == 2
    assert first.path != second.path
    assert not first.path.exists()
    assert second.path.exists()
    assert second.path.read_bytes() == b"def"


@pytest.mark.asyncio
async def test_cache_rejects_oversized_image_and_unknown_mime(tmp_path, monkeypatch):
    ch = FeishuChannel(
        app_id="cli_x",
        app_secret="s",
        config=ChannelConfig(
            media_cache=MediaCacheConfig(root_dir=tmp_path, image_max_bytes=2)
        ),
    )

    async def too_large(lark_client, *, message_id, file_key, resource_type):
        return b"abc", "image/png"

    monkeypatch.setattr("lark_channel.channel._api_helpers.download_media_with_meta", too_large)
    result = await ch.resolve_resource_to_cache(
        message_id="om_1",
        resource=ResourceDescriptor(type="image", file_key="img_1"),
    )
    assert result.decision == "rejected"
    assert result.reason == "image_max_bytes"

    async def bad_mime(lark_client, *, message_id, file_key, resource_type):
        return b"a", "application/octet-stream"

    monkeypatch.setattr("lark_channel.channel._api_helpers.download_media_with_meta", bad_mime)
    result = await ch.resolve_resource_to_cache(
        message_id="om_1",
        resource=ResourceDescriptor(type="image", file_key="img_2"),
    )
    assert result.reason == "unsupported_mime_type"


@pytest.mark.asyncio
async def test_cache_skips_impossible_retention_limits_without_downloading(tmp_path, monkeypatch):
    ch = FeishuChannel(
        app_id="cli_x",
        app_secret="s",
        config=ChannelConfig(
            media_cache=MediaCacheConfig(root_dir=tmp_path, max_entries=0)
        ),
    )
    called = False

    async def fake_download(*args, **kwargs):
        nonlocal called
        called = True
        return b"abc", "image/png"

    monkeypatch.setattr("lark_channel.channel._api_helpers.download_media_with_meta", fake_download)

    result = await ch.resolve_resource_to_cache(
        message_id="om_1",
        resource=ResourceDescriptor(type="image", file_key="img_1"),
    )

    assert result.decision == "skipped"
    assert result.reason == "cache_limit"
    assert result.path is None
    assert called is False


@pytest.mark.asyncio
async def test_cache_skips_negative_retention_limits_without_error(tmp_path, monkeypatch):
    ch = FeishuChannel(
        app_id="cli_x",
        app_secret="s",
        config=ChannelConfig(
            media_cache=MediaCacheConfig(root_dir=tmp_path, max_entries=-1)
        ),
    )
    called = False

    async def fake_download(*args, **kwargs):
        nonlocal called
        called = True
        return b"abc", "image/png"

    monkeypatch.setattr("lark_channel.channel._api_helpers.download_media_with_meta", fake_download)

    result = await ch.resolve_resource_to_cache(
        message_id="om_1",
        resource=ResourceDescriptor(type="image", file_key="img_1"),
    )

    assert result.decision == "skipped"
    assert result.reason == "cache_limit"
    assert result.path is None
    assert called is False


@pytest.mark.asyncio
async def test_cache_rejects_file_larger_than_total_cache_limit(tmp_path, monkeypatch):
    ch = FeishuChannel(
        app_id="cli_x",
        app_secret="s",
        config=ChannelConfig(media_cache=MediaCacheConfig(root_dir=tmp_path, max_bytes=2)),
    )

    async def fake_download(lark_client, *, message_id, file_key, resource_type):
        return b"abc", "application/octet-stream"

    monkeypatch.setattr("lark_channel.channel._api_helpers.download_media_with_meta", fake_download)

    result = await ch.resolve_resource_to_cache(
        message_id="om_1",
        resource=ResourceDescriptor(type="file", file_key="file_1"),
    )

    assert result.decision == "rejected"
    assert result.reason == "max_bytes"
    assert result.path is None
    assert [p for p in tmp_path.iterdir() if p.is_file()] == []


@pytest.mark.asyncio
async def test_cache_write_failure_cleans_temp_file(tmp_path, monkeypatch):
    ch = FeishuChannel(
        app_id="cli_x",
        app_secret="s",
        config=ChannelConfig(media_cache=MediaCacheConfig(root_dir=tmp_path)),
    )

    async def fake_download(lark_client, *, message_id, file_key, resource_type):
        return b"abc", "image/png"

    def failing_replace(src, dst):
        raise OSError("replace failed")

    monkeypatch.setattr("lark_channel.channel._api_helpers.download_media_with_meta", fake_download)
    monkeypatch.setattr("lark_channel.channel.media_cache.os.replace", failing_replace)

    result = await ch.resolve_resource_to_cache(
        message_id="om_1",
        resource=ResourceDescriptor(type="image", file_key="img_1"),
    )

    assert result.decision == "rejected"
    assert result.reason == "write_failed"
    assert result.path is None
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_cache_replaces_existing_digest_file_with_wrong_content(tmp_path, monkeypatch):
    ch = FeishuChannel(
        app_id="cli_x",
        app_secret="s",
        config=ChannelConfig(media_cache=MediaCacheConfig(root_dir=tmp_path)),
    )
    digest = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    poisoned = tmp_path / f"{digest}.png"
    poisoned.write_bytes(b"bad")

    async def fake_download(lark_client, *, message_id, file_key, resource_type):
        return b"abc", "image/png"

    monkeypatch.setattr("lark_channel.channel._api_helpers.download_media_with_meta", fake_download)

    result = await ch.resolve_resource_to_cache(
        message_id="om_1",
        resource=ResourceDescriptor(type="image", file_key="img_1"),
    )

    assert result.decision == "cached"
    assert result.path == poisoned
    assert result.path.read_bytes() == b"abc"


def test_cache_gc_makes_progress_when_unlink_fails(tmp_path, monkeypatch):
    ch = FeishuChannel(
        app_id="cli_x",
        app_secret="s",
        config=ChannelConfig(media_cache=MediaCacheConfig(root_dir=tmp_path, max_entries=1)),
    )
    old = tmp_path / "old.bin"
    new = tmp_path / "new.bin"
    old.write_bytes(b"old")
    new.write_bytes(b"new")
    calls = 0

    def entries():
        nonlocal calls
        calls += 1
        if calls > 3:
            raise AssertionError("gc did not make progress")
        return [old, new]

    monkeypatch.setattr(ch._media_cache, "_entries", entries)
    monkeypatch.setattr(ch._media_cache, "_safe_unlink", lambda path: False)

    ch._media_cache.gc(protect=new)

    assert calls <= 3


@pytest.mark.asyncio
async def test_cache_accepts_image_filename_hint(tmp_path, monkeypatch):
    ch = FeishuChannel(
        app_id="cli_x",
        app_secret="s",
        config=ChannelConfig(media_cache=MediaCacheConfig(root_dir=tmp_path)),
    )

    async def fake_download(lark_client, *, message_id, file_key, resource_type):
        return b"abc", "photo.png"

    monkeypatch.setattr("lark_channel.channel._api_helpers.download_media_with_meta", fake_download)

    result = await ch.resolve_resource_to_cache(
        message_id="om_1",
        resource=ResourceDescriptor(type="image", file_key="img_1"),
    )

    assert result.decision == "cached"
    assert result.mime_type == "image/png"


@pytest.mark.asyncio
async def test_cache_sniffs_image_mime_when_server_meta_is_generic(tmp_path, monkeypatch):
    ch = FeishuChannel(
        app_id="cli_x",
        app_secret="s",
        config=ChannelConfig(media_cache=MediaCacheConfig(root_dir=tmp_path)),
    )
    png = (
        b"\x89PNG\r\n\x1a\n"
        b"\x00\x00\x00\rIHDR"
        b"\x00\x00\x00\x01\x00\x00\x00\x01"
        b"\x08\x02\x00\x00\x00"
    )

    async def fake_download(lark_client, *, message_id, file_key, resource_type):
        return png, "application/octet-stream"

    monkeypatch.setattr("lark_channel.channel._api_helpers.download_media_with_meta", fake_download)

    result = await ch.resolve_resource_to_cache(
        message_id="om_1",
        resource=ResourceDescriptor(type="image", file_key="img_1"),
    )

    assert result.decision == "cached"
    assert result.mime_type == "image/png"
    assert result.path.suffix == ".png"


@pytest.mark.asyncio
async def test_download_media_with_meta_does_not_block_event_loop_when_generated_get_blocks():
    class BlockingMessageResource:
        def get(self, request):
            time.sleep(0.3)
            return SimpleNamespace(
                code=0,
                file=io.BytesIO(b"image-body"),
                file_name="image.png",
            )

    lark_client = SimpleNamespace(
        im=SimpleNamespace(
            v1=SimpleNamespace(
                message_resource=BlockingMessageResource(),
            )
        )
    )

    started = time.monotonic()
    task = asyncio.create_task(
        _api_helpers.download_media_with_meta(
            lark_client,
            message_id="om_1",
            file_key="img_1",
            resource_type="image",
        )
    )

    await asyncio.sleep(0.05)

    assert time.monotonic() - started < 0.2
    body, meta = await asyncio.wait_for(task, timeout=1)
    assert body == b"image-body"
    assert meta == "image.png"


@pytest.mark.asyncio
async def test_cache_gc_enforces_ttl_entries_and_bytes(tmp_path, monkeypatch):
    ch = FeishuChannel(
        app_id="cli_x",
        app_secret="s",
        config=ChannelConfig(
            media_cache=MediaCacheConfig(
                root_dir=tmp_path,
                ttl_seconds=1,
                max_entries=1,
                max_bytes=4,
            )
        ),
    )

    async def fake_download(lark_client, *, message_id, file_key, resource_type):
        return file_key[:3].encode(), "application/octet-stream"

    monkeypatch.setattr("lark_channel.channel._api_helpers.download_media_with_meta", fake_download)

    old = await ch.resolve_resource_to_cache(
        message_id="om_1",
        resource=ResourceDescriptor(type="file", file_key="old"),
    )
    old_time = time.time() - 10
    os.utime(old.path, (old_time, old_time))

    newer = await ch.resolve_resource_to_cache(
        message_id="om_2",
        resource=ResourceDescriptor(type="file", file_key="newer"),
    )

    live_files = [
        p for p in tmp_path.iterdir() if p.is_file() and not p.name.startswith(".")
    ]
    assert newer.decision == "cached"
    assert newer.path.exists()
    assert not old.path.exists()
    assert live_files == [newer.path]


def test_cache_public_method_type_hints_are_resolvable():
    single = get_type_hints(FeishuChannel.resolve_resource_to_cache)
    batch = get_type_hints(FeishuChannel.resolve_resources_to_cache)

    assert single["resource"] is ResourceDescriptor
    assert single["return"] is CachedResource
    assert batch["resources"] == List[ResourceDescriptor]
    assert batch["return"] == List[CachedResource]
