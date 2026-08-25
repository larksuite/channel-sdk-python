"""What ships, what is exported, and what the source is allowed to use.

The package deliberately keeps a trimmed copy of the API layer with a
file-by-file closure test, so adding a service is a packaging decision that
has to be made explicitly rather than by accident.
"""

import ast
import dataclasses
from pathlib import Path

import pytest

import lark_channel
import lark_channel.api as api_root
from lark_channel import channel as channel_pkg
from lark_channel.channel.auth import uat_runner
from lark_channel.channel.config import ChannelConfig, MeetingChannelConfig

ROOT = Path(lark_channel.__file__).resolve().parents[1]
NEW_SOURCE_DIRS = ("lark_channel/api/vc", "lark_channel/channel/meeting")
#: New files that live outside those directories and would otherwise escape the
#: syntax guard below.
NEW_SOURCE_FILES = ("lark_channel/channel/raw_events.py",)

MEETING_PUBLIC_NAMES = [
    "ActivityTypeStats",
    "DocumentContextEvent",
    "LivenessHealth",
    "MeetingActor",
    "MeetingChannelConfig",
    "MeetingChatEvent",
    "MeetingEndEvent",
    "MeetingEventHealth",
    "MeetingInvitedEvent",
    "MeetingOptions",
    "MembershipHealth",
    "MeetingSession",
    "ParticipantEvent",
    "ShareEvent",
    "TranscriptEvent",
]


def _allowlisted_api_files():
    path = ROOT / "tests/runtime/api_file_allowlist.txt"
    return set(
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    )


def _new_source_files():
    files = []
    for directory in NEW_SOURCE_DIRS:
        base = ROOT / directory
        if not base.exists():
            continue
        for path in base.rglob("*.py"):
            if "/tests/" in path.as_posix():
                continue
            files.append(path)
    for name in NEW_SOURCE_FILES:
        path = ROOT / name
        if path.exists():
            files.append(path)
    return files


def _declared_api_roots():
    """Read the packaging closure's root set without importing its module —
    the runtime test directory is not an importable package."""
    source = (ROOT / "tests/runtime/test_api_allowlist.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
            if "ALLOWED_API_ROOTS" in targets:
                return set(ast.literal_eval(node.value))
    raise AssertionError("ALLOWED_API_ROOTS not found")


def test_the_video_conference_service_is_now_part_of_the_packaged_api():
    assert "vc" in _declared_api_roots()
    assert (Path(api_root.__file__).resolve().parent / "vc").exists()


def test_the_other_unpackaged_services_stay_unpackaged():
    api_path = Path(api_root.__file__).resolve().parent
    for name in ("calendar", "bitable", "drive_full", "docx", "admin"):
        assert not (api_path / name).exists(), name


def test_the_new_api_files_are_in_the_packaging_closure():
    allowlisted = _allowlisted_api_files()
    vc_files = set(
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "lark_channel/api/vc").rglob("*.py")
    )
    assert vc_files
    assert vc_files <= allowlisted


def test_the_video_conference_package_stays_a_thin_builder_layer():
    """Copying a generated model tree in would multiply the packaged surface
    and add a second source of truth for field shapes."""
    files = set(
        path.relative_to(ROOT / "lark_channel/api/vc").as_posix()
        for path in (ROOT / "lark_channel/api/vc").rglob("*.py")
    )
    assert files == {"__init__.py", "bot.py"}


def test_the_dependency_list_is_untouched():
    source = (ROOT / "setup.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    requires = None
    for node in ast.walk(tree):
        if isinstance(node, ast.keyword) and node.arg == "install_requires":
            requires = [
                element.value
                for element in node.value.elts
                if isinstance(element, ast.Constant)
            ]
    assert requires == [
        "requests>=2.25",
        "requests_toolbelt>=0.9",
        "pycryptodome>=3.9",
        "websockets>=11,<16",
        "httpx>=0.24,<1.0",
    ]


def test_the_non_interactive_ticket_helper_is_not_part_of_the_public_surface():
    """It returns a bare token; exporting it invites callers to route around
    the store's lifecycle management."""
    assert hasattr(uat_runner, "resolve_user_auth_non_interactive")
    assert "resolve_user_auth_non_interactive" not in getattr(uat_runner, "__all__", [])
    for module in (lark_channel, channel_pkg):
        assert "resolve_user_auth_non_interactive" not in getattr(module, "__all__", [])


@pytest.mark.parametrize("name", MEETING_PUBLIC_NAMES)
def test_meeting_types_are_exported_from_both_public_entry_points(name):
    assert name in channel_pkg.__all__, name
    assert hasattr(channel_pkg, name), name
    assert hasattr(lark_channel, name), name


def test_the_meeting_config_field_is_appended_at_the_end():
    """Field order on this dataclass is part of the public contract for
    positional callers."""
    names = [field.name for field in dataclasses.fields(ChannelConfig)]
    assert names[-1] == "meeting"
    assert isinstance(ChannelConfig().meeting, MeetingChannelConfig)


def test_the_new_sources_stay_within_the_oldest_supported_python():
    """The support matrix starts two releases before union syntax in
    annotations, slot-enabled dataclasses, and the newer asyncio helpers."""
    banned_attributes = {"to_thread", "timeout", "TaskGroup"}
    sources = _new_source_files()
    # Without this the check would quietly pass on an empty file list.
    assert sources
    offences = []
    for path in sources:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            annotations = []
            if isinstance(node, ast.AnnAssign):
                annotations.append(node.annotation)
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                annotations.append(node.returns)
                for arg in list(node.args.args) + list(node.args.kwonlyargs):
                    annotations.append(arg.annotation)
            for annotation in annotations:
                if annotation is None:
                    continue
                for inner in ast.walk(annotation):
                    if isinstance(inner, ast.BinOp) and isinstance(inner.op, ast.BitOr):
                        offences.append("%s: union operator in annotation" % path.name)
            if isinstance(node, ast.Attribute) and node.attr in banned_attributes:
                if isinstance(node.value, ast.Name) and node.value.id == "asyncio":
                    offences.append("%s: asyncio.%s" % (path.name, node.attr))
            if isinstance(node, ast.keyword) and node.arg == "slots":
                offences.append("%s: dataclass slots" % path.name)
    assert offences == []


def test_the_follow_entry_point_states_its_trust_boundary_up_front():
    """The SDK cannot tell whether the supplied open_id belongs to the caller,
    and a cached ticket resolves without notifying its owner. Anybody reading
    the signature has to meet that before they meet the parameters."""
    from lark_channel.channel import FeishuChannel

    doc = FeishuChannel.follow_my_meeting.__doc__ or ""
    first_paragraph = doc.strip().split("\n\n")[0]
    assert "user_open_id" in first_paragraph
    assert "prompt_context" in first_paragraph
