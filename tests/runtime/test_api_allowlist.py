import importlib
import ast
import pkgutil
from pathlib import Path

import lark_channel.api as api_root


ROOT = Path(__file__).resolve().parents[2]

ALLOWED_API_ROOTS = {"im", "contact", "cardkit", "drive", "wiki", "vc"}

DIRECT_API_MODULES = {
    "lark_channel.api.cardkit.v1.model.content_card_element_request",
    "lark_channel.api.cardkit.v1.model.content_card_element_request_body",
    "lark_channel.api.cardkit.v1.model.create_card_request",
    "lark_channel.api.cardkit.v1.model.create_card_request_body",
    "lark_channel.api.cardkit.v1.model.settings_card_request",
    "lark_channel.api.cardkit.v1.model.settings_card_request_body",
    "lark_channel.api.contact.v3.model.batch_user_request",
    "lark_channel.api.drive.comment",
    "lark_channel.api.vc.bot",
    "lark_channel.api.im.v1.model.create_file_request",
    "lark_channel.api.im.v1.model.create_file_request_body",
    "lark_channel.api.im.v1.model.create_file_response",
    "lark_channel.api.im.v1.model.create_file_response_body",
    "lark_channel.api.im.v1.model.create_image_request",
    "lark_channel.api.im.v1.model.create_image_request_body",
    "lark_channel.api.im.v1.model.create_image_response",
    "lark_channel.api.im.v1.model.create_image_response_body",
    "lark_channel.api.im.v1.model.create_message_reaction_request",
    "lark_channel.api.im.v1.model.create_message_reaction_request_body",
    "lark_channel.api.im.v1.model.create_message_request",
    "lark_channel.api.im.v1.model.create_message_request_body",
    "lark_channel.api.im.v1.model.delete_message_reaction_request",
    "lark_channel.api.im.v1.model.delete_message_request",
    "lark_channel.api.im.v1.model.emoji",
    "lark_channel.api.im.v1.model.forward_message_request",
    "lark_channel.api.im.v1.model.forward_message_request_body",
    "lark_channel.api.im.v1.model.get_chat_request",
    "lark_channel.api.im.v1.model.get_file_request",
    "lark_channel.api.im.v1.model.get_image_request",
    "lark_channel.api.im.v1.model.get_message_request",
    "lark_channel.api.im.v1.model.get_message_resource_request",
    "lark_channel.api.im.v1.model.list_message_request",
    "lark_channel.api.im.v1.model.p2_im_chat_member_bot_added_v1",
    "lark_channel.api.im.v1.model.p2_im_chat_member_bot_deleted_v1",
    "lark_channel.api.im.v1.model.p2_im_message_message_read_v1",
    "lark_channel.api.im.v1.model.p2_im_message_reaction_created_v1",
    "lark_channel.api.im.v1.model.p2_im_message_reaction_deleted_v1",
    "lark_channel.api.im.v1.model.p2_im_message_receive_v1",
    "lark_channel.api.im.v1.model.patch_message_request",
    "lark_channel.api.im.v1.model.patch_message_request_body",
    "lark_channel.api.im.v1.model.read_users_message_request",
    "lark_channel.api.im.v1.model.read_users_message_request_body",
    "lark_channel.api.im.v1.model.reply_message_request",
    "lark_channel.api.im.v1.model.reply_message_request_body",
    "lark_channel.api.im.v1.model.update_message_request",
    "lark_channel.api.im.v1.model.update_message_request_body",
    "lark_channel.api.im.v1.processor",
    "lark_channel.api.wiki.node",
}


def test_api_root_contains_only_channel_allowlist():
    roots = {m.name for m in pkgutil.iter_modules(api_root.__path__)}
    assert roots == ALLOWED_API_ROOTS


def test_required_direct_api_modules_import():
    for module_name in sorted(DIRECT_API_MODULES):
        importlib.import_module(module_name)


def test_non_channel_api_roots_are_not_packaged():
    api_path = Path(api_root.__file__).resolve().parent
    for name in ("calendar", "bitable", "drive_full", "docx", "admin"):
        assert not (api_path / name).exists(), name


def test_full_drive_and_wiki_services_are_not_exported():
    assert not hasattr(api_root, "DriveService")
    assert not hasattr(api_root, "WikiService")


def test_api_file_closure_matches_allowlist():
    allowlist_path = ROOT / "tests/runtime/api_file_allowlist.txt"
    expected = {
        line.strip()
        for line in allowlist_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }
    actual = {
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "lark_channel/api").rglob("*.py")
    }
    assert actual == expected


def test_channel_api_imports_are_covered_by_file_allowlist():
    expected = {
        line.strip()
        for line in (ROOT / "tests/runtime/api_file_allowlist.txt").read_text(encoding="utf-8").splitlines()
        if line.strip()
    }
    for path in [ROOT / "lark_channel/client.py", *(ROOT / "lark_channel/channel").rglob("*.py")]:
        current_module = ".".join(path.relative_to(ROOT).with_suffix("").parts)
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            modules = []
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                if node.level:
                    base = "." * node.level + (node.module or "")
                    module = importlib.util.resolve_name(base, current_module.rsplit(".", 1)[0])
                else:
                    module = node.module or ""
                modules = [module]
            for module_name in modules:
                if not module_name.startswith("lark_channel.api."):
                    continue
                rel = ROOT.joinpath(*module_name.split(".")).with_suffix(".py").relative_to(ROOT).as_posix()
                assert rel in expected, f"{path.relative_to(ROOT)} imports {module_name}"
