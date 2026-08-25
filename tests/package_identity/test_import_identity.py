import importlib
import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_new_top_level_package_imports():
    assert importlib.util.find_spec("lark_channel") is not None
    mod = importlib.import_module("lark_channel")
    assert hasattr(mod, "FeishuChannel")


def test_target_source_tree_does_not_keep_lark_oapi_package():
    assert not (ROOT / "lark_oapi").exists()


def test_core_public_entrypoint_imports_from_new_path():
    from lark_channel import FeishuChannel

    assert FeishuChannel.__name__ == "FeishuChannel"


def test_transport_keepalive_config_imports_from_package_root():
    from lark_channel import KeepaliveConfig
    from lark_channel.channel import KeepaliveConfig as ChannelKeepaliveConfig

    assert KeepaliveConfig is ChannelKeepaliveConfig


def test_release_version_is_1_3_0():
    from lark_channel.core.const import VERSION

    assert VERSION == "1.3.0"
