from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DOCS = [
    ROOT / "README.md",
    ROOT / "README.zh.md",
    *(ROOT / "docs").rglob("*.md"),
    *(ROOT / "samples").rglob("*.py"),
    ROOT / "lark_channel" / "channel" / "__init__.py",
    ROOT / "lark_channel" / "channel" / "channel.py",
    ROOT / "lark_channel" / "channel" / "events.py",
]


def test_documented_primary_import_path_is_package_root():
    text = "\n".join(path.read_text(encoding="utf-8") for path in DOCS if path.exists())
    assert "from lark_channel import FeishuChannel" in text
    assert "pip install lark-channel-sdk" in text
    assert "docs/migration-from-lark-oapi.md" in text
    assert "docs/security.md" in text
    assert "docs/release-notes/v1.1.0.md" in text
    assert "pip install lark-oapi" not in text
    assert "lark-oapi[" not in text
    assert "from lark_channel.channel import" not in text
    assert "from lark_oapi import" not in text
    assert "from lark_oapi.channel import" not in text


def test_security_and_migration_docs_are_linked_and_actionable():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    migration = (ROOT / "docs" / "migration-from-lark-oapi.md").read_text(
        encoding="utf-8"
    )
    security = (ROOT / "docs" / "security.md").read_text(encoding="utf-8")

    assert "docs/migration-from-lark-oapi.md" in readme
    assert "docs/security.md" in readme
    assert "docs/release-notes/v1.1.0.md" in readme
    assert "from lark_channel import FeishuChannel" in migration
    assert "SecurityConfig(mode=\"audit\")" in migration
    assert "SecurityConfig(mode=\"audit\")" in security
    assert "mode=\"strict\"" in security
    assert "record(self, reason, *, mode, action, details=None)" in security
