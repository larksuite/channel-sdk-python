"""Converter: PostContent → Markdown (headings / bold / italic / code / links)."""

import re
from typing import Any, Dict, List, NamedTuple, Tuple

from ...types import PostContent, ResourceDescriptor
from ._utils import attr

_AT_MENTION_RE = re.compile(r'<at(\s+)user_id(\s*)=(\s*)"(.*?)">(.*?)</at>')
_IMAGE_KEY_RE = re.compile(r"!\[(.*?)\]\(([^)]+)\)")


class _Attachment(NamedTuple):
    """A validated attachment-zone entry.

    Every field is guaranteed by :func:`_attachment_files`: ``key`` is a
    non-empty string, ``name`` is a string (empty when absent or non-string),
    and ``is_folder`` is a real bool — so callers can interpolate without
    re-checking types.
    """

    key: str
    name: str
    is_folder: bool


def _attachment_files(post: Dict[str, Any]) -> List[_Attachment]:
    """Return the usable entries of a post's top-level attachment zone.

    The attachment zone is a *sibling* of the locale documents rather than part
    of one: ``files: [{file_key, file_name, is_folder}]``.

    Wire values are untrusted, so every field is narrowed here rather than at
    the point of use: a non-string ``file_name`` reaching :func:`attr` would
    raise ``AttributeError`` out of the whole normalize pipeline, and a
    stringly ``is_folder`` (``"false"``) would hide a real, downloadable file
    behind a ``<folder/>`` tag. Entries without a usable key are dropped —
    unlike the standalone converters, there is no single attachment here for a
    ``[file]`` / ``[folder]`` placeholder to stand in for.

    Callers render each entry via :func:`_render_attachment`. The ``name``
    attribute is omitted when empty, matching ``folder.convert`` and node's
    ``post.ts`` (``file.convert`` differs: it always emits ``name=""``).
    """
    if not isinstance(post, dict):
        return []
    files = post.get("files")
    if not isinstance(files, list):
        return []
    attachments: List[_Attachment] = []
    for f in files:
        if not isinstance(f, dict):
            continue
        key = f.get("file_key")
        if not isinstance(key, str) or not key:
            continue
        name = f.get("file_name")
        attachments.append(
            _Attachment(
                key=key,
                name=name if isinstance(name, str) else "",
                is_folder=f.get("is_folder") is True,
            )
        )
    return attachments


def _render_attachment(att: _Attachment) -> str:
    """Render one attachment-zone entry as a ``<file/>`` / ``<folder/>`` tag."""
    tag = "folder" if att.is_folder else "file"
    name_attr = f' name="{attr(att.name)}"' if att.name else ""
    return f'<{tag} key="{attr(att.key)}"{name_attr}/>'


def convert(content: PostContent) -> Tuple[str, List[ResourceDescriptor]]:
    md, md_resources = _post_to_markdown(content.post) if content.post else (content.text or "", [])
    resources = _post_resources(content.post) if content.post else []
    resources.extend(md_resources)
    return md, resources


def convert_body(content: PostContent, drop_open_id: str) -> str:
    """Flatten a post to Markdown with the given open_id's ``<at>`` mention
    removed (both structured ``tag:at`` nodes and inline ``<at user_id=…>``),
    preserving the title, formatting, links and everyone else's mentions.
    Used to build :attr:`InboundMessage.body_text` for post content."""
    if content.post:
        return _post_to_markdown(content.post, drop_open_id=drop_open_id)[0]
    return content.text or ""


def _iter_documents(post: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(post, dict) or not post:
        return []
    if "content" in post:
        return [post]
    return [doc for doc in post.values() if isinstance(doc, dict)]


def _post_to_markdown(
    post: Dict[str, Any], drop_open_id: str = ""
) -> Tuple[str, List[ResourceDescriptor]]:
    docs = _iter_documents(post)
    # The attachment zone is a sibling of the locale documents, not part of
    # one, so it must be read before the guard below — otherwise a post with
    # attachments but no usable locale document would silently drop them from
    # the text while still surfacing them as resources.
    attachments = _attachment_files(post)
    if not docs and not attachments:
        return "", []
    locale = docs[0] if docs else {}

    # Choose source paragraphs: prefer content_v2, fallback to content.
    content_v2 = locale.get("content_v2")
    if isinstance(content_v2, list) and len(content_v2) > 0:
        source_paragraphs = content_v2
    else:
        source_paragraphs = locale.get("content") or []

    lines: List[str] = []
    resources: List[ResourceDescriptor] = []
    title = locale.get("title")
    if title:
        lines.append(f"# {title}")
    for para in source_paragraphs:
        chunks: List[str] = []
        for el in para or []:
            if not isinstance(el, dict):
                continue
            tag = el.get("tag")
            if tag == "text":
                t = el.get("text") or ""
                styles = el.get("style") or []
                if "bold" in styles:
                    t = f"**{t}**"
                if "italic" in styles:
                    t = f"*{t}*"
                if "code" in styles:
                    t = f"`{t}`"
                if "strikethrough" in styles:
                    t = f"~~{t}~~"
                chunks.append(t)
            elif tag == "a":
                chunks.append(f"[{el.get('text') or ''}]({el.get('href') or ''})")
            elif tag == "at":
                # Drop the current bot's own mention when building body_text.
                if drop_open_id and el.get("user_id") == drop_open_id:
                    continue
                nm = el.get("user_name") or el.get("user_id") or ""
                chunks.append(f"@{nm}")
            elif tag == "emotion":
                chunks.append(f":{el.get('emoji_type') or ''}:")
            elif tag == "img":
                chunks.append(f"![image]({el.get('image_key') or ''})")
            elif tag == "media":
                chunks.append(f"[media:{el.get('file_key') or ''}]")
            elif tag == "code_block":
                lang = (el.get("language") or "").lower()
                text = el.get("text") or ""
                chunks.append(f"```{lang}\n{text}\n```")
            elif tag == "hr":
                chunks.append("---")
            elif tag == "md":
                text, res = _process_md_text(el.get("text") or "", drop_open_id=drop_open_id)
                chunks.append(text)
                resources.extend(res)
        line = "".join(chunks)
        if line:
            lines.append(line)
    # Attachment zone renders after the body; resource extraction for it lives
    # in _post_resources (files only — folders are tag-only, mirroring
    # folder.convert's resources=[]).
    lines.extend(_render_attachment(att) for att in attachments)
    return "\n\n".join(lines).strip(), resources


def _post_resources(post: Dict[str, Any]) -> List[ResourceDescriptor]:
    resources: List[ResourceDescriptor] = []
    seen = set()

    def add(kind: str, key: Any, *, file_name: Any = None) -> None:
        if not isinstance(key, str) or not key:
            return
        dedup_key = (kind, key)
        if dedup_key in seen:
            return
        seen.add(dedup_key)
        resources.append(
            ResourceDescriptor(
                type=kind,  # type: ignore[arg-type]
                file_key=key,
                file_name=file_name if isinstance(file_name, str) and file_name else None,
            )
        )

    for doc in _iter_documents(post):
        for para in doc.get("content") or []:
            for el in para or []:
                if not isinstance(el, dict):
                    continue
                tag = el.get("tag")
                if tag == "img":
                    add("image", el.get("image_key"))
                elif tag == "media":
                    add("video", el.get("file_key"))
                elif tag == "audio":
                    add("audio", el.get("file_key"))
                elif tag == "file":
                    add("file", el.get("file_key"), file_name=el.get("file_name"))
    # Attachment zone: files are downloadable resources; folders are rendered
    # as tags only (mirrors the standalone folder converter, resources=[]).
    for att in _attachment_files(post):
        if att.is_folder:
            continue
        add("file", att.key, file_name=att.name)
    return resources


def _process_md_text(
    text: str, drop_open_id: str = ""
) -> Tuple[str, List[ResourceDescriptor]]:
    """Post-process raw markdown text from an "md" element.

    Splits by fenced code block delimiters (```) and only applies
    transformations (at-mention replacement, image key extraction)
    to text outside of properly paired code blocks. Unclosed fences
    are treated as outside-code-block text.
    """
    resources: List[ResourceDescriptor] = []
    parts = text.split("```")
    total = len(parts)
    for i, part in enumerate(parts):
        # Odd-index segments are inside code blocks, UNLESS it's the last
        # segment of an even-length split (unclosed fence).
        is_inside = (i % 2 == 1)
        if is_inside and total % 2 == 0 and i == total - 1:
            is_inside = False
        if not is_inside:
            # Outside code block: apply at-mention replacement.
            def _replace_at(m: re.Match) -> str:
                user_id = m.group(4)
                name = m.group(5)
                if drop_open_id and user_id == drop_open_id:
                    return ""  # remove the current bot's own inline mention
                if user_id in ("all", "all_members"):
                    return "@all"
                return f"@{name}" if name else f"@{user_id}"

            parts[i] = _AT_MENTION_RE.sub(_replace_at, part)

            # Extract image keys from ![...](key) patterns.
            for _alt, img_key in _IMAGE_KEY_RE.findall(parts[i]):
                if img_key:
                    resources.append(ResourceDescriptor(
                        type="image",  # type: ignore[arg-type]
                        file_key=img_key,
                    ))
        # Inside code block: preserve as-is.
    return "```".join(parts), resources
