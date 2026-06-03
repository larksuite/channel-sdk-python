"""Converter: MergeForwardContent → ``<forwarded_messages>`` wrapper.

Aligned with node-sdk's ``converters/merge-forward.ts``:

- Each item is rendered as ``[timestamp] sender:\\n<4-space-indented body>``.
- The whole inner body (including any nested ``<forwarded_messages>`` block)
  is indented 4 spaces relative to its header. Nested forwards thus step in
  by another 4 spaces at each depth, without the *header* itself being
  indented at depth 0.
- Empty or loading payloads emit ``<forwarded_messages/>`` (self-closing).
- Truncation footer: ``\\n... (truncated)`` (no indent).

Indentation follows node's ``indentLines(content, '    ')`` helper — i.e.
indent the rendered inner text, NOT the header.
"""

from typing import Dict, List, Set, Tuple

from ...types import MergeForwardContent, ResourceDescriptor
from ._utils import rfc3339_beijing


def convert(content: MergeForwardContent) -> Tuple[str, List[ResourceDescriptor]]:
    return _flatten(content), _collect_resources(content)


def _flatten(content: MergeForwardContent) -> str:
    return _flatten_iterative(content)


def _flatten_single(
    content: MergeForwardContent,
    rendered_children: Dict[int, str],
) -> str:
    if content.loading or not content.items:
        return "<forwarded_messages/>"

    parts: List[str] = []
    for item in content.items:
        # Per-item error isolation — node's ``try { renderItem } catch { skip }``
        # (see node's merge-forward.ts ``formatSubTree`` loop).
        try:
            sender = item.sender_name or item.sender_open_id or "unknown"
            ts = rfc3339_beijing(item.create_time) or "unknown"
            inner_text = ""
            if item.content is not None:
                if isinstance(item.content, MergeForwardContent):
                    inner_text = rendered_children.get(
                        id(item.content),
                        "<forwarded_messages/>",
                    )
                else:
                    # Import late to avoid cycles — flatten dispatches via the registry.
                    from ..flatten import flatten

                    txt, _ = flatten(item.content)
                    inner_text = txt
            indented = _indent(inner_text, "    ")
            parts.append(
                f"[{ts}] {sender}:\n{indented}" if indented else f"[{ts}] {sender}:"
            )
        except Exception:  # pragma: no cover - defensive
            continue

    if not parts:
        return "<forwarded_messages/>"

    body = "\n".join(parts)
    footer = ""
    if content.truncated:
        footer += "\n... (truncated)"
    if content.error:
        footer += f"\n[error: {content.error}]"
    return f"<forwarded_messages>\n{body}{footer}\n</forwarded_messages>"


def _flatten_iterative(content: MergeForwardContent) -> str:
    rendered: Dict[int, str] = {}
    visiting: Set[int] = set()
    stack: List[Tuple[MergeForwardContent, bool]] = [(content, False)]
    while stack:
        node, expanded = stack.pop()
        node_id = id(node)
        if expanded:
            rendered[node_id] = _flatten_single(node, rendered)
            visiting.discard(node_id)
            continue
        if node_id in rendered:
            continue
        if node_id in visiting:
            rendered[node_id] = "<forwarded_messages/>"
            continue
        visiting.add(node_id)
        stack.append((node, True))
        for item in reversed(node.items or []):
            try:
                child = item.content
                if isinstance(child, MergeForwardContent) and id(child) not in rendered:
                    stack.append((child, False))
            except Exception:  # pragma: no cover - defensive
                continue
    return rendered.get(id(content), "<forwarded_messages/>")


def _indent(text: str, prefix: str) -> str:
    if not text:
        return ""
    return "\n".join(f"{prefix}{line}" for line in text.splitlines())


def _collect_resources(content: MergeForwardContent) -> List[ResourceDescriptor]:
    out: List[ResourceDescriptor] = []
    seen_forwards: Set[int] = set()
    stack = [iter(content.items or [])]
    seen_forwards.add(id(content))
    while stack:
        try:
            item = next(stack[-1])
        except StopIteration:
            stack.pop()
            continue

        # Per-item error isolation — keeps resource collection resilient to
        # one malformed child (mirrors the converter's skip-bad-item policy).
        try:
            child = item.content
            if child is None:
                continue
            if isinstance(child, MergeForwardContent):
                child_id = id(child)
                if child_id in seen_forwards:
                    continue
                seen_forwards.add(child_id)
                stack.append(iter(child.items or []))
            else:
                from ..flatten import flatten

                _, res = flatten(child)
                out.extend(res)
        except Exception:  # pragma: no cover - defensive
            continue
    return out
