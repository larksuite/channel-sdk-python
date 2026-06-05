"""Converter: InteractiveContent — deep-walk a CardKit v2 card.

Concatenates markdown / plain-text leaves in traversal order, deduping
adjacent duplicates. Unknown widgets are skipped.
"""

from typing import Any, Dict, List, Tuple

from ...types import InteractiveContent, ResourceDescriptor


def convert(content: InteractiveContent) -> Tuple[str, List[ResourceDescriptor]]:
    return _walk(content.card), []


def _walk(card: Dict[str, Any]) -> str:
    if not isinstance(card, dict):
        return "[interactive]"
    seen: List[str] = []

    def visit_iterative(root: Any) -> None:
        stack: List[Any] = [root]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                tag = node.get("tag")
                if tag == "markdown":
                    c = node.get("content")
                    if c:
                        seen.append(str(c))
                    continue
                if tag == "plain_text":
                    c = node.get("content")
                    if c:
                        seen.append(str(c))
                    continue
                children = []
                if tag == "div":
                    text = node.get("text")
                    if isinstance(text, dict):
                        children.append(text)
                children.extend(node.values())
                stack.extend(reversed(children))
            elif isinstance(node, list):
                stack.extend(reversed(node))

    header = card.get("header") or {}
    if isinstance(header, dict):
        visit_iterative(header.get("title"))
        visit_iterative(header.get("subtitle"))
    body = card.get("body") or {}
    visit_iterative(body)

    deduped: List[str] = []
    for s in seen:
        s = s.strip()
        if not s:
            continue
        if deduped and deduped[-1] == s:
            continue
        deduped.append(s)
    return "\n".join(deduped) or "[interactive]"
