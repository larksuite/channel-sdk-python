"""Tests for flat-string content + resources[] derivation (Node-aligned)."""

from lark_channel.channel.normalize.flatten import flatten
from lark_channel.channel.types import (
    AudioContent,
    FileContent,
    FolderContent,
    GeneralCalendarContent,
    HongbaoContent,
    ImageContent,
    InteractiveContent,
    LocationContent,
    MediaContent,
    MergeForwardContent,
    MergeForwardItem,
    PostContent,
    ShareCalendarEventContent,
    ShareChatContent,
    ShareUserContent,
    StickerContent,
    TextContent,
    UnknownContent,
)


def test_text_flat_passthrough():
    t, r = flatten(TextContent(text="hello"))
    assert t == "hello" and r == []


def test_image_markdown_placeholder_plus_resource():
    t, r = flatten(ImageContent(image_key="img_abc"))
    assert t == "![image](img_abc)"
    assert len(r) == 1
    assert r[0].type == "image" and r[0].file_key == "img_abc"


def test_file_xml_placeholder_resource_has_name():
    t, r = flatten(FileContent(file_key="f_x", file_name="report.pdf"))
    assert "key=\"f_x\"" in t and "report.pdf" in t
    assert r[0].type == "file" and r[0].file_name == "report.pdf"


def test_video_uses_media_content_with_cover():
    t, r = flatten(MediaContent(file_key="v_1", image_key="cov_1", duration_ms=3000))
    assert "<video " in t and 'key="v_1"' in t
    assert r[0].type == "video" and r[0].cover_image_key == "cov_1"


def test_audio_and_sticker_emit_resource():
    _, ra = flatten(AudioContent(file_key="a_1", duration_ms=1500))
    assert ra[0].type == "audio" and ra[0].duration_ms == 1500
    _, rs = flatten(StickerContent(file_key="s_1"))
    assert rs[0].type == "sticker"


def test_share_chat_and_user_emit_tag_only():
    t, _ = flatten(ShareChatContent(chat_id="oc_123"))
    assert "<group_card" in t and "oc_123" in t
    t, _ = flatten(ShareUserContent(user_id="ou_456"))
    assert "<contact_card" in t


def test_location_tag():
    t, _ = flatten(LocationContent(name="HQ", longitude=116.4, latitude=39.9))
    assert "<location" in t and "HQ" in t


def test_folder_hongbao_tags():
    # Folder requires a file_key (node-aligned); falls back to [folder] without one.
    assert "<folder" in flatten(FolderContent(file_key="fk_1", file_name="docs"))[0]
    assert "[folder]" == flatten(FolderContent(file_name="docs"))[0]
    assert "<hongbao" in flatten(HongbaoContent(text="新年快乐"))[0]


def test_general_calendar_and_share_calendar_event():
    # Node-aligned tags: GeneralCalendar → <calendar>, ShareCalendarEvent → <calendar_share>.
    assert "<calendar>" in flatten(GeneralCalendarContent(summary="Sync"))[0]
    assert "<calendar_share>" in flatten(
        ShareCalendarEventContent(summary="Demo", organizer="Alice")
    )[0]


def test_post_to_markdown_style_mapping():
    post = {
        "zh_cn": {
            "title": "Title",
            "content": [
                [
                    {"tag": "text", "text": "bold bit", "style": ["bold"]},
                    {"tag": "text", "text": " normal "},
                    {"tag": "a", "text": "link", "href": "https://x"},
                ],
                [{"tag": "code_block", "language": "python", "text": "print(1)"}],
            ],
        }
    }
    t, _ = flatten(PostContent(post=post))
    assert "# Title" in t
    assert "**bold bit**" in t
    assert "[link](https://x)" in t
    assert "```python" in t


def test_post_resources_include_images_media_audio_and_files_deduped():
    post = {
        "zh_cn": {
            "title": "Assets",
            "content": [
                [
                    {"tag": "text", "text": "see "},
                    {"tag": "img", "image_key": "img_1"},
                    {"tag": "img", "image_key": "img_1"},
                    {"tag": "media", "file_key": "vid_1"},
                ],
                [
                    {"tag": "audio", "file_key": "aud_1"},
                    {"tag": "file", "file_key": "file_1", "file_name": "report.pdf"},
                ],
            ],
        }
    }

    t, r = flatten(PostContent(post=post))

    assert "![image](img_1)" in t
    assert "[media:vid_1]" in t
    assert [(x.type, x.file_key, x.file_name) for x in r] == [
        ("image", "img_1", None),
        ("video", "vid_1", None),
        ("audio", "aud_1", None),
        ("file", "file_1", "report.pdf"),
    ]


def test_post_direct_document_shape_flattens_text_and_resources():
    post = {
        "title": "Direct",
        "content": [
            [
                {"tag": "text", "text": "hello "},
                {"tag": "a", "text": "link", "href": "https://x"},
                {"tag": "img", "image_key": "img_direct"},
            ]
        ],
    }

    t, r = flatten(PostContent(post=post))

    assert "# Direct" in t
    assert "[link](https://x)" in t
    assert r[0].type == "image"
    assert r[0].file_key == "img_direct"


def test_post_content_v2_md_preferred_and_post_processed():
    post = {
        "zh_cn": {
            "title": "V2",
            "content": [[{"tag": "text", "text": "legacy content"}]],
            "content_v2": [
                [
                    {
                        "tag": "md",
                        "text": (
                            'hello <at user_id="ou_1">Alice</at> '
                            'and <at user_id="all">All</at> '
                            "![diagram](img_v2)\n\n"
                            "```text\n"
                            '<at user_id="ou_code">Code</at> ![ignored](img_code)\n'
                            "```"
                        ),
                    }
                ]
            ],
        }
    }

    t, r = flatten(PostContent(post=post))

    assert "# V2" in t
    assert "legacy content" not in t
    assert "hello @Alice and @all ![diagram](img_v2)" in t
    assert '<at user_id="ou_code">Code</at> ![ignored](img_code)' in t
    assert [(x.type, x.file_key) for x in r] == [("image", "img_v2")]


def test_post_content_v2_empty_falls_back_to_content():
    """An empty content_v2 list must fall back to legacy content paragraphs."""
    post = {
        "zh_cn": {
            "title": "Fallback",
            "content_v2": [],
            "content": [[{"tag": "text", "text": "from legacy"}]],
        }
    }

    t, r = flatten(PostContent(post=post))

    assert "# Fallback" in t
    assert "from legacy" in t
    assert r == []


def test_post_content_v2_non_list_falls_back_to_content():
    """A non-list content_v2 (malformed) must fall back to legacy content."""
    post = {
        "zh_cn": {
            "title": "Bad",
            "content_v2": "not-a-list",
            "content": [[{"tag": "text", "text": "still works"}]],
        }
    }

    t, _ = flatten(PostContent(post=post))

    assert "still works" in t


def test_post_md_text_at_all_members_alias_and_unnamed_at():
    """`all_members` resolves to @all; <at> without inner text falls back to user_id."""
    post = {
        "zh_cn": {
            "content_v2": [
                [
                    {
                        "tag": "md",
                        "text": (
                            'hi <at user_id="all_members"></at> '
                            'and <at user_id="ou_42"></at> done'
                        ),
                    }
                ]
            ],
        }
    }

    t, r = flatten(PostContent(post=post))

    assert "hi @all and @ou_42 done" in t
    assert r == []


def test_post_md_text_unclosed_fence_is_treated_as_outside():
    """An unclosed code fence must not protect at-mentions / image keys after it."""
    post = {
        "zh_cn": {
            "content_v2": [
                [
                    {
                        "tag": "md",
                        "text": (
                            'before <at user_id="ou_1">Alice</at>\n'
                            "```python\n"
                            "still no close fence ![pic](img_unclosed)\n"
                            '<at user_id="ou_2">Bob</at>'
                        ),
                    }
                ]
            ],
        }
    }

    t, r = flatten(PostContent(post=post))

    assert "before @Alice" in t
    assert "@Bob" in t
    assert [(x.type, x.file_key) for x in r] == [
        ("image", "img_unclosed"),
    ]


def test_post_md_text_multiple_paired_fences_protect_inner_blocks():
    """Multiple complete fence pairs: only outside-of-fence transformations apply."""
    post = {
        "zh_cn": {
            "content_v2": [
                [
                    {
                        "tag": "md",
                        "text": (
                            "outer1 ![a](img_a)\n"
                            "```\nblock1 <at user_id=\"x\">X</at>\n```\n"
                            "outer2 ![b](img_b)\n"
                            "```\nblock2 ![c](img_c)\n```\n"
                            "outer3"
                        ),
                    }
                ]
            ],
        }
    }

    t, r = flatten(PostContent(post=post))

    # Inside-fence content preserved verbatim; outside-fence transformed.
    assert 'block1 <at user_id="x">X</at>' in t
    assert "block2 ![c](img_c)" in t
    # Only outside-fence images extracted (img_a, img_b), inner img_c skipped.
    assert [(x.type, x.file_key) for x in r] == [
        ("image", "img_a"),
        ("image", "img_b"),
    ]


def test_merge_forward_flatten_recursive():
    child = TextContent(text="child content")
    item = MergeForwardItem(
        message_id="c1",
        sender_name="Alice",
        create_time=int(__import__("time").time() * 1000),
        content=child,
    )
    content = MergeForwardContent(loading=False, items=[item])
    t, r = flatten(content)
    assert "<forwarded_messages>" in t
    assert "Alice:" in t
    assert "child content" in t


def test_merge_forward_flatten_handles_deep_tree_without_recursion_error():
    child = TextContent(text="deep child")
    for index in range(1200):
        child = MergeForwardContent(
            loading=False,
            items=[
                MergeForwardItem(
                    message_id=f"m_{index}",
                    sender_name="Alice",
                    create_time=0,
                    content=child,
                )
            ],
        )

    t, _ = flatten(child)

    assert "deep child" in t


def test_interactive_walk_picks_markdown_leaves():
    card = {
        "schema": "2.0",
        "header": {
            "title": {"tag": "plain_text", "content": "Header"},
            "subtitle": {"tag": "plain_text", "content": "sub"},
        },
        "body": {
            "elements": [
                {"tag": "markdown", "content": "body1"},
                {"tag": "column_set", "columns": [
                    {"tag": "column", "elements": [
                        {"tag": "markdown", "content": "in column"},
                    ]},
                ]},
            ],
        },
    }
    t, _ = flatten(InteractiveContent(card=card, card_version="v2"))
    for expected in ["Header", "body1", "in column"]:
        assert expected in t


def test_interactive_walk_handles_deep_card_without_recursion_error():
    node = {"tag": "markdown", "content": "deep leaf"}
    for _ in range(1500):
        node = {"tag": "column_set", "columns": [{"tag": "column", "elements": [node]}]}

    t, _ = flatten(InteractiveContent(card={"body": {"elements": [node]}}, card_version="v2"))

    assert "deep leaf" in t


def test_unknown_fallback_uses_raw_text():
    t, _ = flatten(UnknownContent(raw={"text": "raw text"}))
    assert t == "raw text"
    t, _ = flatten(UnknownContent(raw={}))
    assert t == "[unsupported message]"
