"""Regressions for passive quote capture and recoverable adapter failures."""
import asyncio
import json
from types import SimpleNamespace

import pytest
from astrbot.api.message_components import Image, Plain, Reply

from .test_chat_workflows import event, plugin  # noqa: F401


def onebot(plugin, action):
    plugin.context.platform_inst = SimpleNamespace(get_client=lambda: SimpleNamespace(call_action=action))


def test_plain_quote_is_remembered_for_later_text_request(plugin):
    async def run():
        quoted = event("这张可以", messages=[Reply(id="88", chain=[Image.fromURL("https://example.test/ref.png")])])
        await plugin.remember_chat_images(quoted)
        assert not plugin._is_consumed(quoted) and not plugin.context.sent
        context = json.loads(await plugin.handle_workflow_context(event("用刚才引用的图", mid="later")))
        assert len(context["images"]) == 1
        assert context["images"][0]["origin"] == "reply"
        assert context["images"][0]["source"] == "recent"
        assert not json.loads(await plugin.handle_workflow_context(event(user="other")))["images"]
    asyncio.run(run())


def test_failed_lookup_retries_in_same_event(plugin):
    calls = []

    async def action(name, **kwargs):
        calls.append(name)
        if len(calls) == 1:
            raise TimeoutError("temporary")
        return {"group_id": 20001, "message": [{"type": "image", "data": {"url": "https://example.test/retry.png"}}]}

    onebot(plugin, action)

    async def run():
        request = event(messages=[Reply(id="88", chain=[])])
        await plugin.remember_chat_images(request)
        assert not request.get_extra("rh_images_reply")
        context = json.loads(await plugin.handle_workflow_context(request))
        assert len(context["images"]) == 1
        assert len(calls) == 2
        await plugin.handle_workflow_context(request)
        assert len(calls) == 2
    asyncio.run(run())


def test_nonempty_text_chain_uses_fallback_only_when_tool_needs_it(plugin):
    calls = []

    async def action(name, **kwargs):
        calls.append(name)
        return {"data": {"group_id": 20001, "message": [{"type": "image", "data": {"url": "https://example.test/found.png"}}]}}

    onebot(plugin, action)

    async def run():
        request = event(messages=[Reply(id="88", chain=[Plain("[Image]")])])
        await plugin.remember_chat_images(request)
        assert not calls
        assert len(json.loads(await plugin.handle_workflow_context(request))["images"]) == 1
        assert calls == ["get_msg"]
    asyncio.run(run())


def test_current_unresolved_image_can_be_resolved_later_with_same_event(plugin):
    image = Image(file="opaque-qq-file")

    async def run():
        request = event("带文字的图", messages=[image])
        await plugin.remember_chat_images(request)
        assert not request.get_extra("rh_images_current")
        image.url = "https://example.test/resolved.png"
        assert len(json.loads(await plugin.handle_workflow_context(request))["images"]) == 1
    asyncio.run(run())


def test_partial_quote_fallback_keeps_known_images_in_their_original_positions(plugin):
    async def action(*args, **kwargs):
        return {"group_id": 20001, "message": [
            {"type": "image", "data": {"file": "opaque"}},
            {"type": "image", "data": {"url": "https://example.test/second.png"}},
        ]}
    onebot(plugin, action)

    async def run():
        request = event(messages=[Reply(id="88", chain=[
            Image.fromURL("https://example.test/first.png"), Image(file="opaque"),
        ])])
        context = json.loads(await plugin.handle_workflow_context(request))
        assert sorted(i["position"] for i in context["images"]) == [1, 2]
    asyncio.run(run())


@pytest.mark.parametrize("raw", [
    {"group_id": 999, "message": [{"type": "image", "data": {"url": "https://example.test/foreign.png"}}]},
    {"status": "failed", "retcode": 1, "message": "not found"},
    {"data": {}},
])
def test_unusable_quote_lookup_is_not_registered(plugin, raw):
    async def action(*args, **kwargs):
        return raw
    onebot(plugin, action)

    async def run():
        result = await plugin._remember_event_images(event(messages=[Reply(id="88", chain=[])]))
        assert not result["reply"]
    asyncio.run(run())
