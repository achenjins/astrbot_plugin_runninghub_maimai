"""Native AstrBot media tools: scoped discovery, reusable images and typed plans."""
import asyncio
import json
from types import SimpleNamespace

import pytest
from astrbot.api.message_components import Image, Record, Reply, Video

from rh_generic_lib.config import InputNodeSection
from rh_generic_lib.media_store import MediaStore
from .test_chat_workflows import PNG, event, plugin, finish_jobs  # noqa: F401


def test_workflow_catalog_brief_and_explicit_detail_do_not_read_media(plugin, monkeypatch):
    async def unexpected(*args, **kwargs):
        raise AssertionError("Workflow discovery should not read message history")
    monkeypatch.setattr(plugin, "_remember_event_images", unexpected)

    async def run():
        brief = json.loads(await plugin._workflow_catalog_tool(event()))
        assert brief["success"] and "inputs" not in brief["workflows"][0]
        assert brief["workflows"][0]["media_inputs"][0]["type"] == "image"
        detail = json.loads(await plugin._workflow_catalog_tool(event(), "改图"))
        inputs = detail["workflows"][0]["inputs"]
        assert next(node for node in inputs if node["key"] == "3/width")["maximum"] == 2048
        assert not any(node["key"] == "4/model" for node in inputs)
        assert "test-key" not in json.dumps(detail)
        assert not json.loads(await plugin._workflow_catalog_tool(event(), "missing"))["success"]
    asyncio.run(run())


def test_media_query_filters_current_reply_recent_and_never_exposes_sources(plugin):
    async def run():
        request = event(messages=[Image.fromURL("https://private.test/current.png"),
                                  Record(file="https://private.test/voice.mp3"),
                                  Reply(id="88", chain=[Video(file="https://private.test/quoted.mp4")])])
        current = json.loads(await plugin._media_catalog_tool(request, "current"))
        assert {item["type"] for item in current["media"]} == {"image", "audio"}
        quoted = json.loads(await plugin._media_catalog_tool(request, "reply", "video"))
        assert len(quoted["media"]) == 1 and quoted["media"][0]["source"] == "reply"
        all_media = json.loads(await plugin._media_catalog_tool(request))
        assert len(all_media["media"]) == 3
        assert "private.test" not in json.dumps(all_media)
        later = json.loads(await plugin._media_catalog_tool(event(mid="2"), "recent", "audio"))
        assert later["media"][0]["media_id"] == current["media"][1]["media_id"]
        assert not json.loads(await plugin._media_catalog_tool(event(user="other"), "recent"))["media"]
        assert not json.loads(await plugin._media_catalog_tool(request, "url"))["success"]
    asyncio.run(run())


def test_memory_only_and_cached_inspect_do_not_read_chats_download_or_reinspect(plugin, monkeypatch):
    async def run():
        owner = MediaStore.owner(plugin._event_ctx(event()))
        saved = await plugin._remember_image_usage(owner, "old-host-id", PNG, description="窗边白猫")

        async def unexpected(*args, **kwargs):
            raise AssertionError("Saved image must not read/download/describe again")
        monkeypatch.setattr(plugin, "_remember_event_images", unexpected)
        monkeypatch.setattr(plugin, "_describe_image", unexpected)
        monkeypatch.setattr(plugin._client, "download_bytes", unexpected)
        plugin._media_store = None
        catalog = json.loads(await plugin._media_catalog_tool(event(mid="2"), "memory"))
        assert catalog["media"][0]["media_id"] == saved["memory_id"]
        result = json.loads(await plugin._inspect_media_tool(event(mid="2"), "old-host-id"))
        assert result["cached"] and result["description"] == "窗边白猫"
        data, _ = await plugin._materialize_image(plugin._get_media_store(), owner, "old-host-id", plugin._client)
        assert data == PNG
        assert not json.loads(await plugin._inspect_media_tool(event(user="other"), saved["memory_id"]))["success"]
    asyncio.run(run())


def test_audio_video_inspect_does_not_invoke_vision_or_download(plugin, monkeypatch):
    async def run():
        request = event(messages=[Record(file="https://example.test/input.mp3"), Video(file="https://example.test/input.mp4")])
        assets = json.loads(await plugin._media_catalog_tool(request))["media"]

        async def unexpected(*args, **kwargs):
            raise AssertionError("Audio/video must not use vision or decode during discovery")
        monkeypatch.setattr(plugin, "_materialize_media", unexpected)
        monkeypatch.setattr(plugin, "_describe_image", unexpected)
        for asset in assets:
            result = json.loads(await plugin._inspect_media_tool(request, asset["media_id"]))
            assert result["success"] and "不解码" in result["message"]
    asyncio.run(run())


def test_mixed_current_replied_saved_bindings_create_typed_deferred_nodes(plugin, monkeypatch):
    plugin._workflows[0].input_nodes.extend([
        InputNodeSection(node_id="5", field_name="audio", value_type="audio", label="配音"),
        InputNodeSection(node_id="6", field_name="video", value_type="video", label="动作参考"),
    ])
    captured = []

    async def start(name, prompt, **context):
        captured.append(context)
        return {"success": True, "task_id": "rh-example", "status": "queued"}
    monkeypatch.setattr(plugin, "_start_workflow", start)

    async def run():
        owner = MediaStore.owner(plugin._event_ctx(event()))
        saved = await plugin._remember_image_usage(owner, "saved", PNG, description="角色立绘")
        request = event(messages=[Record(file="https://example.test/voice.mp3"),
                                  Reply(id="88", chain=[Video(file="https://example.test/motion.mp4")])])
        media = json.loads(await plugin._media_catalog_tool(request))["media"]
        by_type = {item["type"]: item["media_id"] for item in media}
        result = await plugin._run_natural_workflow(request, "改图", "让角色跳舞", media_bindings={
            "人物参考": saved["memory_id"], "配音": by_type["audio"], "动作参考": by_type["video"]})
        assert json.loads(result)["task_id"] == "rh-example"
        context = captured[0]
        assert context["node_overrides"]["2/image"] == "rh-image-ref:" + saved["memory_id"]
        assert context["node_overrides"]["5/audio"] == "rh-media-ref:" + by_type["audio"]
        assert context["node_overrides"]["6/video"] == "rh-media-ref:" + by_type["video"]
        assert context["media_sources"][by_type["audio"]]["type"] == "audio"
        assert not plugin._client.uploads
        invalid = json.loads(await plugin._run_natural_workflow(event(mid="2"), "改图", "角色", media_bindings={"配音": saved["memory_id"]}))
        assert "无法绑定" in invalid["message"] and len(captured) == 1
    asyncio.run(run())


def test_distinct_effective_requests_in_same_message_are_not_suppressed(plugin):
    async def run():
        request = event(images=1)
        first = json.loads(await plugin._run_natural_workflow(request, "改图", "水彩"))
        second = json.loads(await plugin._run_natural_workflow(request, "改图", "油画"))
        assert first["task_id"] != second["task_id"]
        duplicate = json.loads(await plugin._run_natural_workflow(request, "改图", "水彩"))
        assert duplicate["task_id"] == first["task_id"]
        await finish_jobs(plugin)
        assert len(plugin._client.submissions) == 2
    asyncio.run(run())


def test_result_media_uses_journal_and_rejects_foreign_owners(plugin):
    async def run():
        context = plugin._event_ctx(event())
        journal = await plugin._load_task_journal()
        await journal.update("rh-result", **context, status="success", workflow="动画", outputs=[
            {"url": "https://private.test/result.mp4", "type": "video"}])
        result = json.loads(await plugin._media_catalog_tool(event(), "result", "video"))
        assert result["media"][0]["media_id"] == "result:rh-result:1"
        assert "private.test" not in json.dumps(result)
        owner = MediaStore.owner(context)
        entry = await plugin._validate_media_ref(owner, result["media"][0]["media_id"], "video")
        assert entry["source"].endswith("result.mp4")
        with pytest.raises(ValueError, match="不属于"):
            await plugin._validate_media_ref(MediaStore.owner(plugin._event_ctx(event(user="other"))), entry["ref"])
    asyncio.run(run())


def test_opaque_voice_uses_only_its_host_adapter_file_id(plugin):
    actions = []

    async def action(name, **kwargs):
        actions.append((name, kwargs))
        return {"data": {"base64": __import__("base64").b64encode(b"ID3voice").decode()}}
    plugin.context.platform_inst = SimpleNamespace(get_client=lambda: SimpleNamespace(call_action=action))

    async def run():
        request = event(messages=[Record(file="qq-voice-id")])
        ref = json.loads(await plugin._media_catalog_tool(request, "current", "audio"))["media"][0]["media_id"]
        owner = MediaStore.owner(plugin._event_ctx(request))
        data, name = await plugin._materialize_media(plugin._get_media_store(), owner, ref, plugin._client)
        assert data == b"ID3voice" and name.endswith(".mp3")
        assert actions == [("get_record", {"file": "qq-voice-id", "out_format": "mp3"})]
    asyncio.run(run())


def test_unreadable_quote_returns_warning_instead_of_claiming_empty_chat(plugin):
    async def action(*args, **kwargs):
        raise TimeoutError("temporary")
    plugin.context.platform_inst = SimpleNamespace(get_client=lambda: SimpleNamespace(call_action=action))

    async def run():
        request = event(messages=[Reply(id="88", chain=[])])
        result = json.loads(await plugin._media_catalog_tool(request, "reply"))
        assert result["success"] and not result["media"]
        assert any("引用" in warning for warning in result["warnings"])
        memory = json.loads(await plugin._media_catalog_tool(request, "memory"))
        assert not memory["warnings"]
    asyncio.run(run())


def test_conflicting_image_apis_and_mismatched_media_type_are_rejected(plugin, monkeypatch):
    async def unexpected(*args, **kwargs):
        raise AssertionError("Invalid binding must never start a task")
    monkeypatch.setattr(plugin, "_start_workflow", unexpected)

    async def run():
        request = event(images=1)
        image_id = json.loads(await plugin._media_catalog_tool(request, "current", "image"))["media"][0]["media_id"]
        result = json.loads(await plugin._run_natural_workflow(request, "改图", "水彩", image_refs=[image_id],
                                                             media_bindings={"人物参考": image_id}))
        assert "不能同时指定" in result["message"]
    asyncio.run(run())


def test_audio_download_and_adapter_decoding_obey_configured_file_limit(plugin):
    async def run():
        plugin.config.generation.max_file_mb = 1
        limits = []

        async def download(url, *, max_bytes):
            limits.append(max_bytes)
            return b"ID3voice"
        plugin._client.download_bytes = download
        request = event(messages=[Record(file="https://example.test/voice.mp3")])
        ref = json.loads(await plugin._media_catalog_tool(request, "current", "audio"))["media"][0]["media_id"]
        owner = MediaStore.owner(plugin._event_ctx(request))
        await plugin._materialize_media(plugin._get_media_store(), owner, ref, plugin._client)
        assert limits == [1024 * 1024]
        with pytest.raises(ValueError, match="大小上限"):
            await plugin._adapter_media_bytes({"data": {"base64": "A" * (2 * 1024 * 1024)}}, plugin._client, 1024 * 1024)
    asyncio.run(run())


@pytest.mark.parametrize("scope,raw", [
    ("20001", {"sender": {"user_id": "10001"}}),
    ("20001", {"group_id": 999}),
    ("", {"group_id": 20001, "sender": {"user_id": "10001"}}),
    ("", {"sender": {"user_id": "other"}}),
])
def test_reply_audio_fallback_rejects_unknown_or_foreign_chat_scope(plugin, scope, raw):
    async def action(*args, **kwargs):
        return {**raw, "message": [{"type": "record", "data": {"url": "https://example.test/private.mp3"}}]}
    plugin.context.platform_inst = SimpleNamespace(get_client=lambda: SimpleNamespace(call_action=action))

    async def run():
        result = json.loads(await plugin._media_catalog_tool(event(group=scope, messages=[Reply(id="88", chain=[])]), "reply"))
        assert result["success"] and not result["media"]
        assert result["warnings"]
    asyncio.run(run())
