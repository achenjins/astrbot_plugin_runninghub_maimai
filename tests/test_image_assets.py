"""Persistent image use, optional vision, trusted file sources and scoped avatars."""
import asyncio
import base64
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from astrbot.api.message_components import File, Image, Reply

from rh_generic_lib.config import build_config_model, dump_config_dict
from rh_generic_lib.delivery import DeliveryTarget
from rh_generic_lib.file_source import trusted_local_file
from rh_generic_lib.media_store import MediaStore
from .test_chat_workflows import PNG, event, plugin, finish_jobs  # noqa: F401


def test_zero_and_false_survive_config_and_editor_round_trip(plugin):
    raw = dump_config_dict(plugin.config)
    raw["feature"].update(recent_images=0, avatar_candidates=False, image_descriptions=False, vision_model="my-vision")
    raw["generation"].update(max_queued=0, query_retries=0)
    raw["workflows"][0]["output_type"] = "video"
    cfg = build_config_model(raw)
    saved = dump_config_dict(cfg)
    for section in ("feature", "generation"):
        assert saved[section] == raw[section]
    plugin.config = cfg
    payload = plugin._page_config_payload()
    workflows, error = plugin._workflows_from_page_payload(payload["workflows"])
    assert not error and workflows[0].output_type == "video"


def test_used_images_are_deduplicated_described_once_and_scoped(plugin):
    async def run():
        calls = []

        async def describe(owner, data):
            calls.append(owner)
            return "一只坐在窗前的白猫"

        plugin._describe_image = describe
        first = json.loads(await plugin.handle_run_workflow(event(images=1), "", "水彩"))
        await finish_jobs(plugin)
        await plugin.handle_run_workflow(event(images=1, mid="2"), "", "油画")
        await finish_jobs(plugin)
        owner = MediaStore.owner(plugin._event_ctx(event()))
        candidates = await plugin._memory_candidates(owner)
        assert len(candidates) == 1 and len(calls) == 1
        assert candidates[0]["description"] == "一只坐在窗前的白猫"
        plugin._image_memory = None
        assert (await plugin._memory_candidates(owner))[0]["memory_id"] == candidates[0]["memory_id"]
        reply = await plugin.handle_run_workflow(event(mid="3", user="other"), "", "水彩", image_refs=[candidates[0]["memory_id"]])
        assert "不属于" in reply
        assert len(plugin._client.submissions) == 2
        plugin.config.feature.recent_images = 0
        assert await plugin._memory_candidates(owner) == []
        assert not list(plugin._image_memory.path.glob("*.img"))
    asyncio.run(run())


def test_vision_failure_never_blocks_upload_and_is_not_repeated(plugin):
    async def run():
        calls = []

        async def describe(owner, data):
            calls.append(1)
            raise TimeoutError("vision unavailable")

        plugin._describe_image = describe
        await plugin.handle_run_workflow(event(images=1), "", "水彩")
        await finish_jobs(plugin)
        await plugin.handle_run_workflow(event(images=1, mid="2"), "", "油画")
        await finish_jobs(plugin)
        assert len(plugin._client.submissions) == 2 and calls == [1]
    asyncio.run(run())


def test_vision_single_flight_uses_astrbot_provider_and_no_tools(plugin):
    async def run():
        calls = []
        plugin.config.feature.vision_model = "selected-provider"

        async def generate(**kwargs):
            calls.append(kwargs)
            await asyncio.sleep(0)
            return SimpleNamespace(completion_text="蓝色天空" * 30)

        plugin.context.llm_generate = generate
        owner = MediaStore.owner(plugin._event_ctx(event()))
        descriptions = await asyncio.gather(*(plugin._describe_image(owner, PNG) for _ in range(3)))
        assert len(set(descriptions)) == 1 and len(descriptions[0]) <= 50
        assert len(calls) == 1 and calls[0]["chat_provider_id"] == "selected-provider" and calls[0]["tools"] is None
    asyncio.run(run())


def test_avatar_candidates_only_use_current_user_and_group(plugin):
    async def run():
        calls = []

        async def action(name, **kwargs):
            calls.append((name, kwargs))
            return {"retcode": 0, "data": {"b64": base64.b64encode(PNG).decode()}}

        plugin.context.platform_inst = SimpleNamespace(get_client=lambda: SimpleNamespace(call_action=action))
        request = event()
        context = json.loads(await plugin.handle_workflow_context(request))
        assert {r["ref"] for r in context["avatars"]} == {"avatar:self", "avatar:group"}
        assert not context["images"]
        await plugin.handle_run_workflow(request, "", "画我", image_refs=["avatar:self"])
        await finish_jobs(plugin)
        assert calls == [("get_avatar", {"type": 1, "qq": 10001, "group_id": 0})]
        assert plugin._client.uploads[0][0] == PNG
        plugin.config.feature.avatar_candidates = False
        assert "候选" in await plugin.handle_run_workflow(event(mid="2"), "", "画群头像", image_refs=["avatar:group"])
        assert len(plugin._client.submissions) == 1
    asyncio.run(run())


def test_avatar_rejects_non_image_adapter_data_and_uses_cdn(plugin):
    async def run():
        async def action(name, **kwargs):
            return {"data": {"base64": base64.b64encode(b"not an image").decode()}}

        plugin.context.platform_inst = SimpleNamespace(get_client=lambda: SimpleNamespace(call_action=action))
        await plugin.handle_run_workflow(event(), "", "画群头像", image_refs=["avatar:group"])
        await finish_jobs(plugin)
        assert plugin._client.downloads == ["https://p.qlogo.cn/gh/20001/20001/640"]
    asyncio.run(run())


def test_file_images_and_opaque_image_file_with_valid_url_are_captured(plugin):
    async def run():
        request = event(messages=[File("cat.png", url="https://example.test/cat.png"),
                                  Image(file="opaque-id", url="https://example.test/emoji.gif"),
                                  Reply(id="22", chain=[File("old.png", url="https://example.test/old.png")])])
        context = json.loads(await plugin.handle_workflow_context(request))
        assert len(context["images"]) == 3
        files = await plugin._extract_files_from_event(request)
        assert files == [("image", "https://example.test/cat.png"), ("image", "https://example.test/emoji.gif")]
        assert plugin._detect_file_type_from_name("unknown.bin") == "unknown"
    asyncio.run(run())


def test_local_files_reject_outside_roots_and_remote_file_uris(plugin, tmp_path, monkeypatch):
    allowed = tmp_path / "cache"
    allowed.mkdir()
    good = allowed / "good.png"
    good.write_bytes(PNG)
    private = tmp_path / "private.txt"
    private.write_bytes(b"private")
    monkeypatch.setattr("rh_generic_lib.file_source.tempfile.gettempdir", lambda: str(allowed))
    monkeypatch.setattr(plugin, "_trusted_file_roots", lambda: [allowed])
    assert trusted_local_file(good.as_uri(), [allowed]) == good
    assert trusted_local_file(private, [allowed]) is None
    assert trusted_local_file("file://server/share/secret.png", [allowed]) is None
    assert trusted_local_file(r"\\server\share\secret.png", [allowed]) is None
    assert plugin._image_source(str(private), "https://example.test/image.png") == "https://example.test/image.png"
    with pytest.raises(Exception, match="允许的缓存"):
        asyncio.run(plugin._fetch_file_bytes(str(private)))
    assert asyncio.run(plugin._fetch_file_bytes(good.as_uri())) == PNG
    assert asyncio.run(plugin._extract_bytes_from_napcat_result({"data": {"path": str(private)}})) is None


def test_ambiguous_media_role_returns_exact_keys_without_upload(plugin):
    from rh_generic_lib.config import InputNodeSection
    plugin._workflows[0].input_nodes.append(InputNodeSection(node_id="5", field_name="image", value_type="image", label="风格"))

    async def run():
        request = event(images=1)
        context = json.loads(await plugin.handle_workflow_context(request))
        reply = json.loads(await plugin.handle_run_workflow(request, "", "水彩", image_bindings={"IMAGE": context["images"][0]["ref"]}))
        assert "2/image" in reply["message"] and "5/image" in reply["message"]
        assert reply["correction_context"]["images"]
        assert not plugin._client.uploads
    asyncio.run(run())


def test_missing_group_fields_are_recovered_from_astrbot_session():
    target = DeliveryTarget.from_dict({"stream_id": "qq:GroupMessage:222", "user_id": "111"})
    assert target.platform_id == "qq" and target.group_id == "222" and target.user_id == "111"


def test_group_image_file_resolves_adapter_url_and_is_cached_for_later(plugin):
    async def run():
        calls = []

        async def action(name, **kwargs):
            calls.append((name, kwargs))
            return {"data": {"url": "https://example.test/actual.png"}}

        plugin.context.platform_inst = SimpleNamespace(get_client=lambda: SimpleNamespace(call_action=action))
        request = event(messages=[File("cat.png", url="https://gzc-download.ftn.qq.com/bad.zip")])
        request.message_obj.raw_message = {"message": [{"type": "file", "data": {
            "name": "cat.png", "file_id": "file-1", "url": "https://gzc-download.ftn.qq.com/bad.zip"}}]}
        await plugin.remember_chat_images(request)
        ctx = json.loads(await plugin.handle_workflow_context(event(mid="later")))
        assert len(ctx["images"]) == 1 and not plugin._client.downloads
        owner = MediaStore.owner(plugin._event_ctx(request))
        assert plugin._get_media_store().get(owner, ctx["images"][0]["ref"])["source"] == "https://example.test/actual.png"
        assert calls == [("get_group_file_url", {"file_id": "file-1", "group_id": "20001"})]
    asyncio.run(run())


def test_group_image_notice_is_observed_without_consuming_user_message(plugin):
    async def run():
        async def action(name, **kwargs):
            return {"data": {"url": "https://example.test/notice.png"}}

        plugin.context.platform_inst = SimpleNamespace(get_client=lambda: SimpleNamespace(call_action=action))
        request = event(mid="notice")
        request.message_obj.raw_message = {"post_type": "notice", "notice_type": "group_upload", "group_id": 20001,
                                           "user_id": 10001, "file": {"name": "cat.png", "id": "file-2"}}
        await plugin.handle_notice_collector(request)
        ctx = json.loads(await plugin.handle_workflow_context(event(mid="later")))
        assert len(ctx["images"]) == 1 and not plugin._is_consumed(request)
        assert not plugin.context.sent and not plugin._client.downloads
    asyncio.run(run())
