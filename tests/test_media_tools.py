"""Native AstrBot media tools: scoped discovery, reusable images and typed plans."""
import asyncio
import base64
import json
from types import SimpleNamespace

import pytest
from astrbot.api.message_components import File, Image, Plain, Record, Reply, Video

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


@pytest.mark.parametrize("kind,component", [("image", Image), ("audio", Record), ("video", Video)])
def test_observed_media_uses_the_same_natural_language_access_policy_as_the_tools(plugin, kind, component):
    plugin.config.access.allow_users = ["administrator"]
    plugin.config.access.allow_groups = ["20001"]
    plugin.config.access.natural_language_ignore_user_allowlist = True

    async def run():
        await plugin.remember_chat_images(event(mid="previous", messages=[component(file="https://example.test/reference")]))
        result = json.loads(await plugin._media_catalog_tool(event(mid="later"), "recent", kind))
        assert result["success"] and len(result["media"]) == 1
        assert not json.loads(await plugin._media_catalog_tool(event(group="blocked"), "recent", kind))["success"]
        plugin.config.access.natural_language_ignore_user_allowlist = False
        assert not json.loads(await plugin._media_catalog_tool(event(mid="later"), "recent", kind))["success"]
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


@pytest.mark.parametrize("saved_before_inspection", [False, True])
def test_explicit_inspection_can_describe_images_when_automatic_descriptions_are_disabled(
        plugin, monkeypatch, saved_before_inspection):
    plugin.config.feature.image_descriptions = False
    calls = []

    async def describe(owner, data):
        calls.append((owner, data))
        return "窗边白猫"
    monkeypatch.setattr(plugin, "_describe_image", describe)

    async def run():
        request = event(images=1)
        owner = MediaStore.owner(plugin._event_ctx(request))
        ref = json.loads(await plugin._media_catalog_tool(request, "current", "image"))["media"][0]["media_id"]
        if saved_before_inspection:
            await plugin._remember_image_usage(owner, ref, PNG)
        result = json.loads(await plugin._inspect_media_tool(request, ref))
        assert result["success"] and result["description"] == "窗边白猫"
        assert calls == [(owner, PNG)]
        cached = json.loads(await plugin._inspect_media_tool(request, ref))
        assert cached["cached"] and cached["description"] == "窗边白猫"
        assert len(calls) == 1
    asyncio.run(run())


def test_explicit_inspection_returns_description_when_image_memory_is_disabled(plugin, monkeypatch):
    plugin.config.feature.recent_images = 0
    calls = []

    async def describe(owner, data):
        calls.append(data)
        return "窗边白猫"
    monkeypatch.setattr(plugin, "_describe_image", describe)

    async def run():
        request = event(images=1)
        ref = json.loads(await plugin._media_catalog_tool(request, "current", "image"))["media"][0]["media_id"]
        result = json.loads(await plugin._inspect_media_tool(request, ref))
        assert result["success"] and result["description"] == "窗边白猫"
        assert result["media_id"] == ref and calls == [PNG]
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


@pytest.mark.parametrize("kind,component,action_name,arguments,data,extension", [
    ("audio", Record, "get_record", {"file": "qq-media-id", "out_format": "mp3"}, b"ID3voice", ".mp3"),
    ("video", Video, "get_file", {"file": "qq-media-id"}, b"\x00\x00\x00\x18ftypmp42video", ".mp4"),
])
def test_opaque_media_uses_documented_host_adapter_api(
        plugin, kind, component, action_name, arguments, data, extension):
    actions = []

    async def action(name, **kwargs):
        actions.append((name, kwargs))
        # NapCat registers get_file for video and get_record for transcoding.
        assert name == action_name
        return {"data": {"base64": base64.b64encode(data).decode()}}
    plugin.context.platform_inst = SimpleNamespace(get_client=lambda: SimpleNamespace(call_action=action))

    async def run():
        request = event(messages=[component(file="qq-media-id")])
        ref = json.loads(await plugin._media_catalog_tool(request, "current", kind))["media"][0]["media_id"]
        owner = MediaStore.owner(plugin._event_ctx(request))
        actual, name = await plugin._materialize_media(plugin._get_media_store(), owner, ref, plugin._client)
        assert actual == data and name.endswith(extension)
        assert actions == [(action_name, arguments)]
    asyncio.run(run())


@pytest.mark.parametrize("kind,component,raw_type,extension", [
    ("audio", Record, "record", "mp3"),
    ("video", Video, "video", "mp4"),
])
@pytest.mark.parametrize("quoted", [False, True])
def test_native_and_raw_media_are_one_input_after_host_component_conversion(
        plugin, monkeypatch, kind, component, raw_type, extension, quoted):
    plugin._workflows[0].input_nodes[1] = InputNodeSection(
        node_id="2", field_name=kind, value_type=kind, required=True)
    url = f"https://example.test/reference.{extension}"
    # AstrBot combines adjacent text segments into one Plain component. Raw
    # message positions therefore cannot identify slots in the parsed chain.
    raw = {"group_id": 20001, "message": [
        {"type": "text", "data": {"text": "first"}},
        {"type": "text", "data": {"text": "second"}},
        {"type": raw_type, "data": {"file": "host-media-id", "url": url}},
    ]}

    async def action(name, **kwargs):
        assert name == "get_msg"
        return raw
    plugin.context.platform_inst = SimpleNamespace(get_client=lambda: SimpleNamespace(call_action=action))
    captured = []

    async def start(name, prompt, **context):
        captured.append(context)
        return {"success": True, "task_id": "rh-one-media", "status": "queued"}
    monkeypatch.setattr(plugin, "_start_workflow", start)

    async def run():
        chain = [Plain(text="firstsecond"), component(file="host-media-id", url=url)]
        request = event(messages=[Reply(id="88", chain=chain)] if quoted else chain)
        if not quoted:
            request.message_obj.raw_message = raw
        result = json.loads(await plugin._run_natural_workflow(request, "改图", "处理这份素材"))
        assert result.get("task_id") == "rh-one-media", result
        assert len(captured[0]["media_bindings"]) == 1
        assets = json.loads(await plugin._media_catalog_tool(request, "reply" if quoted else "current", kind))
        assert len(assets["media"]) == 1
    asyncio.run(run())


@pytest.mark.parametrize("kind,extension", [("image", "png"), ("audio", "mp3"), ("video", "mp4")])
def test_private_file_media_uses_the_native_private_file_url_api(plugin, kind, extension):
    calls = []
    url = f"https://example.test/reference.{extension}"

    async def action(name, **kwargs):
        calls.append((name, kwargs))
        assert name == "get_private_file_url"
        return {"url": url}
    plugin.context.platform_inst = SimpleNamespace(get_client=lambda: SimpleNamespace(call_action=action))

    async def run():
        name = f"reference.{extension}"
        request = event(group="", messages=[File(name, url="https://gzc-download.ftn.qq.com/stale.zip")])
        request.message_obj.raw_message = {"message": [{"type": "file", "data": {"name": name, "file_id": "private-file-id"}}]}
        result = json.loads(await plugin._media_catalog_tool(request, "current", kind))
        assert result["success"] and len(result["media"]) == 1
        assert await plugin._extract_files_from_event(request) == [(kind, url)]
        assert calls == [("get_private_file_url", {"file_id": "private-file-id"})] * 2
    asyncio.run(run())


@pytest.mark.parametrize("kind,extension", [("audio", "mp3"), ("video", "mp4")])
@pytest.mark.parametrize("group", ["20001", ""], ids=["group", "private"])
def test_quoted_media_files_resolve_adapter_placeholders(plugin, kind, extension, group):
    calls = []
    stale = "https://gzc-download.ftn.qq.com/stale.zip"
    actual = f"https://example.test/reference.{extension}"
    filename = f"reference.{extension}"

    async def action(name, **kwargs):
        calls.append((name, kwargs))
        if name == "get_msg":
            return {"group_id": group, "sender": {"user_id": "10001"}, "message": [
                {"type": "file", "data": {"name": filename, "file_id": "quoted-file-id", "url": stale}}]}
        assert name == ("get_group_file_url" if group else "get_private_file_url")
        return {"url": actual}
    plugin.context.platform_inst = SimpleNamespace(get_client=lambda: SimpleNamespace(call_action=action))

    async def run():
        request = event(group=group, messages=[Reply(id="88", chain=[File(filename, url=stale)])])
        result = json.loads(await plugin._media_catalog_tool(request, "reply", kind))
        assert result["success"] and len(result["media"]) == 1
        owner = MediaStore.owner(plugin._event_ctx(request))
        entry = plugin._get_other_media_registry().get(owner, result["media"][0]["media_id"])
        assert entry["source"] == actual
        parameters = {"file_id": "quoted-file-id", **({"group_id": group} if group else {})}
        assert calls == [("get_msg", {"message_id": 88}),
                         ("get_group_file_url" if group else "get_private_file_url", parameters)]
    asyncio.run(run())


@pytest.mark.parametrize("group", ["20001", ""], ids=["group", "private"])
@pytest.mark.parametrize("quoted", [False, True], ids=["current", "reply"])
def test_file_lookup_failure_keeps_other_media_and_warns(plugin, group, quoted):
    calls = []
    stale = "https://gzc-download.ftn.qq.com/stale.zip"
    image = "https://example.test/good.png"
    audio = "https://example.test/good.mp3"

    async def action(name, **kwargs):
        calls.append((name, kwargs))
        if name == "get_msg":
            return raw
        raise TimeoutError("file resolver timed out")
    plugin.context.platform_inst = SimpleNamespace(get_client=lambda: SimpleNamespace(call_action=action))
    raw = {"group_id": group, "sender": {"user_id": "10001"}, "message": [
        {"type": "image", "data": {"url": image}},
        {"type": "file", "data": {"name": "bad.mp4", "file_id": "bad-file-id", "url": stale}},
        {"type": "record", "data": {"file": audio}},
    ]}

    async def run():
        request = event(group=group, messages=[Reply(id="88", chain=[])] if quoted else [
            Image.fromURL(image), File("bad.mp4", url=stale), Record(file=audio)])
        if not quoted:
            request.message_obj.raw_message = raw
        origin = "reply" if quoted else "current"
        result = json.loads(await plugin._media_catalog_tool(request, origin))
        assert result["success"], result
        assert sorted(item["type"] for item in result["media"]) == ["audio", "image"]
        assert result["warnings"] and not request.get_extra(f"rh_media_{origin}_complete")
        parameters = {"file_id": "bad-file-id", **({"group_id": group} if group else {})}
        assert calls == ([("get_msg", {"message_id": 88})] if quoted else []) + [
            ("get_group_file_url" if group else "get_private_file_url", parameters)]
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


@pytest.mark.parametrize("kind,component,data,extension", [
    ("audio", Record, b"ID3voice", ".mp3"),
    ("video", Video, b"\x00\x00\x00\x18ftypmp42video", ".mp4"),
])
def test_base64_media_is_preserved_before_acknowledgement_then_uploaded_and_submitted(
        plugin, monkeypatch, kind, component, data, extension):
    plugin._workflows[0].input_nodes[1] = InputNodeSection(
        node_id="2", field_name=kind, value_type=kind, label="参考素材", required=True)
    scheduled = []
    monkeypatch.setattr(plugin, "_schedule_job", lambda record: scheduled.append(record["task_id"]))

    async def run():
        request = event(messages=[component(file="base64://" + base64.b64encode(data).decode())])
        catalog = json.loads(await plugin._media_catalog_tool(request, "current", kind))
        ref = catalog["media"][0]["media_id"]
        result = json.loads(await plugin._run_natural_workflow(
            request, "改图", "生成作品", media_bindings={"参考素材": ref}))
        task_id = result["task_id"]
        assert result["status"] == "queued" and scheduled == [task_id]
        item = plugin._task_journal.get(task_id)["inputs"][ref]
        assert item["type"] == kind and item["filename"].endswith(extension)
        assert await plugin._get_task_inputs().read_async(task_id, item) == data
        assert not plugin._client.uploads and not plugin._client.submissions

        # The task must use its retained bytes even if the chat reference expires.
        plugin._get_other_media_registry().entries.clear()
        await plugin._run_job(task_id)
        assert plugin._task_journal.get(task_id)["status"] == "success"
        assert plugin._client.uploads == [(data, item["filename"])]
        assert len(plugin._client.submissions) == 1
        nodes = {node["fieldName"]: node["fieldValue"] for node in plugin._client.submissions[0][0]}
        assert nodes[kind] == "api/upload-1.png"
        assert not plugin._client.downloads
    asyncio.run(run())


@pytest.mark.parametrize("kind,component,header", [
    ("audio", Record, b"ID3"),
    ("video", Video, b"\x00\x00\x00\x18ftypmp42"),
])
def test_oversized_base64_media_cannot_be_reserved_uploaded_or_submitted(plugin, kind, component, header):
    plugin.config.generation.max_file_mb = 1
    plugin._workflows[0].input_nodes[1] = InputNodeSection(
        node_id="2", field_name=kind, value_type=kind, label="参考素材", required=True)
    data = header + b"x" * (1024 * 1024 + 1 - len(header))

    async def run():
        request = event(messages=[component(file="base64://" + base64.b64encode(data).decode())])
        catalog = json.loads(await plugin._media_catalog_tool(request, "current", kind))
        ref = catalog["media"][0]["media_id"]
        result = json.loads(await plugin._run_natural_workflow(
            request, "改图", "生成作品", media_bindings={"参考素材": ref}))
        assert "大小上限" in result["message"]
        assert not (await plugin._load_task_journal()).records()
        assert not list(plugin._get_task_inputs().root.glob("rh-*"))
        assert not plugin._client.uploads and not plugin._client.submissions
        assert not plugin._client.downloads
    asyncio.run(run())


@pytest.mark.parametrize("source_kind", ["path", "file_uri", "remote_file_uri", "unc"])
def test_preserving_media_still_rejects_untrusted_local_paths(plugin, tmp_path, monkeypatch, source_kind):
    allowed = tmp_path / "cache"
    allowed.mkdir()
    private = tmp_path / "private.mp3"
    private.write_bytes(b"ID3private")
    monkeypatch.setattr("rh_generic_lib.file_source.tempfile.gettempdir", lambda: str(allowed))
    monkeypatch.setattr(plugin, "_trusted_file_roots", lambda: [allowed])
    source = {"path": str(private), "file_uri": private.as_uri(),
              "remote_file_uri": "file://server/share/private.mp3",
              "unc": r"\\server\share\private.mp3"}[source_kind]
    plugin._workflows[0].input_nodes[1] = InputNodeSection(
        node_id="2", field_name="audio", value_type="audio", label="参考素材", required=True)

    async def run():
        request = event()
        owner = MediaStore.owner(plugin._event_ctx(request))
        entry = plugin._get_other_media_registry().remember(
            owner, "audio", source, message_id="earlier", position=1, origin="current")
        result = json.loads(await plugin._run_natural_workflow(
            request, "改图", "生成作品", media_bindings={"参考素材": entry["ref"]}))
        assert "参考素材缓存不可用" in result["message"]
        assert not (await plugin._load_task_journal()).records()
        assert not plugin._client.uploads and not plugin._client.submissions
        assert not plugin._client.downloads
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
