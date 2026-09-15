"""Natural-language runs against real AstrBot types and a fake RunningHub client."""
from __future__ import annotations

import asyncio
import base64
import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from astrbot.api.message_components import Image, Reply
from astrbot.core.provider.register import llm_tools

import main as plugin_main
from rh_generic_lib.chat_workflows import parameter_value
from rh_generic_lib.config import InputNodeSection, build_config_model, dump_config_dict
from rh_generic_lib.media_store import MediaStore
from .test_plugin import FakeContext, FakeEvent


PNG = b"\x89PNG\r\n\x1a\n" + b"test image"


def event(text="", *, images=0, user="10001", group="20001", mid="1", messages=None):
    value = FakeEvent(text, user_id=user, group_id=group,
                      messages=messages if messages is not None else [Image.fromURL(f"https://example.test/{mid}/{i}.png") for i in range(images)])
    value.message_obj = SimpleNamespace(message_id=mid)
    return value


class Client:
    def __init__(self):
        self.downloads = []
        self.uploads = []
        self.submissions = []

    async def download_bytes(self, url, **kwargs):
        self.downloads.append(url)
        return PNG

    async def upload_file(self, data, filename):
        self.uploads.append((data, filename))
        return f"api/upload-{len(self.uploads)}.png"

    async def submit(self, node_info, **kwargs):
        self.submissions.append((node_info, kwargs))
        return f"task-{len(self.submissions)}"


@pytest.fixture
def plugin(tmp_path, monkeypatch):
    monkeypatch.setattr(plugin_main, "get_astrbot_plugin_data_path", lambda: str(tmp_path))
    star = plugin_main.RunningHubGenericPlugin(FakeContext(), None)
    star._apply_config_dict({"server": {"api_key": "test-key"}, "workflows": [{
        "name": "改图", "workflow_id": "42", "description": "人物换背景",
        "input_nodes": [
            {"node_id": "1", "field_name": "prompt", "value_type": "prompt"},
            {"node_id": "2", "field_name": "image", "value_type": "image", "label": "人物参考", "required": True},
            {"node_id": "3", "field_name": "width", "value_type": "text", "label": "宽度", "field_value": "512",
             "param_type": "integer", "minimum": 256, "maximum": 2048},
            {"node_id": "4", "field_name": "model", "value_type": "default", "field_value": "fixed-model"},
        ]}]})
    star._refresh_workflows()
    star._client = Client()

    async def no_poll(task_id, *args, **kwargs):
        await star._task_journal.update(task_id, status="success", delivery_status="sent")

    monkeypatch.setattr(star, "_poll_and_send", no_poll)
    return star


async def finish_jobs(plugin):
    if plugin._pending:
        await asyncio.wait_for(asyncio.gather(*list(plugin._pending.values())), 5)


async def with_jobs(coro, plugin):
    await coro
    await finish_jobs(plugin)


def values(plugin):
    return {f"{n['nodeId']}/{n['fieldName']}": n["fieldValue"] for n in plugin._client.submissions[-1][0]}


def test_actual_astrbot_tool_schemas():
    tool = llm_tools.get_func("run_workflow")
    props = tool.parameters["properties"]
    assert props["image_refs"]["type"] == "array"
    assert props["image_refs"]["items"]["type"] == "string"
    assert props["image_bindings"]["type"] == "object"
    assert props["parameters"]["type"] == "object"
    assert llm_tools.get_func("get_workflow_context") is not None


def test_current_image_and_parameters_submit_once_without_changing_config(plugin):
    async def run():
        request = event("把这张图改成水彩，宽度1024", images=1)
        reply = await plugin.handle_run_workflow(request, "改图", "改成水彩", parameters={"宽度": 1024})
        duplicate = await plugin.handle_run_workflow(request, "改图", "再调用一次")
        assert json.loads(reply)["task_id"] == json.loads(duplicate)["task_id"]
        assert json.loads(reply)["task_id"].startswith("rh-")
    asyncio.run(with_jobs(run(), plugin))
    assert len(plugin._client.submissions) == 1
    assert len(plugin._client.uploads) == 1
    assert values(plugin)["3/width"] == "1024"
    assert values(plugin)["2/image"] == "api/upload-1.png"
    assert plugin._workflows[0].input_nodes[2].field_value == "512"
    assert not plugin._input_sessions


def test_quoted_image_uses_reply_chain(plugin):
    async def run():
        request = event("给这张图换背景", messages=[Reply(id="88", chain=[Image.fromURL("https://example.test/quoted.png")])])
        reply = await plugin.handle_run_workflow(request, "改图", "更换背景")
        assert "queued" in reply
    asyncio.run(with_jobs(run(), plugin))
    assert plugin._client.downloads == ["https://example.test/quoted.png"]


def test_quote_fallback_calls_napcat_get_msg(plugin):
    actions = []
    class Bot:
        async def call_action(self, action, **kwargs):
            actions.append((action, kwargs))
            return {"group_id": 20001, "message": [{"type": "image", "data": {"url": "https://example.test/fallback.png"}}]}
    plugin.context.platform_inst = SimpleNamespace(get_client=lambda: Bot())
    async def run():
        request = event(messages=[Reply(id="88", chain=[])])
        assert "queued" in await plugin.handle_run_workflow(request, "改图", "水彩")
    asyncio.run(with_jobs(run(), plugin))
    assert actions == [("get_msg", {"message_id": 88})]
    assert plugin._client.downloads == ["https://example.test/fallback.png"]


def test_previous_message_images_are_selectable_and_not_consumed(plugin):
    async def run():
        previous = event(images=1, mid="earlier")
        await plugin.remember_chat_images(previous)
        assert not plugin._is_consumed(previous)
        assert not plugin.context.sent
        request = event("用刚才那张", mid="later")
        context = json.loads(await plugin.handle_workflow_context(request))
        ref = context["images"][0]["ref"]
        assert context["images"][0]["source"] == "recent"
        assert "https://" not in json.dumps(context)
        assert "queued" in await plugin.handle_run_workflow(request, "改图", "水彩", image_refs=[ref])
    asyncio.run(with_jobs(run(), plugin))


@pytest.mark.parametrize("other", [{"user": "other"}, {"group": "other"}])
def test_foreign_image_reference_rejected_before_upload(plugin, other):
    async def run():
        ctx = json.loads(await plugin.handle_workflow_context(event(images=1)))
        reply = await plugin.handle_run_workflow(event(**other), "改图", "水彩", image_refs=[ctx["images"][0]["ref"]])
        assert "不属于" in reply
    asyncio.run(with_jobs(run(), plugin))
    assert not plugin._client.uploads
    assert not plugin._client.submissions


def test_current_and_quoted_images_require_explicit_selection(plugin):
    async def run():
        request = event(messages=[Image.fromURL("https://example.test/a.png"), Reply(id="88", chain=[Image.fromURL("https://example.test/b.png")])])
        assert "明确选择" in await plugin.handle_run_workflow(request, "改图", "水彩")
    asyncio.run(with_jobs(run(), plugin))
    assert not plugin._client.uploads


def test_multimage_roles_and_same_image_in_two_slots(plugin):
    plugin._workflows[0].input_nodes.append(InputNodeSection(node_id="5", field_name="image", value_type="image", label="风格参考"))
    async def run():
        request = event(images=1)
        context = json.loads(await plugin.handle_workflow_context(request))
        ref = context["images"][0]["ref"]
        assert "明确绑定角色" in await plugin.handle_run_workflow(request, "改图", "水彩", image_refs=[ref])
        reply = await plugin.handle_run_workflow(request, "改图", "水彩", image_bindings={"人物参考": ref, "风格参考": ref})
        assert "queued" in reply
    asyncio.run(with_jobs(run(), plugin))
    assert len(plugin._client.uploads) == 1
    assert values(plugin)["2/image"] == values(plugin)["5/image"]


@pytest.mark.parametrize("params,reason", [({"3/width": 99999}, "不能大于"), ({"model": "other"}, "不存在"), ({"width": "NaN"}, "有限数字"), ({"width": 512.5}, "整数")])
def test_bad_parameters_cannot_upload_or_submit(plugin, params, reason):
    async def run():
        assert reason in await plugin.handle_run_workflow(event(images=1), "改图", "水彩", parameters=params)
    asyncio.run(with_jobs(run(), plugin))
    assert not plugin._client.uploads
    assert not plugin._client.submissions


def test_missing_image_returns_waiting_and_cancel_works(plugin):
    async def run():
        request = event("改成水彩")
        reply = await plugin.handle_run_workflow(request, "改图", "水彩")
        assert "尚未提交" in reply
        await plugin.handle_input_collector(event("跳过剩余"))
        assert not plugin._client.submissions
        assert plugin._input_sessions
        await plugin.handle_input_collector(event(images=1, mid="second"))
        await finish_jobs(plugin)
        assert len(plugin._client.submissions) == 1
        assert not plugin._input_sessions
    asyncio.run(with_jobs(run(), plugin))


def test_cancel_during_prompt_input_and_at_quota(plugin):
    async def run():
        await plugin.handle_run_workflow(event(images=1), "改图", "")
        assert next(iter(plugin._input_sessions.values())).phase == "text"
        plugin.config.access.max_per_user_per_hour = 1
        await (await plugin._load_task_journal()).update("quota-task", status="success", user_id="10001", submitted_at=time.time(), delivery_status="sent")
        cancel = event("/wf中断")
        await plugin.handle_input_collector(cancel)
        assert not plugin._is_consumed(cancel)
        await plugin.handle_rh_cancel(cancel)
        assert not plugin._input_sessions
    asyncio.run(with_jobs(run(), plugin))
    assert not plugin._client.submissions


def test_followup_inherits_original_input_and_parameters(plugin):
    async def run():
        first = json.loads(await plugin.handle_run_workflow(event(images=1), "改图", "水彩", parameters={"width": 1024}))
        await finish_jobs(plugin)
        reply = await plugin.handle_run_workflow(event(mid="2"), "", "换成夜景", reuse_task_id=first["task_id"])
        assert json.loads(reply)["task_id"] != first["task_id"]
    asyncio.run(with_jobs(run(), plugin))
    assert values(plugin)["3/width"] == "1024"
    assert "水彩" in values(plugin)["1/prompt"] and "夜景" in values(plugin)["1/prompt"]
    assert len(plugin._client.uploads) == 2
    assert len(plugin._client.downloads) == 1  # second use reads the local cache


def test_generated_second_image_can_be_edited_after_reload(plugin):
    async def run():
        original = event(images=1)
        first = json.loads(await plugin.handle_run_workflow(original, "改图", "水彩"))
        await finish_jobs(plugin)
        context = plugin._event_ctx(original)
        plugin._remember_generated_image("task-1", 2, "https://example.test/generated2.png", original.unified_msg_origin, context)
        plugin._remember_generated_image("task-1", 2, "https://example.test/generated2.png", original.unified_msg_origin, context, base64.b64encode(PNG).decode())
        plugin._media_store = None
        request = event("把第二张改成夜景", mid="3")
        available = json.loads(await plugin.handle_workflow_context(request))
        generated = [r for r in available["images"] if r["origin"] == "generated"]
        assert len(generated) == 1 and generated[0]["position"] == 2
        assert "queued" in await plugin.handle_run_workflow(request, "改图", "改成夜景", image_refs=[generated[0]["ref"]], reuse_task_id=first["task_id"])
    asyncio.run(with_jobs(run(), plugin))
    assert len(plugin._client.downloads) == 1  # generated result already cached


def test_disabled_workflow_not_exposed_or_executed(plugin):
    plugin._workflows[0].llm_enabled = False
    async def run():
        request = event(images=1)
        assert not json.loads(await plugin.handle_workflow_context(request))["workflows"]
        assert "未开启" in await plugin.handle_run_workflow(request, "改图", "水彩")
    asyncio.run(with_jobs(run(), plugin))
    assert not plugin._client.uploads


def test_new_configuration_survives_dynamic_form_and_pages(plugin):
    raw = dump_config_dict(plugin.config)
    raw["workflows"][0]["llm_enabled"] = False
    recovered = build_config_model(raw)
    wf = recovered.workflows.items[0]
    assert wf.description == "人物换背景" and not wf.llm_enabled
    assert wf.input_nodes[1].required
    assert wf.input_nodes[2].maximum == 2048
    payload = plugin._page_config_payload()
    items, error = plugin._workflows_from_page_payload(payload["workflows"])
    assert not error and items[0].input_nodes[2].param_type == "integer"
    payload["workflows"][0]["nodes"][2]["minimum"] = 4096
    assert "约束无效" in plugin._workflows_from_page_payload(payload["workflows"])[1]


def test_integer_seed_does_not_lose_precision():
    node = InputNodeSection(node_id="1", param_type="integer")
    assert parameter_value(node, "9223372036854775807") == "9223372036854775807"


def test_media_store_restart_expiry_and_isolation(tmp_path, monkeypatch):
    store = MediaStore(tmp_path, ttl=60)
    first = store.remember("alice", "https://example.test/image", data=PNG)
    second = store.remember("alice", "https://example.test/image", message_id="another", data=PNG)
    assert first["ref"] != second["ref"]
    store = MediaStore(tmp_path, ttl=60)
    assert Path(store.get("alice", first["ref"])["source"]).read_bytes() == PNG
    with pytest.raises(ValueError):
        store.get("bob", first["ref"])
    monkeypatch.setattr("rh_generic_lib.media_store.time.time", lambda: first["created_at"] + 61)
    with pytest.raises(ValueError, match="过期"):
        store.get("alice", first["ref"])
    assert not list(tmp_path.glob("img_*.bin"))


def test_media_store_limits_history(tmp_path):
    store = MediaStore(tmp_path)
    for i in range(40):
        store.remember("alice", f"https://example.test/{i}")
    assert len(store.recent("alice")) == 32


def test_concurrent_submissions_reserve_user_quota(plugin):
    async def run():
        plugin.config.access.max_per_user_per_hour = 1
        started, release = asyncio.Event(), asyncio.Event()
        original = plugin._client.submit

        async def slow_submit(*args, **kwargs):
            started.set()
            await release.wait()
            return await original(*args, **kwargs)

        plugin._client.submit = slow_submit
        first = asyncio.create_task(plugin.handle_run_workflow(event(images=1), "改图", "水彩"))
        await started.wait()
        second = await plugin.handle_run_workflow(event(images=1, mid="2"), "改图", "油画")
        assert "上限" in second
        release.set()
        assert "queued" in await first
        await finish_jobs(plugin)
        assert plugin._task_journal.quota_used("10001", time.time()) == 1
    asyncio.run(with_jobs(run(), plugin))
    assert len(plugin._client.submissions) == 1


def test_unconfirmed_submission_is_not_retried_in_same_turn(plugin):
    attempts = []

    async def fail_submit(*args, **kwargs):
        attempts.append(args)
        raise TimeoutError("server response timed out")

    plugin._client.submit = fail_submit
    plugin.config.access.max_per_user_per_hour = 1

    async def run():
        request = event(images=1)
        first = await plugin.handle_run_workflow(request, "改图", "水彩")
        await finish_jobs(plugin)
        second = await plugin.handle_run_workflow(request, "改图", "水彩")
        assert json.loads(first)["task_id"] == json.loads(second)["task_id"]
        assert json.loads(second)["status"] == "unknown_submission"
        assert plugin._task_journal.quota_used("10001", time.time()) == 1
        assert plugin._limiter.active == 0
    asyncio.run(with_jobs(run(), plugin))
    assert len(attempts) == 1


def test_skip_and_cancel_during_image_upload_cannot_submit(plugin, monkeypatch):
    async def run():
        started, release = asyncio.Event(), asyncio.Event()
        original = plugin._client.upload_file

        async def slow_upload(*args, **kwargs):
            started.set()
            await release.wait()
            return await original(*args, **kwargs)

        plugin._client.upload_file = slow_upload

        async def fetch(source):
            return PNG

        monkeypatch.setattr(plugin, "_fetch_file_bytes", fetch)
        assert "尚未提交" in await plugin.handle_run_workflow(event(), "改图", "水彩")
        upload = asyncio.create_task(plugin.handle_input_collector(event(images=1, mid="2")))
        await started.wait()
        await plugin.handle_input_collector(event("跳过剩余", mid="3"))
        assert not plugin._client.submissions and plugin._input_sessions
        await plugin.handle_rh_cancel(event("/wf中断", mid="4"))
        assert not plugin._input_sessions
        release.set()
        await upload
        assert not plugin._client.submissions
    asyncio.run(with_jobs(run(), plugin))


def test_required_parameter_must_be_supplied_after_image_collection(plugin):
    node = plugin._workflows[0].input_nodes[2]
    node.required, node.field_value = True, ""

    async def run():
        result = await plugin.handle_run_workflow(event(images=1), "改图", "水彩")
        assert "尚未提交" in result
        await plugin.handle_input_collector(event("不变", mid="2"))
        assert not plugin._client.submissions
        await plugin.handle_input_collector(event("1e3", mid="3"))
        await finish_jobs(plugin)
        assert values(plugin)["3/width"] == "1000"
        assert not plugin._input_sessions
    asyncio.run(with_jobs(run(), plugin))


def test_optional_empty_parameter_keeps_cloud_default(plugin):
    plugin._workflows[0].input_nodes[2].field_value = ""

    async def run():
        assert "queued" in await plugin.handle_run_workflow(event(images=1), "改图", "水彩")
        await finish_jobs(plugin)
        assert "3/width" not in values(plugin)
    asyncio.run(with_jobs(run(), plugin))


def test_boolean_default_is_normalized_before_submission(plugin):
    plugin._workflows[0].input_nodes.append(InputNodeSection(
        node_id="5", field_name="enabled", value_type="text", param_type="boolean", field_value="1"))

    async def run():
        assert "queued" in await plugin.handle_run_workflow(event(images=1), "改图", "水彩")
        await finish_jobs(plugin)
        assert values(plugin)["5/enabled"] == "true"
    asyncio.run(with_jobs(run(), plugin))


@pytest.mark.parametrize("raw", ["0e-999999999", "0e999999999", "1e999999999"])
def test_excessive_numeric_exponents_are_rejected(raw):
    with pytest.raises(ValueError, match="过大或过小"):
        parameter_value(InputNodeSection(node_id="1", param_type="number"), raw)
