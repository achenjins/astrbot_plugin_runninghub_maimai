"""User-facing input guidance, configuration checks and result-only retries."""
from __future__ import annotations

import asyncio
import base64
import logging
import time
from types import SimpleNamespace

import pytest
from astrbot.api.message_components import Image

import main as plugin_main
from rh_generic_lib.config import InputNodeSection
from rh_generic_lib.delivery import Delivery, DeliveryTarget, OneBotChannel
from rh_generic_lib.validation import validate_node
from .test_chat_workflows import PNG, event, plugin  # noqa: F401
from .test_plugin import FakeContext


def messages(plugin):
    return ["".join(str(getattr(c, "text", "")) for c in chain.chain) for _, chain in plugin.context.sent]


def test_upload_guidance_tracks_roles_and_only_optional_inputs_can_be_skipped(plugin, monkeypatch):
    plugin._workflows[0].input_nodes.append(InputNodeSection(node_id="5", field_name="image", value_type="image", label="风格参考"))

    async def fetch(source):
        return PNG

    monkeypatch.setattr(plugin, "_fetch_file_bytes", fetch)

    async def run():
        response = await plugin.handle_run_workflow(event(), "改图", "水彩")
        assert "人物参考（图片，必填）" in response
        assert "风格参考（图片，可跳过）" in response
        assert "/wf中断" in response and "10 分钟" in response
        await plugin.handle_input_collector(event("跳过剩余"))
        assert not plugin._client.submissions
        plugin.context.sent.clear()
        await plugin.handle_input_collector(event(images=1, mid="2"))
        assert len(messages(plugin)) == 1
        assert "已准备：人物参考" in messages(plugin)[0]
        assert "风格参考（图片，可跳过）" in messages(plugin)[0]
        plugin.context.sent.clear()
        await plugin.handle_input_collector(event("跳过剩余", mid="3"))
        assert len(plugin._client.submissions) == 1
        assert len(messages(plugin)) == 1  # one acknowledgement of the accepted task
        assert not plugin._input_sessions
    asyncio.run(run())


def test_required_parameter_prompt_does_not_offer_an_empty_default(plugin):
    node = plugin._workflows[0].input_nodes[2]
    node.required, node.field_value = True, ""

    async def run():
        response = await plugin.handle_run_workflow(event(images=1), "改图", "水彩")
        assert "必填，尚未填写" in response and "至少 256" in response
        assert "全部保留" not in response
        assert not messages(plugin)  # caller receives the prompt, no duplicate chat notice
    asyncio.run(run())


@pytest.mark.parametrize("patch,reason", [
    ({"field_value": "99999"}, "不能大于"),
    ({"field_value": "512.5"}, "需要整数"),
    ({"choices": ["256", "1024"]}, "可选"),
    ({"choices": ["99999"]}, "不能大于"),
    ({"param_type": "string"}, "只有整数或数值"),
])
def test_pages_reject_bad_defaults_before_persisting(plugin, patch, reason, monkeypatch):
    payload = plugin._page_config_payload()
    payload["workflows"][0]["nodes"][2].update(patch)

    async def request():
        return payload

    monkeypatch.setattr(plugin, "_web_request_json", request)
    monkeypatch.setattr(plugin, "_web_jsonify", lambda data: data)
    reply = asyncio.run(plugin.handle_page_save_config())
    assert not reply["success"] and reason in reply["message"]
    assert "3/width" in reply["message"] and "改图" in reply["message"]
    assert plugin._workflows[0].input_nodes[2].field_value == "512"


def test_pages_reject_duplicate_nodes(plugin):
    payload = plugin._page_config_payload()["workflows"]
    payload[0]["nodes"].append(dict(payload[0]["nodes"][2]))
    _, message = plugin._workflows_from_page_payload(payload)
    assert "重复节点 3/width" in message


def test_fixed_required_value_must_be_configured_but_required_upload_can_be_empty():
    assert "必须填写默认值" in validate_node(InputNodeSection(node_id="1", required=True, value_type="default"))
    assert not validate_node(InputNodeSection(node_id="1", required=True, value_type="image"))


async def completed_task(plugin, request, urls, task_id="finished-1"):
    async def result(tid):
        return {"status": "SUCCESS", "results": [{"url": url} for url in urls]}

    async def download(url):
        plugin._client.downloads.append(url)
        return base64.b64encode(PNG).decode("ascii")

    plugin._client.wait_for_result = result
    plugin._client.download_base64 = download
    plugin._task_meta[task_id] = {"name": "改图", "region": "overseas"}
    await plugin._semaphore.acquire()
    await plugin_main.RunningHubGenericPlugin._poll_and_send(
        plugin, task_id, request.unified_msg_origin, client=plugin._client, kwargs=plugin._event_ctx(request))


def test_partial_delivery_retries_only_failed_image_from_cache_after_reload(plugin):
    image_attempts = []

    async def flaky_send(stream, chain):
        plugin.context.sent.append((stream, chain))
        if any(isinstance(c, Image) for c in chain.chain):
            image_attempts.append(chain)
            return len(image_attempts) != 2
        return True

    plugin.context.send_message = flaky_send

    async def run():
        request = event()
        await completed_task(plugin, request, ["https://example.test/a.png", "https://example.test/b.png"])
        assert any("已生成成功，本次已发送 1/2" in text for text in messages(plugin))
        assert any("第 2 项" in text and "/wf补发 finished-1" in text for text in messages(plugin))
        plugin._media_store = None
        plugin.config.access.max_per_user_per_hour = 1
        plugin._user_requests["10001"] = [time.time()]
        await plugin.handle_resend_result(event("/wf补发 finished-1", mid="2"))
        assert len(image_attempts) == 3
        assert len(plugin._client.downloads) == 2  # resend reads the second cached image
        record = plugin._get_media_store().recent_deliveries(plugin._get_media_store().owner(plugin._event_ctx(request)))[0]
        assert all(output["sent"] for output in record["outputs"])
        assert any("已补发 1 项，未重新生成" in text for text in messages(plugin))
        assert len(plugin._task_history) == 1
        assert plugin._semaphore._value == plugin.config.generation.max_concurrent
    asyncio.run(run())
    assert not plugin._client.submissions and not plugin._client.uploads


@pytest.mark.parametrize("foreign", [{"user": "other"}, {"group": "other"}])
def test_resend_cannot_access_another_users_or_sessions_results(plugin, foreign):
    async def run():
        await completed_task(plugin, event(), ["https://example.test/a.png"])
        plugin.context.sent.clear()
        await plugin.handle_resend_result(event("/wf补发 finished-1", **foreign))
        assert not any(isinstance(c, Image) for _, chain in plugin.context.sent for c in chain.chain)
        assert "没有可补发" in messages(plugin)[0]
    asyncio.run(run())


def test_result_selection_and_expired_records_do_not_generate(plugin, monkeypatch):
    async def run():
        await completed_task(plugin, event(), ["https://example.test/a.png", "https://example.test/b.png"])
        plugin.context.sent.clear()
        await plugin.handle_resend_result(event("/wf补发 最新 99"))
        assert "结果序号" in messages(plugin)[0]
        plugin.context.sent.clear()
        await plugin.handle_resend_result(event("/wf补发 最新 2,2"))
        assert sum(isinstance(c, Image) for _, chain in plugin.context.sent for c in chain.chain) == 1
        record = next(iter(plugin._get_media_store().deliveries.values()))
        monkeypatch.setattr("rh_generic_lib.media_store.time.time", lambda: record["created_at"] + 3601)
        plugin.context.sent.clear()
        await plugin.handle_resend_result(event("/wf补发 finished-1"))
        assert "过期" in messages(plugin)[0]
    asyncio.run(run())
    assert not plugin._client.submissions


def test_success_without_message_id_is_not_sent_twice():
    ctx = FakeContext()
    calls = []

    class Bot:
        async def call_action(self, action, **kwargs):
            calls.append(action)
            return {"status": "ok", "retcode": 0, "data": {}}

    ctx.platform_inst = SimpleNamespace(get_client=lambda: Bot())
    delivery = Delivery(ctx, logging.getLogger("test"))
    target = DeliveryTarget(stream_id="fake:group_message:20001", group_id="20001", platform_id="fake")

    async def run():
        receipt = await delivery.send_image_result(target, "abcd", need_message_id=True)
        assert receipt.success and receipt.message_id == ""
        assert calls == ["send_group_msg"] and not ctx.sent
        assert await OneBotChannel(Bot(), logging.getLogger("test"))._send_with_status(DeliveryTarget(), []) == ("", False)
    asyncio.run(run())


def test_failed_generic_send_is_not_reported_as_success():
    ctx = FakeContext()

    async def fail(*args):
        return False

    ctx.send_message = fail
    receipt = asyncio.run(Delivery(ctx, logging.getLogger("test")).send_image_result(DeliveryTarget(stream_id="fake"), "abcd"))
    assert not receipt.success
