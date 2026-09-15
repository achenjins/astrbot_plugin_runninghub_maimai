"""Completed media triggers one contextual reply without running tools again."""
import asyncio
import base64
import copy
import json
from types import SimpleNamespace

import pytest
from astrbot.api.message_components import Image

import main as plugin_main
from .test_chat_workflows import PNG, event, plugin, finish_jobs  # noqa: F401
from .test_usability import messages


def test_failure_event_replies_once_and_remains_in_original_context(bot):
    async def run():
        star = bot.plugin
        request = event()
        context = star._event_ctx(request)
        context["result_context"] = await star._capture_result_context(request.unified_msg_origin)
        journal = await star._load_task_journal()
        await journal.update("rh-failed", status="failed", workflow="改图", message="平台拒绝输入",
                             request={"context": context}, **star._event_ctx(request))
        job = journal.get("rh-failed")
        await star._notify_job(job)
        await star._notify_job(job)
        assert len(bot.calls) == 1
        assert "平台拒绝输入" in bot.calls[0]["system_prompt"] and bot.calls[0]["tools"] is None
        llm_request = SimpleNamespace(conversation=SimpleNamespace(cid="original"), system_prompt="原人格")
        await star._inject_workflow_results(request, llm_request)
        await star._inject_workflow_results(request, llm_request)
        assert llm_request.system_prompt.count('"task_id": "rh-failed"') == 1
        assert '"status": "failed"' in llm_request.system_prompt
    asyncio.run(run())


def test_image_memory_failure_cannot_turn_successful_send_into_retry(bot):
    async def broken_memory(*args, **kwargs):
        raise OSError("cache unavailable")

    async def run():
        bot.plugin._remember_image_usage = broken_memory
        record = await finish(bot)
        assert record["outputs"][0]["sent"]
        assert not record["outputs"][0]["error"]
        assert len(bot.calls) == 1
    asyncio.run(run())


class Conversations:
    def __init__(self):
        self.current = "original"
        self.records = {"original": [{"role": "user", "content": "画一张水彩图"}]}

    async def get_curr_conversation_id(self, stream):
        return self.current

    async def new_conversation(self, stream):
        self.current = "new"
        self.records["new"] = []
        return "new"

    async def get_conversation(self, stream, cid):
        if cid not in self.records:
            return None
        return SimpleNamespace(history=json.dumps(self.records[cid]), persona_id="甘雨")

    async def update_conversation(self, stream, cid, *, history):
        self.records[cid] = copy.deepcopy(history)


@pytest.fixture
def bot(plugin):
    plugin.config.feature.image_descriptions = False
    manager = Conversations()
    plugin.context.conversation_manager = manager
    plugin.context.get_config = lambda stream: {"provider_settings": {"default_personality": "default"}}
    calls, personas = [], []

    async def provider(stream):
        return "chat-provider"

    async def persona(**kwargs):
        personas.append(kwargs)
        return "甘雨", {"prompt": "你是甘雨，温柔简短地说话。"}, None, False

    async def generate(**kwargs):
        calls.append(kwargs)
        # A user chats while the background response is being generated.
        manager.records["original"].append({"role": "user", "content": "另一个问题"})
        return SimpleNamespace(completion_text="你的水彩图已经送到啦。")

    plugin.context.get_current_chat_provider_id = provider
    plugin.context.persona_manager = SimpleNamespace(resolve_selected_persona=persona)
    plugin.context.llm_generate = generate
    return SimpleNamespace(plugin=plugin, manager=manager, calls=calls, personas=personas)


async def finish(bot, *, task_id="result-1", urls=None):
    star = bot.plugin
    request = event()
    kwargs = star._event_ctx(request)
    kwargs["result_context"] = await star._capture_result_context(request.unified_msg_origin)

    async def result(tid):
        return {"status": "SUCCESS", "results": [{"url": u} for u in (urls or ["https://example.test/result.png"])]}

    async def download(url):
        return base64.b64encode(PNG).decode("ascii")

    star._client.wait_for_result = result
    star._client.download_base64 = download
    star._task_meta[task_id] = {"name": "改图", "region": "overseas"}
    await plugin_main.RunningHubGenericPlugin._poll_and_send(star, task_id, request.unified_msg_origin,
                                                           client=star._client, kwargs=kwargs)
    return next(r for r in star._get_media_store().deliveries.values() if r["task_id"] == task_id)


def test_media_then_one_persona_reply_preserves_new_messages_and_no_tools(bot):
    async def run():
        record = await finish(bot)
        star = bot.plugin
        assert any(isinstance(c, Image) for c in star.context.sent[0][1].chain)
        assert messages(star)[-1] == "你的水彩图已经送到啦。"
        assert len(star.context.sent) == 2
        assert len(bot.calls) == 1
        request = bot.calls[0]
        assert request["chat_provider_id"] == "chat-provider"
        assert request["tools"] is None
        assert "甘雨" in request["system_prompt"] and "已发送 1 项" in request["system_prompt"]
        assert bot.personas[0]["conversation_persona_id"] == "甘雨"
        assert "prompt" not in request  # no forged user message
        history = bot.manager.records["original"]
        assert any("任务结果 result-1" in m["content"] for m in history)
        assert any(m["content"] == "另一个问题" for m in history)
        assert history[-1]["content"] == messages(star)[-1]
        target = star._delivery_target(event())
        assert await star._notify_workflow_result(record, target, "重复回调")
        assert len(bot.calls) == 1 and len(star.context.sent) == 2
        assert not star._client.submissions
    asyncio.run(run())


def test_disabled_reply_still_records_completion(bot):
    bot.plugin.config.feature.result_notice = False
    asyncio.run(finish(bot))
    assert not bot.calls
    assert len(bot.plugin.context.sent) == 1
    assert "已发送 1 项" in bot.manager.records["original"][-1]["content"]


def test_disabled_reply_still_reports_delivery_failure(bot):
    bot.plugin.config.feature.result_notice = False
    original_send = bot.plugin.context.send_message

    async def fail_media(stream, chain):
        if any(isinstance(c, Image) for c in chain.chain):
            return False
        return await original_send(stream, chain)
    bot.plugin.context.send_message = fail_media
    asyncio.run(finish(bot))
    assert not bot.calls
    assert "/wf补发 result-1" in messages(bot.plugin)[-1]


def test_model_failure_keeps_result_history_and_falls_back_once(bot):
    attempts = []

    async def fail(**kwargs):
        attempts.append(kwargs)
        raise RuntimeError("provider unavailable")
    bot.plugin.context.llm_generate = fail
    asyncio.run(finish(bot))
    assert len(attempts) == 1
    assert len(bot.plugin.context.sent) == 2
    assert "生成完成，已发送 1 项" in messages(bot.plugin)[-1]
    assert "任务结果 result-1" in bot.manager.records["original"][-1]["content"]
    assert not bot.plugin._client.submissions


def test_partial_delivery_is_told_to_model_before_reply(bot):
    original_send = bot.plugin.context.send_message
    images = []

    async def fail_second(stream, chain):
        if any(isinstance(c, Image) for c in chain.chain):
            images.append(chain)
            if len(images) == 2:
                return False
        return await original_send(stream, chain)
    bot.plugin.context.send_message = fail_second
    asyncio.run(finish(bot, urls=["https://example.test/a.png", "https://example.test/b.png"]))
    assert len(bot.calls) == 1
    assert "1/2" in bot.calls[0]["system_prompt"]
    assert "/wf补发 result-1" in bot.calls[0]["system_prompt"]
    assert "第 2 项" in bot.manager.records["original"][-3]["content"]


@pytest.mark.parametrize("deleted", [False, True])
def test_switch_or_delete_does_not_notify_using_another_conversations_history(bot, deleted):
    async def run():
        star = bot.plugin
        target = star._delivery_target(event())
        context = await star._capture_result_context(target.stream_id)
        bot.manager.current = "other"
        bot.manager.records["other"] = [{"role": "user", "content": "不相关对话"}]
        if deleted:
            del bot.manager.records["original"]
        record = {"task_id": "result-1", "outputs": [], **context}
        await star._notify_workflow_result(record, target, "已发送")
        assert not bot.calls
        assert bot.manager.records["other"] == [{"role": "user", "content": "不相关对话"}]
        if deleted:
            assert "original" not in bot.manager.records
        else:
            assert "任务结果 result-1" in bot.manager.records["original"][-1]["content"]
    asyncio.run(run())


def test_collecting_inputs_keeps_original_conversation_for_completion(bot, monkeypatch):
    async def fetch(source):
        return PNG
    monkeypatch.setattr(bot.plugin, "_fetch_file_bytes", fetch)

    async def run():
        request = event()
        assert "尚未提交" in await bot.plugin.handle_run_workflow(request, "改图", "水彩")
        session = next(iter(bot.plugin._input_sessions.values()))
        assert session.execution_context["result_context"] == {"conversation_id": "original"}
        bot.manager.current = "other"
        bot.manager.records["other"] = []
        captured = []

        async def poll(task_id, stream_id, *, client, kwargs):
            captured.append(kwargs["result_context"])
        monkeypatch.setattr(bot.plugin, "_poll_and_send", poll)
        await bot.plugin.handle_input_collector(event(images=1, mid="2"))
        await asyncio.sleep(0)
        assert captured == [{"conversation_id": "original"}]
    asyncio.run(run())


def test_history_overwrite_and_plugin_reload_keep_result_facts_and_do_not_reply_twice(bot):
    async def run():
        record = copy.deepcopy(await finish(bot))
        record.pop("reply_attempted", None)  # old callback snapshot
        bot.manager.records["original"] = [{"role": "user", "content": "生成好了吗？"}]
        bot.plugin._media_store = None
        request = SimpleNamespace(conversation=SimpleNamespace(cid="original"), system_prompt="原人格")
        await bot.plugin.sync_workflow_results(event(), request)
        assert "原人格" in request.system_prompt and "result-1" in request.system_prompt
        assert '"sent": 1' in request.system_prompt
        await bot.plugin._notify_workflow_result(record, bot.plugin._delivery_target(event()), "已经发送")
        assert len(bot.calls) == 1
        assert "任务结果 result-1" in bot.manager.records["original"][-1]["content"]
        foreign = SimpleNamespace(conversation=SimpleNamespace(cid="original"), system_prompt="原人格")
        await bot.plugin.sync_workflow_results(event(user="other"), foreign)
        assert foreign.system_prompt == "原人格"
        other_conversation = SimpleNamespace(conversation=SimpleNamespace(cid="other"), system_prompt="原人格")
        await bot.plugin.sync_workflow_results(event(), other_conversation)
        assert other_conversation.system_prompt == "原人格"
    asyncio.run(run())
