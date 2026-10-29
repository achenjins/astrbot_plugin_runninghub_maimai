"""Interrupted background work must not automatically repeat billed LLM calls."""
import asyncio
import json
from types import SimpleNamespace

import pytest

from rh_generic_lib.media_store import MediaStore
from .test_chat_workflows import JOB_TIMEOUT, PNG, event, plugin, finish_jobs  # noqa: F401


@pytest.mark.parametrize("interrupt_during_call", [True, False])
def test_recovery_does_not_repeat_prompt_enhancement(plugin, interrupt_during_call):
    async def run():
        plugin.config.feature.image_descriptions = False
        plugin._workflows[0].llm_enhance = True
        started = asyncio.Event()
        calls = []

        async def enhance(*args, **kwargs):
            calls.append(1)
            started.set()
            if interrupt_during_call and len(calls) == 1:
                await asyncio.Event().wait()
            return "扩写后的水彩提示词"

        plugin._enhance_text = enhance
        original_submit = plugin._submit_remote_job

        async def stop_before_post(*args):
            plugin._stopping = True
            return await original_submit(*args)

        if not interrupt_during_call:
            plugin._submit_remote_job = stop_before_post
        response = json.loads(await plugin.handle_run_workflow(event(images=1), "改图", "水彩"))
        task_id = response["task_id"]
        await started.wait()
        if interrupt_during_call:
            worker = plugin._pending[task_id]
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            await asyncio.sleep(0)
        else:
            await finish_jobs(plugin)
        assert not plugin._client.submissions
        assert plugin._task_journal.get(task_id)["status"] == "queued"
        plugin._stopping = False
        plugin._submit_remote_job = original_submit
        plugin._task_journal = None
        await plugin._resume_pending_tasks()
        await finish_jobs(plugin)
        assert len(calls) == 1
        assert len(plugin._client.submissions) == 1
        prompt = next(n["fieldValue"] for n in plugin._client.submissions[0][0]
                      if n["nodeId"] == "1" and n["fieldName"] == "prompt")
        assert prompt == ("水彩" if interrupt_during_call else "扩写后的水彩提示词")

    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


def test_cancelled_vision_call_does_not_charge_a_waiting_duplicate(plugin):
    async def run():
        started = asyncio.Event()
        calls = []

        async def generate(**kwargs):
            calls.append(1)
            if len(calls) == 1:
                started.set()
                await asyncio.Event().wait()
            return SimpleNamespace(completion_text="窗边白猫")

        plugin.config.feature.vision_model = "vision"
        plugin.context.llm_generate = generate
        owner = MediaStore.owner(plugin._event_ctx(event()))
        first = asyncio.create_task(plugin._describe_image(owner, PNG))
        await started.wait()
        second = asyncio.create_task(plugin._describe_image(owner, PNG))
        await asyncio.sleep(0)
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        with pytest.raises(ValueError, match="刚才未能"):
            await second
        assert calls == [1]

    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


def test_reload_does_not_repeat_interrupted_automatic_image_description(plugin):
    async def run():
        started = asyncio.Event()
        calls = []

        async def generate(**kwargs):
            calls.append(1)
            if len(calls) == 1:
                started.set()
                await asyncio.Event().wait()
            return SimpleNamespace(completion_text="窗边白猫")

        plugin.config.feature.vision_model = "vision"
        plugin.context.llm_generate = generate
        owner = MediaStore.owner(plugin._event_ctx(event()))
        first = asyncio.create_task(plugin._remember_image_usage(owner, "first-reference", PNG))
        await started.wait()
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        plugin._image_memory = None
        plugin._vision_cache.clear()
        saved = await plugin._remember_image_usage(owner, "second-reference", PNG)
        assert saved and saved["description"] == ""
        assert calls == [1]
        assert "second-reference" in saved["media_ids"]

    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))
