"""Tool snapshots must distinguish input collection, submission and delivery."""
import asyncio
import json

import pytest

import main as plugin_main
from rh_generic_lib.media_store import MediaStore
from .test_chat_workflows import PNG, event, plugin, finish_jobs  # noqa: F401
from .test_usability import completed_task


def test_pending_input_and_auto_submission_refresh_original_tool_reply(plugin, monkeypatch):
    async def fetch(source):
        return PNG
    monkeypatch.setattr(plugin, "_fetch_file_bytes", fetch)

    async def run():
        original = event("改成水彩")
        assert "尚未提交" in await plugin.handle_run_workflow(original, "改图", "水彩")
        context = json.loads(await plugin.handle_workflow_context(event(mid="query")))
        pending = context["pending_input"]
        assert not pending["submitted"] and pending["phase"] == "files"
        assert pending["waiting_files"][0]["label"] == "人物参考"
        assert not json.loads(await plugin.handle_workflow_context(event(user="other")))["pending_input"]
        blocked = await plugin.handle_run_workflow(event(mid="retry"), "改图", "油画")
        assert "已有待补充" in blocked
        await plugin.handle_input_collector(event(images=1, mid="upload"))
        await finish_jobs(plugin)
        refreshed = await plugin.handle_run_workflow(original, "改图", "水彩")
        assert "rh-" in refreshed and "尚未提交" not in refreshed
        assert not json.loads(await plugin.handle_workflow_context(event()))["pending_input"]
        assert len(plugin._client.submissions) == 1
    asyncio.run(run())


def test_auto_submission_failure_replaces_stale_waiting_reply(plugin, monkeypatch):
    attempts = []

    async def fetch(source):
        return PNG

    async def uncertain_submit(*args, **kwargs):
        attempts.append(args)
        raise TimeoutError("no confirmation")
    monkeypatch.setattr(plugin, "_fetch_file_bytes", fetch)
    plugin._client.submit = uncertain_submit

    async def run():
        original = event("水彩")
        await plugin.handle_run_workflow(original, "改图", "水彩")
        await plugin.handle_input_collector(event(images=1, mid="upload"))
        await finish_jobs(plugin)
        reply = await plugin.handle_run_workflow(original, "改图", "水彩")
        assert json.loads(reply)["status"] == "unknown_submission"
        assert "尚未提交任务" not in reply
        assert len(attempts) == 1
        assert not plugin._input_sessions
    asyncio.run(run())


def test_old_submission_without_live_poll_does_not_claim_running(plugin):
    async def run():
        request = event()
        store = plugin._get_media_store()
        store.remember_run(MediaStore.owner(plugin._event_ctx(request)), "old", workflow="改图", status="submitted")
        plugin._media_store = None  # reload persisted submission
        result = json.loads(await plugin.handle_workflow_context(request))["recent_runs"][0]
        assert result["status"] == "unknown" and not result["monitoring"]
    asyncio.run(run())


def test_completed_delivery_stays_known_after_poll_cleanup(plugin):
    async def run():
        request = event()
        store = plugin._get_media_store()
        store.remember_run(MediaStore.owner(plugin._event_ctx(request)), "finished-1", workflow="改图", status="submitted")
        await completed_task(plugin, request, ["https://example.test/result.png"])
        result = json.loads(await plugin.handle_workflow_context(request))["recent_runs"][0]
        assert result["status"] == "succeeded"
        assert result["delivery"] == {"total": 1, "sent": 1, "failed": 0, "complete": True}
        assert not result["monitoring"]
    asyncio.run(run())


@pytest.mark.parametrize("error,status", [
    (TimeoutError("timeout"), "tracking_paused"),
    (plugin_main.RunningHubError("refused"), "needs_attention"),
    (plugin_main.RunningHubError("bad input", task_status="FAILED"), "failed"),
    (plugin_main.RunningHubError("cancelled", task_status="CANCELLED"), "cancelled"),
])
def test_poll_errors_do_not_claim_running_or_success(plugin, error, status):
    async def fail(*args):
        raise error
    plugin._client.wait_for_result = fail

    async def run():
        request = event()
        store = plugin._get_media_store()
        store.remember_run(MediaStore.owner(plugin._event_ctx(request)), "failed", workflow="改图", status="submitted")
        await plugin_main.RunningHubGenericPlugin._poll_and_send(plugin, "failed", request.unified_msg_origin,
            client=plugin._client, kwargs=plugin._event_ctx(request))
        result = json.loads(await plugin.handle_workflow_context(request))["recent_runs"][0]
        assert result["status"] == status
        assert not result["monitoring"]
    asyncio.run(run())


def test_submitting_task_is_visible_and_cannot_be_submitted_again(plugin):
    async def run():
        started, release = asyncio.Event(), asyncio.Event()
        original_submit = plugin._client.submit

        async def slow_submit(*args, **kwargs):
            started.set()
            await release.wait()
            return await original_submit(*args, **kwargs)
        plugin._client.submit = slow_submit
        first = asyncio.create_task(plugin.handle_run_workflow(event(images=1), "改图", "水彩"))
        await started.wait()
        context = json.loads(await plugin.handle_workflow_context(event(mid="query")))
        assert context["recent_runs"][0]["status"] == "submitting"
        duplicate = json.loads(await plugin.handle_run_workflow(event(images=1), "改图", "油画"))
        assert duplicate["status"] == "submitting"
        release.set()
        await first
        await finish_jobs(plugin)
        assert len(plugin._client.submissions) == 1
    asyncio.run(run())


@pytest.mark.parametrize("status", ["FAILED", "CANCELLED"])
def test_client_preserves_explicit_remote_terminal_status(status):
    client = plugin_main.RunningHubClient(base_url="https://example.test", api_key="test", workflow_id="42")

    async def query(task_id):
        return {"status": status, "errorMessage": "test failure"}
    client.query = query
    with pytest.raises(plugin_main.RunningHubError) as raised:
        asyncio.run(client.wait_for_result("task"))
    assert raised.value.task_status == status
