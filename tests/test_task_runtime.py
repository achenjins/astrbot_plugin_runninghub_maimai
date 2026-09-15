"""Paid task boundaries: persistence, queueing, recovery, cancellation and delivery."""
import asyncio
import base64
import json
import time

import pytest

import main as plugin_main
from rh_generic_lib.media_store import MediaStore
from rh_generic_lib.runninghub_client import RunningHubError, RunningHubTransportError
from .test_chat_workflows import PNG, event, plugin, finish_jobs  # noqa: F401
from .test_usability import messages


@pytest.fixture
def runtime(plugin, monkeypatch):
    plugin.config.feature.image_descriptions = False
    monkeypatch.setattr(plugin, "_poll_and_send", plugin_main.RunningHubGenericPlugin._poll_and_send.__get__(plugin))

    async def result(tid):
        return {"status": "SUCCESS", "results": [{"url": "https://example.test/output.png"}]}

    async def download(url):
        return base64.b64encode(PNG).decode()

    async def cancel(tid):
        return {"code": 0}

    plugin._client.wait_for_result = result
    plugin._client.download_base64 = download
    plugin._client.cancel = cancel
    return plugin


async def submit(star, mid="1", **kw):
    return json.loads(await star.handle_run_workflow(event(images=1, mid=mid), "", "水彩", **kw))


def test_queue_returns_while_upload_is_blocked_and_same_message_deduplicates(runtime):
    async def run():
        started, release = asyncio.Event(), asyncio.Event()
        original = runtime._client.upload_file

        async def upload(*args):
            started.set()
            await release.wait()
            return await original(*args)

        runtime._client.upload_file = upload
        first = await asyncio.wait_for(submit(runtime), 2)
        await started.wait()
        second = await submit(runtime, parameters={"width": 1024})
        assert first["task_id"] == second["task_id"]
        assert first["stop_after_execution"] and not runtime._client.submissions
        assert runtime._task_journal.path.exists()
        release.set()
        await finish_jobs(runtime)
        assert len(runtime._client.submissions) == 1
        assert runtime._task_journal.get(first["task_id"])["status"] == "success"
    asyncio.run(run())


def test_full_queue_and_cancel_before_upload_release(runtime):
    async def run():
        runtime.config.generation.max_concurrent = 1
        runtime.config.generation.max_queued = 1
        started, release = asyncio.Event(), asyncio.Event()
        original = runtime._client.upload_file

        async def upload(*args):
            started.set()
            await release.wait()
            return await original(*args)

        runtime._client.upload_file = upload
        first = await submit(runtime)
        await started.wait()
        second = await submit(runtime, "2")
        refused = await submit(runtime, "3")
        assert "队列已满" in refused["message"] and not runtime._client.submissions
        await runtime._cancel_task(second["task_id"], event().unified_msg_origin)
        await runtime._cancel_task(first["task_id"], event().unified_msg_origin)
        release.set()
        assert not runtime._client.submissions
        third = await submit(runtime, "4")
        await finish_jobs(runtime)
        assert runtime._task_journal.get(third["task_id"])["status"] == "success"
        assert runtime._limiter.active == 0
    asyncio.run(run())


def test_unknown_submission_keeps_remote_slot_and_quota_after_reload(runtime):
    async def run():
        runtime.config.generation.max_concurrent = 1
        runtime.config.generation.max_queued = 0
        attempts = []

        async def unknown(*args, **kwargs):
            attempts.append(1)
            raise RunningHubTransportError("lost response")

        runtime._client.submit = unknown
        first = await submit(runtime)
        await finish_jobs(runtime)
        runtime._task_journal = None
        await runtime._resume_pending_tasks()
        journal = runtime._task_journal
        assert journal.get(first["task_id"])["status"] == "unknown_submission"
        assert journal.quota_used("10001", time.time()) == 1
        assert "队列已满" in (await submit(runtime, "2"))["message"]
        assert (await submit(runtime))["task_id"] == first["task_id"]
        assert attempts == [1]
    asyncio.run(run())


def test_reservation_write_failure_cannot_submit(runtime, monkeypatch):
    async def run():
        journal = await runtime._load_task_journal()

        async def unavailable():
            raise OSError("disk full")

        monkeypatch.setattr(journal, "_write_locked", unavailable)
        reply = await submit(runtime)
        assert "disk full" in reply["message"]
        assert not runtime._pending and not runtime._client.submissions and not runtime._client.uploads
        assert not journal.records()
    asyncio.run(run())


@pytest.mark.parametrize("status", ["pending", "submitting"])
def test_restart_tracks_known_remote_but_never_repeats_submission(runtime, status):
    async def run():
        journal = await runtime._load_task_journal()
        ctx = runtime._event_ctx(event())
        await journal.update("rh-recovered", status=status, remote_task_id="remote-1" if status == "pending" else "",
                             workflow="改图", request={"context": ctx, "workflow": {}}, **ctx)
        runtime._task_journal = None
        await runtime._resume_pending_tasks()
        await finish_jobs(runtime)
        job = runtime._task_journal.get("rh-recovered")
        assert job["status"] == ("success" if status == "pending" else "unknown_submission")
        assert not runtime._client.submissions
    asyncio.run(run())


def test_cancel_reply_cannot_overwrite_concurrent_success(runtime):
    async def run():
        polling, finish = asyncio.Event(), asyncio.Event()
        cancel_started, cancel_release = asyncio.Event(), asyncio.Event()
        original = runtime._client.wait_for_result

        async def result(tid):
            polling.set()
            await finish.wait()
            return await original(tid)

        async def cancel(tid):
            cancel_started.set()
            await cancel_release.wait()
            return {"code": 0}

        runtime._client.wait_for_result = result
        runtime._client.cancel = cancel
        task = await submit(runtime)
        await polling.wait()
        cancelling = asyncio.create_task(runtime._cancel_task(task["task_id"], event().unified_msg_origin))
        await cancel_started.wait()
        finish.set()
        await finish_jobs(runtime)
        cancel_release.set()
        await cancelling
        job = runtime._task_journal.get(task["task_id"])
        assert job["status"] == "success" and job["outputs"][0]["sent"]
    asyncio.run(run())


def test_refused_cancel_retains_remote_slot_and_query(runtime):
    async def run():
        runtime.config.generation.max_concurrent = 1
        runtime.config.generation.max_queued = 0
        polling, finish = asyncio.Event(), asyncio.Event()
        original = runtime._client.wait_for_result

        async def result(tid):
            polling.set()
            await finish.wait()
            return await original(tid)

        async def cancel(tid):
            return {"code": 403, "msg": "denied"}

        runtime._client.wait_for_result = result
        runtime._client.cancel = cancel
        first = await submit(runtime)
        await polling.wait()
        await runtime._cancel_task(first["task_id"], event().unified_msg_origin)
        job = runtime._task_journal.get(first["task_id"])
        assert job["status"] == "needs_attention" and job["remote_task_id"] == "task-1"
        assert first["task_id"] in runtime._pending
        assert "队列已满" in (await submit(runtime, "2"))["message"]
        finish.set()
        await finish_jobs(runtime)
        assert runtime._task_journal.get(first["task_id"])["status"] == "success"
    asyncio.run(run())


def test_cancel_while_submitting_is_applied_after_remote_id_arrives(runtime):
    async def run():
        started, release = asyncio.Event(), asyncio.Event()
        original = runtime._client.submit
        cancelled = []

        async def slow(*args, **kwargs):
            started.set()
            await release.wait()
            return await original(*args, **kwargs)

        async def cancel(tid):
            cancelled.append(tid)
            return {"code": 0}

        runtime._client.submit = slow
        runtime._client.cancel = cancel
        first = await submit(runtime)
        await started.wait()
        await runtime._cancel_task(first["task_id"], event().unified_msg_origin)
        release.set()
        await finish_jobs(runtime)
        assert cancelled == ["task-1"]
        assert runtime._task_journal.get(first["task_id"])["status"] == "cancelled"
    asyncio.run(run())


def test_permanent_query_refusal_pauses_without_retrying_paid_post(runtime):
    async def run():
        original = runtime._client.wait_for_result

        async def refused(tid):
            raise RunningHubError("API key refused")

        runtime._client.wait_for_result = refused
        first = await submit(runtime)
        await finish_jobs(runtime)
        assert runtime._task_journal.get(first["task_id"])["status"] == "needs_attention"
        runtime._client.wait_for_result = original
        await runtime.handle_task_status(event("/wf状态 " + first["task_id"]))
        await finish_jobs(runtime)
        assert len(runtime._client.submissions) == 1
        assert runtime._task_journal.get(first["task_id"])["status"] == "success"
    asyncio.run(run())


def test_duplicate_delivery_does_not_resend_images_or_fallback(runtime):
    async def run():
        first = await submit(runtime)
        await finish_jobs(runtime)
        sent = len(runtime.context.sent)
        job = runtime._task_journal.get(first["task_id"])
        await runtime._deliver_job(job, runtime._client)
        assert len(runtime.context.sent) == sent
        record = runtime._get_media_store().get_delivery(MediaStore.owner(job), first["task_id"])
        await runtime._deliver_saved_results(record, plugin_main.DeliveryTarget.from_dict(job), runtime._client)
        assert len(runtime.context.sent) == sent
    asyncio.run(run())


def test_cancel_menu_is_scoped_expires_and_yields_to_input_collection(runtime, monkeypatch):
    async def run():
        journal = await runtime._load_task_journal()
        ctx = runtime._event_ctx(event())
        await journal.update("rh-old", status="queued", workflow="改图", request={"context": ctx}, **ctx)
        await runtime.handle_rh_cancel(event("/wf中断"))
        await runtime.handle_input_collector(event("1", group="elsewhere"))
        assert journal.get("rh-old")["status"] == "queued"
        key = MediaStore.owner(ctx)
        runtime._cancel_choices[key] = (time.time() - 121, ["rh-old"])
        await runtime.handle_input_collector(event("1"))
        assert journal.get("rh-old")["status"] == "queued" and key not in runtime._cancel_choices
        await runtime.handle_rh_cancel(event("/wf中断"))
        await runtime.handle_run_workflow(event(mid="new"), "改图", "水彩")
        await runtime.handle_input_collector(event("1", mid="input"))
        assert journal.get("rh-old")["status"] == "queued"
        await runtime.terminate()
    asyncio.run(run())


def test_admin_reconcile_releases_unknown_slot_without_regeneration(runtime):
    async def run():
        journal = await runtime._load_task_journal()
        ctx = runtime._event_ctx(event())
        await journal.update("rh-unknown", status="unknown_submission", workflow="改图", request={"context": ctx}, **ctx)
        await runtime.handle_reconcile_task(event("/wf核对 rh-unknown 未创建"))
        assert journal.get("rh-unknown")["status"] == "unknown_submission"
        runtime.config.access.admin_users = ["10001"]
        await runtime.handle_reconcile_task(event("/wf核对 rh-unknown 未创建"))
        assert journal.get("rh-unknown")["status"] == "cancelled"
        assert not runtime._client.submissions
    asyncio.run(run())


def test_declared_output_type_selects_unique_workflow_without_planner(runtime):
    async def run():
        image_wf = runtime._workflows[0]
        image_wf.output_type = "image"
        video_wf = image_wf.model_copy(deep=True)
        video_wf.name, video_wf.output_type, video_wf.workflow_id = "视频", "video", "43"
        runtime._workflows.append(video_wf)
        reply = json.loads(await runtime.handle_run_workflow(event(images=1), "", "水彩", output_type="video"))
        await finish_jobs(runtime)
        assert runtime._task_journal.get(reply["task_id"])["workflow"] == "视频"
        assert runtime._client.submissions[0][1]["workflow_id"] == "43"
    asyncio.run(run())


def test_sending_interrupted_by_restart_does_not_auto_resend(runtime):
    async def run():
        journal = await runtime._load_task_journal()
        ctx = runtime._event_ctx(event())
        await journal.update("rh-interrupted", status="success", delivery_status="sending", workflow="改图",
            outputs=[{"url": "https://example.test/output.png", "type": "image", "sent": False}],
            request={"context": ctx, "workflow": {}}, **ctx)
        runtime._task_journal = None
        await runtime._resume_pending_tasks()
        assert runtime._task_journal.get("rh-interrupted")["delivery_status"] == "uncertain"
        assert not runtime.context.sent and not runtime._pending and not runtime._client.submissions
    asyncio.run(run())


def test_missing_key_keeps_queue_reservation_until_configuration_is_restored(runtime):
    async def run():
        runtime._client.api_key = ""
        first = await submit(runtime)
        await finish_jobs(runtime)
        assert runtime._task_journal.get(first["task_id"])["status"] == "queued"
        assert not runtime._client.submissions
        runtime._client.api_key = "restored"
        await runtime.handle_task_status(event("/wf状态 " + first["task_id"]))
        await finish_jobs(runtime)
        assert runtime._task_journal.get(first["task_id"])["status"] == "success"
        assert len(runtime._client.submissions) == 1
    asyncio.run(run())
