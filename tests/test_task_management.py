"""Task assets, scoped actions, journal resends and dashboard lifecycle boundaries."""
import asyncio
import json
import time

import pytest
from astrbot.api.message_components import Image

from rh_generic_lib.media_store import MediaStore
from rh_generic_lib.task_inputs import TaskInputs
from rh_generic_lib.task_journal import TaskJournal
from rh_generic_lib.runninghub_client import RunningHubTransportError
from .test_chat_workflows import PNG, event, plugin, finish_jobs  # noqa: F401
from .test_task_runtime import runtime, submit  # noqa: F401
from .test_usability import completed_task


def images(star):
    return [c for _, chain in star.context.sent for c in chain.chain if isinstance(c, Image)]


def test_task_inputs_validate_content_limits_and_scoped_cleanup(tmp_path):
    store = TaskInputs(tmp_path / "inputs")
    entry = store.put("rh-one", PNG, "../ref.png")
    assert entry["filename"] == "ref.png" and store.read("rh-one", entry) == PNG
    store.put("rh-two", PNG, "a.png")
    with pytest.raises(ValueError):
        store.put("../outside", PNG, "a.png")
    store.MAX_TOTAL_BYTES = len(PNG) * 2
    with pytest.raises(ValueError, match="缓存已满"):
        store.put("rh-three", PNG, "a.png")
    store.sweep({"rh-one"})
    assert store.directory("rh-one").exists() and not store.directory("rh-two").exists()
    (store.directory("rh-one") / (entry["sha256"] + ".bin")).write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="校验失败"):
        store.read("rh-one", entry)


def test_waiting_task_prefetches_before_slot_and_survives_media_expiry(runtime):
    async def run():
        runtime.config.generation.max_concurrent = 1
        started, release = asyncio.Event(), asyncio.Event()
        upload = runtime._client.upload_file

        async def block(*args):
            started.set()
            await release.wait()
            return await upload(*args)
        runtime._client.upload_file = block
        first = await submit(runtime)
        await started.wait()
        second = await submit(runtime, "2")
        for _ in range(100):
            second_job = runtime._task_journal.get(second["task_id"])
            if second_job["inputs_ready"]:
                break
            await asyncio.sleep(.01)
        assert second_job["inputs_ready"] and second_job["stage"] == "queued"
        assert len(runtime._client.downloads) == 2 and not runtime._client.submissions
        store = runtime._get_media_store()
        for entry in store.images.values():
            entry["created_at"] = time.time() - 7200
        store.prune()
        release.set()
        await finish_jobs(runtime)
        assert len(runtime._client.downloads) == 2 and len(runtime._client.submissions) == 2
        for result in (first, second):
            assert not runtime._get_task_inputs().directory(result["task_id"]).exists()
    asyncio.run(run())


def test_local_reference_is_pinned_before_worker_and_survives_restart(runtime, monkeypatch):
    async def run():
        source = runtime._prompt_library_path().parent / "local.png"
        source.write_bytes(PNG)
        owner = MediaStore.owner(runtime._event_ctx(event()))
        ref = runtime._get_media_store().remember(owner, str(source), message_id="local", position=0)["ref"]
        schedule = runtime._schedule_job
        monkeypatch.setattr(runtime, "_schedule_job", lambda job: None)
        response = json.loads(await runtime.handle_run_workflow(event(), "", "水彩", image_refs=[ref]))
        job = runtime._task_journal.get(response["task_id"])
        assert job["inputs"][ref]
        source.unlink()
        runtime._media_store = None
        runtime._task_journal = None
        runtime._task_inputs = None
        monkeypatch.setattr(runtime, "_schedule_job", schedule)
        await runtime._resume_pending_tasks()
        await finish_jobs(runtime)
        assert len(runtime._client.submissions) == 1 and not runtime._client.downloads
    asyncio.run(run())


def test_unknown_submission_retains_assets_until_explicit_reconciliation(runtime):
    async def run():
        async def uncertain(*args, **kwargs):
            raise RunningHubTransportError("lost")
        runtime._client.submit = uncertain
        response = await submit(runtime)
        await finish_jobs(runtime)
        tid = response["task_id"]
        assert runtime._task_journal.get(tid)["status"] == "unknown_submission"
        assert runtime._get_task_inputs().directory(tid).exists()
        result = json.loads(await runtime.handle_manage_workflow_task(event(mid="cancel"), "cancel", tid))
        assert not result["success"] and runtime._get_task_inputs().directory(tid).exists()
        runtime._is_admin = lambda user: True
        await runtime._reconcile_task_command(event(f"/wf核对 {tid} 未创建"))
        assert not runtime._get_task_inputs().directory(tid).exists()
    asyncio.run(run())


def test_natural_resend_only_second_and_deduplicates_reconstructed_event_after_reload(runtime):
    async def run():
        await completed_task(runtime, event(), ["https://example.test/a.png", "https://example.test/b.png"])
        runtime.context.sent.clear()
        response = json.loads(await runtime.handle_manage_workflow_task(event(mid="resend"), "resend", "latest", "2"))
        assert response["success"] and response["sent"] == 1 and len(images(runtime)) == 1
        assert len(runtime.context.sent) == 1  # the host LLM replies; no fixed bot summary
        runtime._task_journal = None
        duplicate = json.loads(await runtime.handle_manage_workflow_task(event(mid="resend"), "resend", "latest", "2"))
        assert duplicate["duplicate"] and len(images(runtime)) == 1
        await runtime.handle_manage_workflow_task(event(mid="new-message"), "resend", "latest", "2")
        assert len(images(runtime)) == 2 and not runtime._client.submissions
    asyncio.run(run())


@pytest.mark.parametrize("foreign", [{"user": "other"}, {"group": "other"}])
def test_natural_management_cannot_cross_owner(runtime, foreign):
    async def run():
        await completed_task(runtime, event(), ["https://example.test/a.png"])
        runtime.context.sent.clear()
        response = json.loads(await runtime.handle_manage_workflow_task(event(**foreign), "resend", "finished-1"))
        assert not response["success"] and not images(runtime)
        response = json.loads(await runtime.handle_manage_workflow_task(event(**foreign), "status"))
        assert not response["tasks"]
    asyncio.run(run())


def test_ambiguous_cancel_is_read_only_and_explicit_latest_cancels_without_notification(runtime, monkeypatch):
    async def run():
        monkeypatch.setattr(runtime, "_schedule_job", lambda job: None)
        one, two = await submit(runtime), await submit(runtime, "2")
        response = json.loads(await runtime.handle_manage_workflow_task(event(mid="cancel"), "cancel"))
        assert not response["success"] and len(response["tasks"]) == 2
        before = len(runtime.context.sent)
        response = json.loads(await runtime.handle_manage_workflow_task(event(mid="cancel-latest"), "cancel", "latest"))
        assert response["success"] and response["task_id"] == two["task_id"]
        assert len(runtime.context.sent) == before
        assert runtime._task_journal.get(one["task_id"])["status"] == "queued"
        assert not runtime._client.submissions
    asyncio.run(run())


def test_expired_result_url_reports_failure_and_keeps_durable_record(runtime, monkeypatch):
    async def run():
        await completed_task(runtime, event(), ["https://example.test/a.png"])
        store = runtime._get_media_store()
        store.images.clear()
        store.deliveries.clear()
        async def expired(url):
            raise ValueError("404")
        monkeypatch.setattr(runtime._client, "download_base64", expired)
        response = json.loads(await runtime.handle_manage_workflow_task(event(mid="resend"), "resend", "finished-1"))
        assert not response["success"] and "链接可能过期" in response["message"]
        assert runtime._task_journal.get("finished-1")["outputs"][0]["url"]
        assert not runtime._client.submissions
    asyncio.run(run())


def test_panel_preserves_active_tasks_quota_outputs_and_redacts_request(runtime, monkeypatch):
    async def run():
        monkeypatch.setattr(runtime, "_web_jsonify", lambda obj: obj)
        monkeypatch.setattr(runtime, "_schedule_job", lambda job: None)
        active = await submit(runtime)
        await completed_task(runtime, event(), ["https://example.test/a.png"])
        journal = await runtime._load_task_journal()
        await journal.update(active["task_id"], message="等待运行名额", request={"secret": "hidden-sentinel"})
        result = await runtime.handle_page_get_tasks()
        assert len(result["data"]["records"]) == 2  # legacy history must not double-count
        assert "hidden-sentinel" not in json.dumps(result)
        row = next(r for r in result["data"]["records"] if r["task_id"] == active["task_id"])
        assert row["queue_position"] == 1 and row["actions"] == ["cancel", "resume"]
        quota = journal.quota_used("10001", time.time())
        assert (await runtime.handle_page_clear_tasks())["success"]
        assert len((await runtime.handle_page_get_tasks())["data"]["records"]) == 1
        assert journal.quota_used("10001", time.time()) == quota
        assert journal.get("finished-1")["outputs"] and journal.get("finished-1")["hidden"]
    asyncio.run(run())


def test_panel_action_uses_original_destination_and_records_outcome(runtime, monkeypatch):
    async def run():
        await completed_task(runtime, event(), ["https://example.test/a.png"])
        runtime.context.sent.clear()
        monkeypatch.setattr(runtime, "_web_jsonify", lambda obj: obj)
        payload = {"task_id": "finished-1", "action": "resend", "request_id": "page-request-1", "stream_id": "attacker"}
        async def body():
            return payload
        monkeypatch.setattr(runtime, "_web_request_json", body)
        assert (await runtime.handle_page_task_action())["success"]
        await asyncio.gather(*runtime._page_action_workers)
        assert len(images(runtime)) == 1 and runtime.context.sent[0][0] == event().unified_msg_origin
        row = next(r for r in (await runtime.handle_page_get_tasks())["data"]["records"] if r["task_id"] == "finished-1")
        assert "已补发 1 项" in row["action_message"] and not row["busy"]
        assert (await runtime.handle_page_task_action())["success"]
        await asyncio.gather(*runtime._page_action_workers)
        assert len(images(runtime)) == 1
        payload["action"] = "generate"
        assert not (await runtime.handle_page_task_action())["success"]
    asyncio.run(run())


def test_durable_duration_does_not_grow_on_resend(tmp_path):
    async def run():
        journal = TaskJournal(tmp_path / "journal.json")
        await journal.load()
        await journal.update("rh-time", status="queued", started_at=time.time() - 10)
        await journal.mark_success("rh-time", 1, [])
        finished = journal.get("rh-time")["finished_at"]
        await journal.update("rh-time", delivery_status="sent")
        assert journal.get("rh-time")["finished_at"] == finished
    asyncio.run(run())


def test_natural_pending_input_cancel_does_not_cancel_older_job_on_repeat(runtime, monkeypatch):
    async def run():
        monkeypatch.setattr(runtime, "_schedule_job", lambda job: None)
        old = await submit(runtime)
        await runtime.handle_run_workflow(event(mid="pending-input"), "", "画一张水彩")
        status = json.loads(await runtime.handle_manage_workflow_task(event(mid="status")))
        assert status["pending_input"]
        result = json.loads(await runtime.handle_manage_workflow_task(event(mid="cancel-input"), "cancel", "pending_input"))
        assert result["success"] and not runtime._input_sessions
        await runtime.handle_manage_workflow_task(event(mid="cancel-input"), "cancel", "pending_input")
        assert runtime._task_journal.get(old["task_id"])["status"] == "queued"
    asyncio.run(run())


def test_natural_status_and_panel_refresh_never_resume_paused_job(runtime, monkeypatch):
    async def run():
        journal = await runtime._load_task_journal()
        await journal.update("rh-paused", **runtime._event_ctx(event()), status="tracking_paused", remote_task_id="remote-1")
        monkeypatch.setattr(runtime, "_schedule_job", lambda job: pytest.fail("read-only status must not resume"))
        result = json.loads(await runtime.handle_manage_workflow_task(event(mid="status")))
        assert result["tasks"][0]["status"] == "tracking_paused"
        assert (await runtime._page_task_records())[0]["status"] == "tracking_paused"
    asyncio.run(run())


def test_explicit_resume_updates_stage_and_queries_existing_remote_id(runtime):
    async def run():
        journal = await runtime._load_task_journal()
        context = runtime._event_ctx(event())
        await journal.update("rh-paused", **context, status="tracking_paused", remote_task_id="remote-1",
                             request={"context": context})
        started, release = asyncio.Event(), asyncio.Event()
        async def wait(remote_id):
            assert remote_id == "remote-1"
            started.set()
            await release.wait()
            return {"status": "SUCCESS", "results": []}
        runtime._client.wait_for_result = wait
        result = json.loads(await runtime.handle_manage_workflow_task(event(mid="resume"), "resume", "rh-paused"))
        assert result["success"]
        await asyncio.wait_for(started.wait(), 2)
        row = (await runtime._page_task_records())[0]
        assert row["status"] == "pending" and row["stage_label"] == "等待平台结果"
        release.set()
        await finish_jobs(runtime)
        assert not runtime._client.submissions
    asyncio.run(run())
