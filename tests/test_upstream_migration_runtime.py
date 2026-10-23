"""Regression contracts for upstream task behavior through AstrBot's actual adapters."""
import asyncio
import json
import time

import pytest

from rh_generic_lib.delivery import DeliveryReceipt
from rh_generic_lib.media_store import MediaStore
from rh_generic_lib.runninghub_client import RunningHubBusyError, RunningHubError, RunningHubTransportError
from .test_chat_workflows import event, finish_jobs, plugin  # noqa: F401
from .test_task_runtime import runtime, submit  # noqa: F401


async def drain(star):
    """Include a worker explicitly rescheduled by the preceding worker's callback."""
    async with asyncio.timeout(5):
        while star._pending:
            await finish_jobs(star)
            await asyncio.sleep(0)


async def wait_until(predicate):
    async with asyncio.timeout(3):
        while not predicate():
            await asyncio.sleep(.01)


def test_same_message_distinct_requests_queue_but_canonical_retries_deduplicate(runtime):
    async def run():
        uploading, release = asyncio.Event(), asyncio.Event()
        original = runtime._client.upload_file
        async def upload(*args):
            uploading.set()
            await release.wait()
            return await original(*args)
        runtime._client.upload_file = upload
        async def request(prompt, width):
            return json.loads(await runtime.handle_run_workflow(event(images=1, mid="batch"), "改图", prompt,
                                                                parameters={"宽度": width}))
        first = await request("水彩", "768")
        await uploading.wait()
        duplicate = await request("水彩", "768.0")
        second = await request("油画", 768)
        third = await request("水彩", 1024)
        assert duplicate["task_id"] == first["task_id"]
        assert len({first["task_id"], second["task_id"], third["task_id"]}) == 3
        release.set()
        await drain(runtime)
        assert len(runtime._client.submissions) == 3
        assert len(runtime._task_journal.records()) == 3
        values = [{f"{node['nodeId']}/{node['fieldName']}": node["fieldValue"] for node in nodes}
                  for nodes, kwargs in runtime._client.submissions]
        assert {(item["1/prompt"], item["3/width"]) for item in values} == {
            ("水彩", "768"), ("油画", "768"), ("水彩", "1024")}
    asyncio.run(run())


def test_same_message_saved_alias_retry_reuses_original_generation(runtime):
    async def run():
        first = await submit(runtime, mid="alias")
        await drain(runtime)
        job = runtime._task_journal.get(first["task_id"])
        original_ref = job["request"]["context"]["image_bindings"]["2/image"]
        owner = MediaStore.owner(job)
        memory = await runtime._load_image_memory()
        saved = memory.find(owner, "images", original_ref)
        assert saved and saved["memory_id"].startswith("im-")
        retry = json.loads(await runtime.handle_run_workflow(event(images=1, mid="alias"), "改图", "水彩",
                                                            image_bindings={"人物参考": saved["memory_id"]}))
        await drain(runtime)
        assert retry["task_id"] == first["task_id"]
        assert len(runtime._client.submissions) == 1
        assert len(runtime._task_journal.records()) == 1
    asyncio.run(run())


def test_confirmed_busy_retries_identical_payload_under_one_reservation(runtime):
    async def run():
        runtime.config.generation.busy_retry_seconds = 1
        runtime.config.generation.busy_max_wait = 5
        attempts = []
        original = runtime._client.submit
        async def busy_once(nodes, **kwargs):
            attempts.append((nodes, kwargs))
            if len(attempts) == 1:
                raise RunningHubBusyError("TASK_QUEUE_MAXED")
            return await original(nodes, **kwargs)
        runtime._client.submit = busy_once
        first = await submit(runtime)
        await drain(runtime)
        job = runtime._task_journal.get(first["task_id"])
        assert job["status"] == "success"
        assert attempts[0] == attempts[1] and len(attempts) == 2
        assert len(runtime._client.submissions) == len(runtime._task_journal.records()) == 1
        assert runtime._task_journal.quota_used("10001", time.time()) == 1
    asyncio.run(run())


def test_continuous_busy_timeout_releases_nonexistent_remote_task_and_quota(runtime):
    async def run():
        runtime.config.generation.busy_retry_seconds = 1
        runtime.config.generation.busy_max_wait = 1
        attempts = []
        async def busy(*args, **kwargs):
            attempts.append(1)
            raise RunningHubBusyError("capacity")
        runtime._client.submit = busy
        first = await submit(runtime)
        await drain(runtime)
        job = runtime._task_journal.get(first["task_id"])
        assert job["status"] == "failed" and not job["remote_task_id"]
        assert job["submitted_at"] == 0
        assert runtime._task_journal.quota_used("10001", time.time()) == 0
        assert len(attempts) == 2 and not runtime._client.submissions
        assert runtime._limiter.active == 0
        assert not runtime._get_task_inputs().directory(first["task_id"]).exists()
    asyncio.run(run())


def test_busy_then_unknown_response_never_automatically_repeats_paid_submission(runtime):
    async def run():
        runtime.config.generation.busy_retry_seconds = 1
        runtime.config.generation.max_concurrent = 1
        runtime.config.generation.max_queued = 0
        attempts = []
        async def ambiguous(*args, **kwargs):
            attempts.append(1)
            if len(attempts) == 1:
                raise RunningHubBusyError("capacity")
            raise RunningHubTransportError("response lost after paid POST")
        runtime._client.submit = ambiguous
        first = await submit(runtime)
        await drain(runtime)
        await runtime._resume_pending_tasks()
        assert runtime._task_journal.get(first["task_id"])["status"] == "unknown_submission"
        assert not runtime._pending and len(attempts) == 2
        assert runtime._task_journal.quota_used("10001", time.time()) == 1
        assert "队列已满" in (await submit(runtime, mid="another"))["message"]
        assert len(attempts) == 2
    asyncio.run(run())


def test_cancel_while_platform_queue_waits_does_not_cancel_or_create_remote_task(runtime):
    async def run():
        runtime.config.generation.busy_retry_seconds = 1
        attempts, cancellations = [], []
        async def busy(*args, **kwargs):
            attempts.append(1)
            raise RunningHubBusyError("capacity")
        async def cancel(task_id):
            cancellations.append(task_id)
            return {"code": 0}
        runtime._client.submit, runtime._client.cancel = busy, cancel
        first = await submit(runtime)
        await wait_until(lambda: runtime._task_journal.get(first["task_id"])["stage"] == "platform_queue")
        await runtime._cancel_task(first["task_id"], event().unified_msg_origin)
        await drain(runtime)
        assert runtime._task_journal.get(first["task_id"])["status"] == "cancelled"
        assert attempts == [1] and cancellations == []
        assert runtime._task_journal.quota_used("10001", time.time()) == 0
        assert runtime._limiter.active == 0
    asyncio.run(run())


def test_temporary_query_failure_automatically_tracks_only_original_remote_id(runtime):
    async def run():
        runtime.config.generation.auto_resume_tracking = True
        runtime.config.generation.tracking_retry_seconds = 1
        queries = []
        original = runtime._client.wait_for_result
        async def transient(task_id):
            queries.append(task_id)
            if len(queries) == 1:
                raise RunningHubTransportError("temporary outage")
            return await original(task_id)
        runtime._client.wait_for_result = transient
        first = await submit(runtime)
        await drain(runtime)
        assert queries == ["task-1", "task-1"]
        assert len(runtime._client.submissions) == 1
        assert runtime._task_journal.get(first["task_id"])["status"] == "success"
    asyncio.run(run())


def test_permanent_query_refusal_requires_attention_even_when_auto_tracking_enabled(runtime):
    async def run():
        runtime.config.generation.auto_resume_tracking = True
        queries = []
        async def refused(task_id):
            queries.append(task_id)
            raise RunningHubError("invalid API key")
        runtime._client.wait_for_result = refused
        first = await submit(runtime)
        await drain(runtime)
        assert queries == ["task-1"] and len(runtime._client.submissions) == 1
        assert runtime._task_journal.get(first["task_id"])["status"] == "needs_attention"
    asyncio.run(run())


def test_explicit_resume_requested_as_old_worker_exits_is_not_lost(runtime):
    async def run():
        notifying, release = asyncio.Event(), asyncio.Event()
        queries = []
        original_query, original_notice = runtime._client.wait_for_result, runtime._notify_job
        async def query(task_id):
            queries.append(task_id)
            if len(queries) == 1:
                raise RunningHubError("key temporarily misconfigured")
            return await original_query(task_id)
        async def notify(job, message=""):
            if job["status"] == "needs_attention":
                notifying.set()
                await release.wait()
            return await original_notice(job, message)
        runtime._client.wait_for_result, runtime._notify_job = query, notify
        first = await submit(runtime)
        await asyncio.wait_for(notifying.wait(), 3)
        await runtime.handle_task_status(event("/wf状态 " + first["task_id"], mid="resume"))
        release.set()
        await drain(runtime)
        assert queries == ["task-1", "task-1"]
        assert len(runtime._client.submissions) == 1
        assert runtime._task_journal.get(first["task_id"])["status"] == "success"
    asyncio.run(run())


def test_admin_cannot_rebind_existing_remote_id_while_old_poll_is_active(runtime):
    async def run():
        polling, release = asyncio.Event(), asyncio.Event()
        original = runtime._client.wait_for_result
        async def query(task_id):
            polling.set()
            await release.wait()
            return await original(task_id)
        async def refused(task_id):
            return {"code": 403}
        runtime._client.wait_for_result, runtime._client.cancel = query, refused
        runtime.config.access.admin_users = ["10001"]
        first = await submit(runtime)
        await polling.wait()
        await runtime._cancel_task(first["task_id"], event().unified_msg_origin)
        await runtime.handle_reconcile_task(event(f"/wf核对 {first['task_id']} different-remote", mid="rebind"))
        job = runtime._task_journal.get(first["task_id"])
        assert job["status"] == "needs_attention" and job["remote_task_id"] == "task-1"
        release.set()
        await drain(runtime)
        assert runtime._task_journal.get(first["task_id"])["status"] == "success"
        assert runtime._task_journal.get(first["task_id"])["remote_task_id"] == "task-1"
        assert len(runtime._client.submissions) == 1
    asyncio.run(run())


def test_admin_abandon_stops_tracking_and_frees_slot_without_claiming_remote_cancel(runtime):
    async def run():
        polling, release = asyncio.Event(), asyncio.Event()
        runtime.config.generation.max_concurrent = 1
        runtime.config.generation.max_queued = 0
        runtime.config.access.admin_users = ["10001"]
        original = runtime._client.wait_for_result
        cancellations = []
        async def query(task_id):
            polling.set()
            await release.wait()
            return await original(task_id)
        async def refused(task_id):
            cancellations.append(task_id)
            return {"code": 403}
        runtime._client.wait_for_result, runtime._client.cancel = query, refused
        first = await submit(runtime)
        await polling.wait()
        await runtime._cancel_task(first["task_id"], event().unified_msg_origin)
        await runtime.handle_reconcile_task(event(f"/wf核对 {first['task_id']} 放弃", mid="abandon"))
        assert runtime._task_journal.get(first["task_id"])["status"] == "cancelled"
        assert first["task_id"] not in runtime._pending and runtime._limiter.active == 0
        assert cancellations == ["task-1"]  # Abandon itself does not call platform cancellation.
        assert runtime._task_journal.quota_used("10001", time.time()) == 1  # The paid task still counts.
        runtime._client.wait_for_result = original
        second = await submit(runtime, mid="after-abandon")
        await drain(runtime)
        assert runtime._task_journal.get(second["task_id"])["status"] == "success"
        assert len(runtime._client.submissions) == 2
    asyncio.run(run())


def test_natural_allowlist_optin_still_checks_actual_owner_group_and_quota(runtime):
    async def run():
        runtime.config.access.allow_users = ["someone-else"]
        runtime.config.access.allow_groups = ["20001"]
        runtime.config.access.max_per_user_per_hour = 1
        refused = await runtime.handle_run_workflow(event(images=1), "改图", "水彩")
        assert "没有使用" in refused and not runtime._client.submissions
        runtime.config.access.natural_language_ignore_user_allowlist = True
        first = await submit(runtime)
        await drain(runtime)
        job = runtime._task_journal.get(first["task_id"])
        assert job["user_id"] == "10001" and job["group_id"] == "20001"
        own = json.loads(await runtime._manage_workflow_task(event(mid="own"), "status", first["task_id"]))
        assert own["success"] and own["tasks"][0]["task_id"] == first["task_id"]
        foreign = json.loads(await runtime._manage_workflow_task(event(user="10002", mid="foreign"), "status", first["task_id"]))
        assert foreign["tasks"] == []
        wrong_group = await runtime.handle_run_workflow(event(images=1, group="other", mid="group"), "改图", "水彩")
        assert "群组没有" in wrong_group
        quota = await submit(runtime, mid="quota")
        assert "上限" in quota["message"] and len(runtime._client.submissions) == 1
    asyncio.run(run())


@pytest.mark.parametrize("failure,attempts", [("definite", 2), ("uncertain", 1), ("exception", 1)])
def test_delivery_retries_only_explicit_failures_without_another_generation(runtime, failure, attempts):
    async def run():
        runtime.config.generation.delivery_retries = 2
        calls = []
        async def send(target, data, **kwargs):
            calls.append(1)
            if len(calls) > 1:
                return DeliveryReceipt(True)
            if failure == "exception":
                raise TimeoutError("adapter response lost")
            return DeliveryReceipt(False, uncertain=failure == "uncertain")
        runtime.delivery.send_image_result = send
        first = await submit(runtime)
        await drain(runtime)
        job = runtime._task_journal.get(first["task_id"])
        assert len(calls) == attempts and len(runtime._client.submissions) == 1
        assert job["status"] == "success"
        assert job["outputs"][0]["sent"] is (failure == "definite")
        assert job["outputs"][0]["uncertain"] is (failure != "definite")
        assert job["delivery_status"] == ("sent" if failure == "definite" else "uncertain")
    asyncio.run(run())
