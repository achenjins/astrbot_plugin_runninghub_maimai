"""Interactive file identities and shutdown boundaries through AstrBot entrypoints."""
import asyncio
import base64
import json

import pytest
from astrbot.api.message_components import File, Image
from astrbot.core.star.context import Context
from quart import Quart

import main as plugin_main
from .test_chat_workflows import JOB_TIMEOUT, PNG, Client, event, finish_jobs, plugin  # noqa: F401
from .test_task_runtime import runtime, submit  # noqa: F401
from rh_generic_lib.delivery import DeliveryReceipt
from .test_usability import completed_task


def configure_inputs(star, image_count=2):
    star.config.workflows.items[0].input_nodes = [
        plugin_main.InputNodeSection(node_id="1", field_name="prompt", value_type="prompt"),
        *[plugin_main.InputNodeSection(node_id=str(i + 2), field_name="image", value_type="image", required=True)
          for i in range(image_count)],
    ]
    star._refresh_workflows()


@pytest.mark.parametrize("same_message", [False, True])
def test_identical_images_can_fill_distinct_slots(plugin, same_message):
    async def run():
        configure_inputs(plugin)
        context = plugin._event_ctx(event())
        await plugin._start_workflow("改图", "cat", **context)
        if same_message:
            await plugin.handle_input_collector(event(mid="both", messages=[
                Image.fromURL("https://example.test/same.png"), Image.fromURL("https://example.test/same.png")]))
        else:
            await plugin.handle_input_collector(event(images=1, mid="first"))
            await plugin.handle_input_collector(event(images=1, mid="second"))
        await finish_jobs(plugin)
        assert len(plugin._client.uploads) == 2 and len(plugin._client.submissions) == 1
        assert not plugin._input_sessions
    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


@pytest.mark.parametrize("concurrent", [False, True])
def test_repeated_message_position_fills_only_one_slot(plugin, concurrent):
    async def run():
        configure_inputs(plugin)
        context = plugin._event_ctx(event())
        await plugin._start_workflow("改图", "cat", **context)
        requests = [plugin.handle_input_collector(event(images=1, mid="one")) for _ in range(2)]
        if concurrent:
            await asyncio.gather(*requests)
        else:
            for request in requests:
                await request
        session = plugin._find_input_session(context["user_id"], context["stream_id"])
        assert session and len(session.waiting_nodes) == len(plugin._client.uploads) == 1
        await plugin.handle_input_collector(event(images=1, mid="two"))
        await finish_jobs(plugin)
        assert len(plugin._client.uploads) == 2 and len(plugin._client.submissions) == 1
    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


def test_file_components_with_same_name_keep_their_own_file_ids(plugin):
    async def run():
        configure_inputs(plugin)
        await plugin._start_workflow("改图", "cat", **plugin._event_ctx(event()))
        request = event(mid="both-files", messages=[
            File("same.png", url="https://example.test/same.png"),
            File("same.png", url="https://example.test/same.png")])
        request.message_obj.raw_message = {"message": [
            {"type": "file", "data": {"name": "same.png", "file_id": file_id}}
            for file_id in ("file-one", "file-two")]}
        await plugin.handle_input_collector(request)
        await finish_jobs(plugin)
        assert len(plugin._client.uploads) == 2 and len(plugin._client.submissions) == 1
    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


def test_failed_upload_can_retry_same_message(plugin):
    async def run():
        configure_inputs(plugin, 1)
        await plugin._start_workflow("改图", "cat", **plugin._event_ctx(event()))
        original = plugin._client.upload_file
        attempts = []

        async def upload(*args):
            attempts.append(True)
            if len(attempts) == 1:
                raise OSError("temporary upload failure")
            return await original(*args)

        plugin._client.upload_file = upload
        await plugin.handle_input_collector(event(images=1, mid="retry"))
        await plugin.handle_input_collector(event(images=1, mid="retry"))
        await finish_jobs(plugin)
        assert len(attempts) == 2 and len(plugin._client.submissions) == 1
    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


@pytest.mark.parametrize("notice_first", [False, True])
def test_group_file_notice_and_message_fill_only_one_slot(plugin, monkeypatch, notice_first):
    async def run():
        configure_inputs(plugin)
        context = plugin._event_ctx(event())
        await plugin._start_workflow("改图", "cat", **context)

        async def group_bytes(*args, **kwargs):
            return PNG

        monkeypatch.setattr(plugin, "_fetch_napcat_file_bytes", group_bytes)
        normal = event(mid="file-message", messages=[File("cat.png", url="https://example.test/cat.png")])
        normal.message_obj.raw_message = {"message": [{"type": "file", "data": {"name": "cat.png", "file_id": "file-one"}}]}
        notice = event(mid="file-notice")
        notice.message_obj.raw_message = {"post_type": "notice", "notice_type": "group_upload", "group_id": 20001,
                                         "user_id": 10001, "file": {"name": "cat.png", "id": "file-one"}}
        calls = [(plugin.handle_notice_collector, notice), (plugin.handle_input_collector, normal)]
        for handler, request in calls if notice_first else reversed(calls):
            await handler(request)
        session = plugin._find_input_session(context["user_id"], context["stream_id"])
        assert session and len(session.waiting_nodes) == 1 and len(plugin._client.uploads) == 1
        # A separately uploaded file may contain exactly the same bytes.
        second = event(mid="second-file", messages=[File("cat.png", url="https://example.test/cat.png")])
        second.message_obj.raw_message = {"message": [{"type": "file", "data": {"name": "cat.png", "file_id": "file-two"}}]}
        await plugin.handle_input_collector(second)
        await finish_jobs(plugin)
        assert len(plugin._client.uploads) == 2 and len(plugin._client.submissions) == 1
    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


def test_disabled_plugin_api_cannot_rebuild_clients_or_create_jobs(plugin, monkeypatch):
    async def run():
        configure_inputs(plugin, 0)
        plugin.config.access.allow_plugin_api = True
        plugin.context.registered_web_apis = []
        Context.register_web_api(plugin.context, "/runninghub_workflow_adapter/run_workflow_api",
                                 plugin.handle_run_workflow_api, ["POST"], "run")
        retained_handler = plugin.context.registered_web_apis[0][1]
        await plugin.terminate()
        assert not plugin.context.registered_web_apis
        rebuilt = []

        def rebuild():
            rebuilt.append(True)
            plugin._client = Client()

        monkeypatch.setattr(plugin, "_rebuild_client", rebuild)
        app = Quart("disabled-plugin")
        context = plugin._event_ctx(event())
        async with app.test_request_context("/api/plug/runninghub_workflow_adapter/run_workflow_api", method="POST",
                                           json={"workflow_name": "改图", "prompt": "cat", **context}):
            response = await retained_handler()
            result = await response.get_json()
        await finish_jobs(plugin)
        assert not result["success"] and not rebuilt and not plugin._pending
        assert plugin._task_journal is None or not plugin._task_journal.records()
    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


def test_terminating_old_instance_preserves_replacement_and_foreign_api_handlers(plugin):
    async def run():
        context = plugin.context
        context.registered_web_apis = []
        Context.register_web_api(context, "/shared", plugin.handle_run_workflow_api, ["POST"], "old")
        Context.register_web_api(context, "/old-only", plugin.handle_page_get_config, ["GET"], "old")
        replacement = plugin_main.RunningHubGenericPlugin(context, None)
        Context.register_web_api(context, "/shared", replacement.handle_run_workflow_api, ["POST"], "new")

        async def foreign():
            return "foreign"

        Context.register_web_api(context, "/foreign", foreign, ["GET"], "foreign")
        await plugin.terminate()
        assert context.registered_web_apis == [
            ("/shared", replacement.handle_run_workflow_api, ["POST"], "new"),
            ("/foreign", foreign, ["GET"], "foreign")]
        assert replacement._runtime_running()
    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


@pytest.mark.parametrize("image_count", [0, 1])
def test_stop_during_start_cannot_create_jobs_or_input_sessions(plugin, monkeypatch, image_count):
    async def run():
        configure_inputs(plugin, image_count)
        captured, release = asyncio.Event(), asyncio.Event()

        async def capture(stream_id):
            captured.set()
            await release.wait()
            return {}

        monkeypatch.setattr(plugin, "_capture_result_context", capture)
        request = asyncio.create_task(plugin._start_workflow("改图", "cat", **plugin._event_ctx(event())))
        await captured.wait()
        await plugin.terminate()
        release.set()
        result = await request
        await finish_jobs(plugin)
        assert not result["success"] and not plugin._pending and not plugin._input_sessions
        assert plugin._task_journal is None or not plugin._task_journal.records()
    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


@pytest.mark.parametrize("phase", ["identity", "inputs", "journal"])
def test_stop_during_reservation_cannot_leave_recoverable_new_job(plugin, monkeypatch, phase):
    async def run():
        configure_inputs(plugin, 0)
        captured, release = asyncio.Event(), asyncio.Event()
        journal = await plugin._load_task_journal()
        target = journal if phase == "journal" else plugin
        name = {"identity": "_generation_identity", "inputs": "_preserve_job_inputs", "journal": "update"}[phase]
        original = getattr(target, name)

        async def block(*args, **kwargs):
            if phase != "journal" or kwargs.get("status") == "queued":
                captured.set()
                await release.wait()
            return await original(*args, **kwargs)

        monkeypatch.setattr(target, name, block)
        client = plugin._client
        request = asyncio.create_task(plugin._start_workflow("改图", "cat", **plugin._event_ctx(event())))
        await captured.wait()
        stopping = asyncio.create_task(plugin.terminate())
        await asyncio.sleep(0)
        release.set()
        await stopping
        result = await request
        assert not result["success"] and not client.submissions and not plugin._pending
        assert all(job["status"] == "cancelled" for job in journal.records())
        assert not journal.recoverable_records()
        for job in journal.records():
            assert not plugin._get_task_inputs().directory(job["task_id"]).exists()
    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


def test_terminate_waits_for_written_reservation_to_become_nonrecoverable(plugin, monkeypatch):
    async def run():
        configure_inputs(plugin, 0)
        captured, release = asyncio.Event(), asyncio.Event()
        journal = await plugin._load_task_journal()
        original = journal.update

        async def update(*args, **kwargs):
            result = await original(*args, **kwargs)
            if kwargs.get("status") == "queued":
                captured.set()
                await release.wait()
            return result

        monkeypatch.setattr(journal, "update", update)
        client = plugin._client
        request = asyncio.create_task(plugin._start_workflow("改图", "cat", **plugin._event_ctx(event())))
        await captured.wait()
        stopping = asyncio.create_task(plugin.terminate())
        await asyncio.sleep(0)
        assert not stopping.done()
        release.set()
        await stopping
        # A new plugin instance may load the journal as soon as terminate returns.
        from rh_generic_lib.task_journal import TaskJournal
        reloaded = TaskJournal(journal.path)
        await reloaded.load()
        assert not reloaded.recoverable_records() and not client.submissions
        result = await request
        assert not result["success"]
    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


def test_cancelled_request_cleans_written_reservation_before_releasing_lock(plugin, monkeypatch):
    async def run():
        configure_inputs(plugin, 0)
        captured = asyncio.Event()
        journal = await plugin._load_task_journal()
        original = journal.update

        async def update(*args, **kwargs):
            result = await original(*args, **kwargs)
            if kwargs.get("status") == "queued":
                captured.set()
                await asyncio.Event().wait()
            return result

        monkeypatch.setattr(journal, "update", update)
        request = asyncio.create_task(plugin._start_workflow("改图", "cat", **plugin._event_ctx(event())))
        await captured.wait()
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request
        await plugin.terminate()
        assert not journal.recoverable_records() and not plugin._pending
        assert all(job["status"] == "cancelled" for job in journal.records())
    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


def test_stop_during_initialize_cannot_recreate_clients_or_routes(plugin, monkeypatch):
    async def run():
        captured, release = asyncio.Event(), asyncio.Event()
        rebuilt = []

        async def reload():
            captured.set()
            await release.wait()

        monkeypatch.setattr(plugin, "_reload_from_context_config", reload)
        monkeypatch.setattr(plugin, "_rebuild_client", lambda: rebuilt.append(True))
        request = asyncio.create_task(plugin.initialize())
        await captured.wait()
        await plugin.terminate()
        release.set()
        await request
        assert not rebuilt and not plugin.context.web_apis and plugin._cleanup_task is None
    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


@pytest.mark.parametrize("phase", ["prepare", "upload"])
def test_stop_during_file_collection_cannot_submit(plugin, monkeypatch, phase):
    async def run():
        configure_inputs(plugin, 1)
        await plugin._start_workflow("改图", "cat", **plugin._event_ctx(event()))
        captured, release = asyncio.Event(), asyncio.Event()
        client = plugin._client
        target = plugin if phase == "prepare" else client
        name = "_prepare_input_upload" if phase == "prepare" else "upload_file"
        original = getattr(target, name)

        async def block(*args, **kwargs):
            captured.set()
            await release.wait()
            return await original(*args, **kwargs)

        monkeypatch.setattr(target, name, block)
        request = asyncio.create_task(plugin.handle_input_collector(event(images=1, mid="collect")))
        await captured.wait()
        await plugin.terminate()
        release.set()
        await request
        assert not client.submissions and not plugin._input_sessions and not plugin._pending
        assert not client.uploads or phase == "upload"
    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


@pytest.mark.parametrize("phase", ["request", "journal"])
def test_stop_during_page_request_cannot_create_action_worker(plugin, monkeypatch, phase):
    async def run():
        journal = await plugin._load_task_journal()
        await journal.update("rh-page", status="queued", workflow="test")
        captured, release = asyncio.Event(), asyncio.Event()
        monkeypatch.setattr(plugin, "_web_jsonify", lambda value: value)

        async def payload():
            if phase == "request":
                captured.set()
                await release.wait()
            return {"task_id": "rh-page", "action": "resume", "request_id": "request-123"}

        async def load():
            captured.set()
            await release.wait()
            return journal

        monkeypatch.setattr(plugin, "_web_request_json", payload)
        if phase == "journal":
            monkeypatch.setattr(plugin, "_load_task_journal", load)
        request = asyncio.create_task(plugin.handle_page_task_action())
        await captured.wait()
        await plugin.terminate()
        release.set()
        result = await request
        assert not result["success"] and not plugin._page_action_workers and not plugin._pending
    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


def test_stop_during_resume_action_cannot_report_success_or_schedule(plugin, monkeypatch):
    async def run():
        journal = await plugin._load_task_journal()
        await journal.update("rh-resume", status="queued", workflow="test")
        captured, release = asyncio.Event(), asyncio.Event()

        async def resize(*args):
            captured.set()
            await release.wait()

        monkeypatch.setattr(plugin._limiter, "resize", resize)
        request = asyncio.create_task(plugin._perform_task_action("rh-resume", "resume"))
        await captured.wait()
        await plugin.terminate()
        release.set()
        result = await request
        assert not result["success"] and not plugin._pending and not plugin._task_meta
    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


def test_stopped_plugin_cannot_schedule_recovered_job(plugin):
    async def run():
        journal = await plugin._load_task_journal()
        await journal.update("rh-stopped", status="queued", request={"context": plugin._event_ctx(event())})
        await plugin.terminate()
        plugin._schedule_job(journal.get("rh-stopped"))
        assert not plugin._pending and not plugin._task_meta
    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


def test_stop_during_paid_post_preserves_uncertain_submission(runtime):
    async def run():
        started = asyncio.Event()

        async def paid_submit(*args, **kwargs):
            started.set()
            await asyncio.Event().wait()

        runtime._client.submit = paid_submit
        result = await submit(runtime, mid="stop-post")
        await started.wait()
        await runtime.terminate()
        job = runtime._task_journal.get(result["task_id"])
        assert job["status"] == "unknown_submission" and not job["remote_task_id"]
        assert runtime._get_task_inputs().directory(result["task_id"]).exists()
        assert not runtime._pending
    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


def test_account_request_cannot_rebuild_clients_after_stop(plugin, monkeypatch):
    async def run():
        plugin.config.server.api_key_cn = "test-cn-key"
        captured, release = asyncio.Event(), asyncio.Event()
        client = plugin._client
        rebuilt = []

        async def account():
            captured.set()
            await release.wait()
            return {"remainCoins": 1}

        def rebuild():
            rebuilt.append(True)
            plugin._client = Client()
            plugin._client_cn = Client()

        client.account_status = account
        monkeypatch.setattr(plugin, "_web_jsonify", lambda value: value)
        monkeypatch.setattr(plugin, "_rebuild_client", rebuild)
        request = asyncio.create_task(plugin.handle_page_get_account())
        await captured.wait()
        await plugin.terminate()
        release.set()
        result = await request
        assert not result["success"] and not rebuilt and plugin._client is None and plugin._client_cn is None
    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


def test_disabled_instance_cannot_rebuild_clients_directly(plugin):
    async def run():
        await plugin.terminate()
        plugin._rebuild_client()
        assert plugin._client is None and plugin._client_cn is None
    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


def test_chat_resend_cannot_send_after_stop_during_download(runtime):
    async def run():
        await completed_task(runtime, event(), ["https://example.test/result.png"])
        runtime.context.sent.clear()
        outputs = runtime._task_journal.get("finished-1")["outputs"]
        outputs[0]["image_ref"] = ""
        await runtime._task_journal.update("finished-1", outputs=outputs)
        captured, release = asyncio.Event(), asyncio.Event()
        sends = []

        async def download(url):
            captured.set()
            await release.wait()
            return base64.b64encode(PNG).decode("ascii")

        async def send(*args, **kwargs):
            sends.append(True)
            return DeliveryReceipt(True)

        runtime._client.download_base64 = download
        runtime.delivery.send_image_result = send
        request = asyncio.create_task(runtime._manage_workflow_task(event(mid="stopped-resend"), "resend", "latest"))
        await captured.wait()
        await runtime.terminate()
        release.set()
        result = json.loads(await request)
        assert not result["success"] and not sends and not runtime.context.sent and not runtime._recall_tasks
        output = runtime._task_journal.get("finished-1")["outputs"][0]
        assert not output["sent"] and not output["uncertain"]
    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


def test_disabled_instance_cannot_schedule_recall(plugin):
    async def run():
        await plugin.terminate()
        plugin._schedule_recall("message", 100)
        assert not plugin._recall_tasks
    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))
