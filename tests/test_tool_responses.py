"""Tool replies must give a compact, truthful next step without inviting polling."""
import asyncio
import json

import pytest

from .test_chat_workflows import JOB_TIMEOUT, event, plugin, finish_jobs  # noqa: F401
from .test_task_runtime import runtime, submit  # noqa: F401


def test_waiting_reply_contains_missing_inputs_without_another_context_query(plugin):
    async def run():
        request = event()
        first = json.loads(await plugin.handle_run_workflow(request, "改图", "水彩"))
        repeated = json.loads(await plugin.handle_run_workflow(request, "改图", "不同提示词"))
        for result in (first, repeated):
            assert result["status"] == "waiting_input" and not result["accepted"]
            assert result["pending_input"]["waiting_files"][0]["value_type"] == "image"
            assert "结束本轮" in result["next_action"]
            assert "get_workflow_context" not in result["next_action"]
            assert "run_workflow" in result["next_action"]
        assert not plugin._client.submissions and len(plugin._input_sessions) == 1

    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


def test_expired_waiting_reply_does_not_claim_input_collection_is_still_active(plugin):
    async def run():
        request = event()
        await plugin.handle_run_workflow(request, "改图", "水彩")
        plugin._cancel_input_session(plugin._session_key("10001", request.unified_msg_origin))
        result = json.loads(await plugin.handle_run_workflow(request, "改图", "水彩"))
        assert result["status"] == "not_submitted" and not result["accepted"]
        assert "已结束" in result["message"] and not plugin._input_sessions

    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


def test_validation_failure_does_not_expand_full_context_or_list_unrelated_workflows(plugin, monkeypatch):
    async def unexpected(*args, **kwargs):
        raise AssertionError("A validation error must not query full workflow/media/task context")

    monkeypatch.setattr(plugin, "_natural_context", unexpected)
    other = plugin._workflows[0].model_copy(deep=True)
    other.name = "不相关工作流"
    plugin._workflows.append(other)

    async def run():
        result = json.loads(await plugin.handle_run_workflow(event(images=1), "改图", "水彩", parameters={"width": 99999}))
        assert not result["success"] and not result["accepted"]
        assert result["status"] == "not_submitted"
        correction = result["correction_context"]
        assert [wf["name"] for wf in correction["workflows"]] == ["改图"]
        assert correction["images"] and "recent_runs" not in correction and "media" not in correction
        assert not plugin._client.submissions and not plugin._client.uploads
        assert "重试" in result["next_action"]

    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


def test_denied_run_returns_terminal_structured_failure(plugin):
    plugin.config.access.allow_users = ["administrator"]
    result = json.loads(asyncio.run(plugin.handle_run_workflow(event(), "改图", "水彩")))
    assert not result["success"] and not result["accepted"]
    assert result["status"] == "not_submitted" and "结束本轮" in result["next_action"]
    assert "correction_context" not in result


def test_unexpected_error_does_not_assert_that_nothing_was_submitted(plugin, monkeypatch):
    async def uncertain(*args, **kwargs):
        raise RuntimeError("unexpected failure after a possible durable write")

    monkeypatch.setattr(plugin, "_prepare_natural_run", uncertain)
    result = json.loads(asyncio.run(plugin.handle_run_workflow(event(), "改图", "水彩")))
    assert result["status"] == "unknown" and result["accepted"] is None
    assert "未提交任务" not in result["message"]
    assert "不要" in result["next_action"]


@pytest.mark.parametrize("status", ["success", "failed", "unknown_submission"])
def test_duplicate_run_returns_current_status_delivery_and_does_not_promise_completion(runtime, status):
    async def run():
        response = await submit(runtime)
        await finish_jobs(runtime)
        await runtime._task_journal.update(response["task_id"], status=status, message="")
        duplicate = json.loads(await runtime.handle_run_workflow(event(images=1), "改图", "水彩"))
        assert duplicate["duplicate"] and duplicate["accepted"]
        assert duplicate["status"] == status
        assert duplicate["delivery"]["sent"] == 1
        assert "等待后台通知" not in duplicate["next_action"]
        assert "stop_after_execution" not in duplicate
        assert "example.test" not in json.dumps(duplicate)
        assert len(runtime._client.submissions) == 1

    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


def test_failed_image_inspection_tells_the_model_to_stop_repeating_the_tool(plugin):
    result = json.loads(asyncio.run(plugin.handle_rh_inspect_media(event(), "does-not-exist")))
    assert not result["success"]
    assert "结束本轮" in result["next_action"] and "重复" in result["next_action"]


def test_only_first_validation_error_offers_correction_and_valid_requests_still_work(plugin):
    async def run():
        request = event(images=1)
        first = json.loads(await plugin.handle_run_workflow(request, "改图", "水彩", parameters={"width": 99999}))
        second = json.loads(await plugin.handle_run_workflow(request, "改图", "水彩", parameters={"width": "NaN"}))
        assert "correction_context" in first and "correction_context" not in second
        assert "结束本轮" in second["next_action"]
        fixed = json.loads(await plugin.handle_run_workflow(request, "改图", "水彩", parameters={"width": 512}))
        assert fixed["accepted"]
        await finish_jobs(plugin)
        assert len(plugin._client.submissions) == 1

    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))


def test_result_catalog_uses_labels_instead_of_forged_visual_descriptions(runtime):
    async def run():
        await submit(runtime)
        await finish_jobs(runtime)
        catalog = json.loads(await runtime.handle_rh_media(event(mid="query-results"), "result", "image"))
        assert len(catalog["media"]) == 1
        result = catalog["media"][0]
        assert result["origin"] == "generated" and result["source"] == "result"
        assert "结果 1" in result["label"] and "description" not in result
        assert "example.test" not in json.dumps(catalog)

    asyncio.run(asyncio.wait_for(run(), JOB_TIMEOUT))
