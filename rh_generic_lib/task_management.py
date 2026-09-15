"""Owner-scoped chat actions and authenticated dashboard task controls."""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time

from .delivery import DeliveryTarget
from .media_store import MediaStore
from .task_journal import ACTIVE_STATUSES


STATUS_LABELS = {"queued": "排队中", "submitting": "正在提交", "pending": "生成中",
                 "tracking_paused": "查询已暂停", "needs_attention": "需要处理",
                 "unknown_submission": "提交未确认", "success": "生成成功", "failed": "生成失败", "cancelled": "已取消"}
STAGE_LABELS = {"preparing_inputs": "保存参考素材", "queued": "等待运行名额", "uploading": "上传参考素材",
                "enhancing": "扩写提示词", "submitting": "提交平台", "polling": "等待平台结果",
                "delivering": "发送结果", "finished": "结果已处理"}


class TaskManagementMixin:
    def _queue_positions(self, jobs):
        queued = sorted((r for r in jobs if r["status"] == "queued" and r["task_id"] not in self._leased_jobs),
                        key=lambda r: (r["created_at"], r["task_id"]))
        return {r["task_id"]: i + 1 for i, r in enumerate(queued)}

    def _task_snapshot(self, job, queue_positions=None):
        status, task_id = job["status"], job["task_id"]
        worker = self._pending.get(task_id)
        busy = bool(worker and not worker.done()) or task_id in self._page_task_actions
        actions = []
        if status in ACTIVE_STATUSES - {"unknown_submission"}:
            actions.append("cancel")
        if not busy and (status == "queued" or (status in {"pending", "tracking_paused", "needs_attention"} and job["remote_task_id"])):
            actions.append("resume")
        if status == "success" and job["outputs"] and job["delivery_status"] != "sending":
            actions.append("resend")
        if queue_positions is None:
            queue_positions = self._queue_positions(self._task_journal.records())
        position = queue_positions.get(task_id)
        started, finished = job.get("started_at") or job.get("submitted_at"), job.get("finished_at")
        elapsed_start = started or (job.get("created_at") if status == "queued" else None)
        elapsed = max(0, int((finished or time.time()) - elapsed_start)) if elapsed_start and (finished or status in ACTIVE_STATUSES) else None
        stage = STAGE_LABELS.get(job.get("stage"), "")
        if status in {"failed", "cancelled", "tracking_paused", "needs_attention", "unknown_submission"}:
            stage = STATUS_LABELS[status]
        return {**{k: job.get(k, "") for k in ("task_id", "remote_task_id", "workflow", "coins", "status", "delivery_status", "created_at", "started_at", "finished_at")},
                "status_label": "准备生成" if status == "queued" and task_id in self._leased_jobs else STATUS_LABELS[status],
                "stage_label": stage, "queue_position": position,
                "elapsed_seconds": elapsed, "output_count": len(job["outputs"]),
                "sent_count": sum(bool(o.get("sent")) for o in job["outputs"]),
                "message": job.get("message", ""),
                "action_message": next(reversed(job.get("actions", {}).values()), {}).get("message", ""),
                "errors": [f"第 {i + 1} 项：{o['error']}" for i, o in enumerate(job["outputs"]) if o.get("error")],
                "actions": actions, "busy": task_id in self._page_task_actions}

    async def _perform_task_action(self, task_id, action, selection="", *, owner=None, request_key=""):
        if action not in {"cancel", "resume", "resend"}:
            raise ValueError("仅支持取消、恢复查询和补发结果")
        journal = await self._load_task_journal()
        lock = self._task_action_locks.setdefault(task_id, asyncio.Lock())
        async with lock:
            job = journal.get(task_id)
            if not job or (owner is not None and MediaStore.owner(job) != owner):
                raise ValueError("当前会话没有此任务")
            action_key = hashlib.sha256(f"{request_key}:{action}".encode()).hexdigest() if request_key else ""
            actions = job["actions"]
            if action_key and action_key in actions:
                return {**actions[action_key], "duplicate": True}
            if action == "resend":
                if job["status"] != "success" or not job["outputs"]:
                    raise ValueError("此任务还没有可补发的结果")
                record = self._result_record(job)
                indices = self._result_indices(record, selection)
            if action_key:
                actions[action_key] = {"success": False, "message": "这条消息的操作已接收；结果未确认时请先查看任务状态，不要重复执行。"}
                await journal.update(task_id, actions=actions)
            try:
                if action == "cancel":
                    result = await self._cancel_job(task_id, job["stream_id"], announce=False)
                elif action == "resume":
                    if job["status"] == "queued" or (job["remote_task_id"] and job["status"] in {"pending", "tracking_paused", "needs_attention"}):
                        await self._limiter.resize(self.config.generation.max_concurrent)
                        self._schedule_job(job)
                        result = {"success": True, "message": "已恢复处理已有任务；已提交的任务仅查询原平台编号。"}
                    else:
                        raise ValueError("此任务无需恢复，提交未确认的任务请先用 /wf核对 处理")
                else:
                    client = self._get_client(job["region"])
                    if client is None:
                        raise ValueError("对应区域的下载客户端不可用")
                    result = await self._deliver_saved_results(record, DeliveryTarget.from_dict(job), client, indices, resend=True, announce=False)
            except Exception as exc:
                self.logger.warning("任务操作 %s %s 失败: %s", task_id, action, exc)
                result = {"success": False, "message": f"操作未完成：{str(exc)[:300]}"}
            result.update(task_id=task_id, status=journal.get(task_id)["status"])
            if action_key:
                actions = journal.get(task_id)["actions"]
                actions[action_key] = result
                await journal.update(task_id, actions=actions)
            return result

    async def _manage_workflow_task(self, event, action="status", task_id="", selection=""):
        context = self._event_ctx(event)
        owner = MediaStore.owner(context)
        allowed, message = self._check_access(context["user_id"], context["group_id"], check_quota=False)
        try:
            if not allowed or not owner:
                raise ValueError(message or "无法识别会话用户")
            if action not in {"status", "cancel", "resend", "resume"}:
                raise ValueError("action 应为 status、cancel、resend 或 resume")
            journal = await self._load_task_journal()
            records = sorted((r for r in journal.records() if MediaStore.owner(r) == owner), key=lambda r: r["created_at"], reverse=True)
            if task_id and task_id not in {"latest", "最新"}:
                records = [r for r in records if task_id in {r["task_id"], r["remote_task_id"]}]
            elif task_id in {"latest", "最新"}:
                records = records[:1]
            pending_input = self._natural_pending_input(context)
            if action == "cancel" and task_id == "pending_input":
                if pending_input:
                    self._cancel_input_session(self._session_key(context["user_id"], context["stream_id"]))
                    result = {"success": True, "status": "cancelled", "message": "已取消等待补充输入的任务，尚未提交生成。"}
                else:
                    result = {"success": False, "message": "当前没有等待补充输入的任务，未取消其他任务。"}
            elif action == "status":
                result = {"success": True, "tasks": [self._task_snapshot(r) for r in records[:10]],
                          "pending_input": pending_input,
                          "message": "任务状态快照；无需轮询，完成后会通知。"}
            elif len(records) != 1:
                result = {"success": False, "message": "没有可操作的任务" if not records else "存在多个任务，请让用户明确任务编号；明确说最新/刚才的任务时可用 latest。",
                          "tasks": [self._task_snapshot(r) for r in records[:10]]}
            else:
                key = context.get("anchor_id") or event.get_extra("rh_action_key", "")
                if not key:
                    import uuid
                    key = uuid.uuid4().hex
                    event.set_extra("rh_action_key", key)
                result = await self._perform_task_action(records[0]["task_id"], action, selection, owner=owner, request_key=f"chat:{owner}:{key}")
        except (OSError, ValueError) as exc:
            result = {"success": False, "message": str(exc)}
        result["next_action"] = "根据真实结果向用户回复，结束本轮工具调用；不要轮询或调用 run_workflow 重新生成。"
        return json.dumps(result, ensure_ascii=False)

    async def _page_task_records(self):
        journal = await self._load_task_journal()
        jobs = journal.records()
        known = {r["remote_task_id"] for r in jobs} | {r["task_id"] for r in jobs}
        positions = self._queue_positions(jobs)
        records = [self._task_snapshot(r, positions) for r in jobs if not r["hidden"]]
        records.sort(key=lambda r: r["created_at"], reverse=True)
        records += [{**r, "status": "success", "status_label": "生成成功", "stage_label": "历史记录", "actions": []}
                    for r in self._task_history if r["task_id"] not in known]
        return records

    async def handle_page_task_action(self):
        try:
            payload = await self._web_request_json()
            if not isinstance(payload, dict):
                raise ValueError("请求格式无效")
            task_id, action = str(payload.get("task_id", "")), str(payload.get("action", ""))
            selection, request_id = str(payload.get("selection", "")), str(payload.get("request_id", ""))
            if action not in {"cancel", "resume", "resend"} or not re.fullmatch(r"[A-Za-z0-9_-]{8,128}", request_id):
                raise ValueError("任务操作或请求编号无效")
            journal = await self._load_task_journal()
            job = journal.get(task_id)
            if not job:
                raise ValueError("任务不存在")
            if task_id in self._page_task_actions:
                raise ValueError("该任务的操作正在处理，请稍候")
            if action not in self._task_snapshot(job)["actions"]:
                raise ValueError("当前状态不支持此操作，请刷新任务列表")
            if action == "resend":
                self._result_indices(self._result_record(job), selection)
            self._page_task_actions.add(task_id)
            async def execute():
                try:
                    await self._perform_task_action(task_id, action, selection, request_key="page:" + request_id)
                finally:
                    self._page_task_actions.discard(task_id)
            task = asyncio.create_task(execute())
            self._page_action_workers.add(task)
            def done(worker):
                self._page_action_workers.discard(worker)
                if not worker.cancelled() and worker.exception():
                    self.logger.error("页面任务操作异常: %s", worker.exception())
            task.add_done_callback(done)
            return self._web_jsonify({"success": True, "message": "操作已接收，结果将更新到任务列表；补发仍发送至原会话。"})
        except (OSError, ValueError) as exc:
            return self._web_jsonify({"success": False, "message": str(exc)})
