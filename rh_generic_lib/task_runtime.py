"""Persist reservations before paid requests; keep unresolved remote jobs reserved."""
from __future__ import annotations

import asyncio
import copy
import json
import re
import time
import uuid
from contextlib import asynccontextmanager

from .config import WorkflowItemSection
from .delivery import DeliveryTarget
from .media_store import MediaStore
from .runninghub_client import RunningHubError, RunningHubTransportError
from .task_journal import ACTIVE_STATUSES, TaskJournal
from .task_inputs import TaskInputs
from .file_source import trusted_local_file
from .media_store import MAX_IMAGE_BYTES


class TaskRuntimeMixin:
    def _get_task_inputs(self):
        if self._task_inputs is None:
            self._task_inputs = TaskInputs(self._prompt_library_path().parent / "task_inputs")
        return self._task_inputs

    async def _preserve_job_inputs(self, task_id, request, inputs=None, *, local_only=False):
        """Copy local references before acknowledging; prefetch URLs outside paid slots."""
        inputs = dict(inputs or {})
        context = request.get("context", {})
        owner = MediaStore.owner(context)
        self._avatar_scopes[owner] = context.copy()
        refs = dict.fromkeys(context.get("image_bindings", {}).values())
        for ref in refs:
            if ref in inputs:
                self._get_task_inputs().read(task_id, inputs[ref])
                continue
            source = ""
            if ref.startswith("avatar:"):
                if local_only:
                    continue
                data = await self._read_special_image(owner, ref, self._get_client(request["workflow"]["region"]))
            elif ref.startswith("im-"):
                data = await self._read_special_image(owner, ref, None)
            else:
                # Snapshot avoids pruning the shared cache before its bytes are copied.
                entry = context.get("image_sources", {}).get(ref)
                if not entry:
                    entry = self._get_media_store().get(owner, ref)
                source = entry["source"]
                if source.startswith(("http://", "https://")):
                    if local_only:
                        continue
                    client = self._get_client(request["workflow"]["region"])
                    data = await client.download_bytes(source, max_bytes=MAX_IMAGE_BYTES)
                else:
                    path = trusted_local_file(source, self._trusted_file_roots())
                    if path is None or path.stat().st_size > MAX_IMAGE_BYTES:
                        raise ValueError("参考图片缓存不可用或超过 20MB，请重新发送")
                    data = path.read_bytes()
            inputs[ref] = self._get_task_inputs().put(task_id, data, self._guess_filename(source, "image", data))
            if not local_only:
                if not await self._task_journal.update(task_id, inputs=inputs, expected_statuses={"queued"}):
                    return inputs
        return inputs

    def _release_terminal_inputs(self, task_id):
        record = self._task_journal.get(task_id)
        if record and record["status"] not in ACTIVE_STATUSES and task_id.startswith("rh-"):
            try:
                self._get_task_inputs().release(task_id)
            except (OSError, ValueError) as exc:
                self.logger.warning("清理任务素材失败 %s: %s", task_id, exc)

    async def _load_task_journal(self):
        if self._task_journal is None:
            self._task_journal = TaskJournal(self._prompt_library_path().parent / "task_journal.json")
        await self._task_journal.load()
        return self._task_journal

    async def _find_event_job(self, context):
        anchor = context.get("anchor_id")
        if not anchor:
            return None
        journal = await self._load_task_journal()
        owner = MediaStore.owner(context)
        return next((r for r in journal.records() if r.get("anchor_id") == anchor
                     and MediaStore.owner(r) == owner), None)

    async def _reserve_job(self, client, workflow, node_info_list, stream_id, kwargs):
        context = {**kwargs, "stream_id": stream_id}
        if not MediaStore.owner(context):
            return {"success": False, "message": "无法识别当前用户或会话"}
        try:
            async with self._request_lock:
                journal = await self._load_task_journal()
                previous = await self._find_event_job(context)
                if previous:
                    return {"success": True, "duplicate": True, "task_id": previous["task_id"],
                            "status": previous["status"], "message": "本条消息的任务已记录，请勿重复生成"}
                allowed, reason = self._check_access(context["user_id"], context.get("group_id", ""), check_quota=False)
                if not allowed:
                    raise ValueError(reason)
                limit = self.config.access.max_per_user_per_hour
                if limit and journal.quota_used(context["user_id"], time.time()) >= limit:
                    raise ValueError("本小时的任务次数已达上限（包含排队及提交未确认的任务）")
                active = sum(r["status"] in ACTIVE_STATUSES for r in journal.records())
                if active >= self.config.generation.max_concurrent + self.config.generation.max_queued:
                    raise ValueError("任务队列已满，请稍后再试；用 /wf状态 查看，或 /wf中断 取消旧任务")
                # Only host-derived serializable context is persisted. Never keep SDK events or keys.
                saved_context = {k: copy.deepcopy(context[k]) for k in (
                    "user_id", "group_id", "platform_id", "platform_name", "stream_id", "anchor_id",
                    "result_context", "image_bindings", "image_sources", "natural", "skip_enhance",
                    "actual_file_desc", "trigger") if k in context}
                request = {"workflow": workflow.model_dump(), "nodes": copy.deepcopy(node_info_list),
                           "context": saved_context}
                task_id = "rh-" + uuid.uuid4().hex[:16]
                try:
                    inputs = await self._preserve_job_inputs(task_id, request, local_only=True)
                    await journal.update(task_id, status="queued", stage="preparing_inputs", inputs=inputs,
                                         submitted_at=0, request=request, workflow=workflow.name, region=workflow.region,
                                         **{k: context.get(k, "") for k in ("stream_id", "user_id", "group_id", "platform_id", "platform_name", "anchor_id")})
                except BaseException:
                    if not journal.get(task_id):
                        self._get_task_inputs().release(task_id)
                    raise
                await self._limiter.resize(self.config.generation.max_concurrent)
                self._schedule_job(journal.get(task_id))
                try:
                    self._remember_workflow_run(task_id, workflow, node_info_list, stream_id, kwargs)
                except Exception as exc:
                    self.logger.warning("任务已记录，但复用记录保存失败: %s", exc)
                self._set_workflow_run_state(task_id, stream_id, kwargs, "queued", "已记录，等待后台处理")
                result = {"success": True, "task_id": task_id, "status": "queued",
                          "message": f"已接手，任务 {task_id} 将在后台运行，完成后通知。"}
                source_event = kwargs.get("natural_event")
                if source_event is not None:
                    source_event.set_extra("rh_natural_result", self._job_tool_result(result))
                return result
        except (OSError, ValueError) as exc:
            return {"success": False, "message": str(exc)}

    @staticmethod
    def _job_tool_result(result):
        return json.dumps({"task_id": result.get("task_id", ""), "status": result.get("status", "queued"),
                           "message": result.get("message", "任务已记录，后台完成后会通知"),
                           "stop_after_execution": True, "next_action": "等待后台通知，结束本轮工具调用，不要轮询或重复生成"}, ensure_ascii=False)

    def _schedule_job(self, record):
        task_id = record["task_id"]
        if task_id in self._pending and not self._pending[task_id].done():
            return
        self._task_meta[task_id] = {**{k: record.get(k, "") for k in (
            "stream_id", "user_id", "group_id", "platform_id", "platform_name", "region")}, "name": record["workflow"]}
        task = asyncio.create_task(self._run_job(task_id))
        self._pending[task_id] = task
        def done(finished):
            if self._pending.get(task_id) is finished:
                self._pending.pop(task_id, None)
            latest = self._task_journal.get(task_id)
            if latest and latest["status"] not in ACTIVE_STATUSES:
                self._task_meta.pop(task_id, None)
            if not finished.cancelled() and finished.exception():
                self.logger.error("后台任务 %s 异常: %s", task_id, finished.exception())
        task.add_done_callback(done)

    @asynccontextmanager
    async def _job_slot(self, task_id):
        def external():
            if self._task_journal.get(task_id)["status"] != "queued":
                return 0
            return sum(r["status"] in ACTIVE_STATUSES - {"queued"} and r["task_id"] not in self._leased_jobs
                       for r in self._task_journal.records())
        async with self._limiter.slot(extra=external):
            self._leased_jobs.add(task_id)
            try:
                yield
            finally:
                self._leased_jobs.discard(task_id)

    async def _resume_pending_tasks(self):
        journal = await self._load_task_journal()
        try:
            self._get_task_inputs().sweep({r["task_id"] for r in journal.records() if r["status"] in ACTIVE_STATUSES})
        except (OSError, ValueError) as exc:
            self.logger.warning("清理已结束任务素材失败: %s", exc)
        await self._limiter.resize(self.config.generation.max_concurrent)
        for record in sorted(journal.recoverable_records(), key=lambda r: (r["status"] == "queued", r["created_at"])):
            if record["status"] == "submitting":
                await journal.update(record["task_id"], status="unknown_submission", message="提交期间进程退出，需用 /wf核对 核对平台任务，禁止自动重提")
            elif record["status"] not in {"unknown_submission", "needs_attention"}:
                self._schedule_job(record)

    async def _run_job(self, task_id):
        journal = await self._load_task_journal()
        try:
            record = journal.get(task_id)
            client = self._get_client(record["region"])
            if record["status"] == "queued" and client is not None and getattr(client, "api_key", None) != "":
                async with self._input_fetch_limiter:
                    if journal.get(task_id)["status"] != "queued":
                        return
                    await journal.update(task_id, stage="preparing_inputs", message="保存任务参考素材")
                    inputs = await self._preserve_job_inputs(task_id, record["request"], record["inputs"])
                    await journal.update(task_id, inputs=inputs, inputs_ready=True, stage="queued", message="等待运行名额", expected_statuses={"queued"})
            async with self._job_slot(task_id):
                record = journal.get(task_id)
                if record["status"] in {"cancelled", "failed", "unknown_submission"}:
                    return
                client = self._get_client(record["region"])
                if client is None or getattr(client, "api_key", None) == "":
                    if record["remote_task_id"]:
                        await journal.update(task_id, status="needs_attention", message="对应区域 API Key 未配置", expected_statuses=ACTIVE_STATUSES)
                    await self._notify_job(journal.get(task_id), "任务等待对应区域的 RunningHub 配置恢复；用 /wf状态 继续查询。")
                    return
                if record["status"] == "success":
                    await self._deliver_job(record, client)
                    return
                context = record["request"]["context"]
                if record["status"] == "queued":
                    await journal.update(task_id, started_at=record.get("started_at") or time.time(), stage="uploading")
                    allowed, reason = self._check_access(record["user_id"], record["group_id"], check_quota=False)
                    if not allowed:
                        raise ValueError(reason)
                    workflow = WorkflowItemSection.model_validate(record["request"]["workflow"])
                    nodes = copy.deepcopy(record["request"]["nodes"])
                    owner = MediaStore.owner(record)
                    self._avatar_scopes[owner] = context.copy()
                    uploaded = {}
                    for node in nodes:
                        ref = context.get("image_bindings", {}).get(f"{node['nodeId']}/{node['fieldName']}")
                        if ref and node["fieldValue"].startswith("rh-image-ref:"):
                            await journal.update(task_id, message="准备参考图片")
                            if ref not in uploaded:
                                item = journal.get(task_id)["inputs"][ref]
                                data, filename = self._get_task_inputs().read(task_id, item), item["filename"]
                                if not ref.startswith(("im-", "avatar:")):
                                    try:
                                        self._get_media_store().cache(owner, ref, data)
                                    except (ValueError, OSError) as exc:
                                        self.logger.debug("任务素材已保存，聊天缓存不可用: %s", exc)
                                await self._remember_image_usage(owner, ref, data)
                                uploaded[ref] = await client.upload_file(data, filename)
                            node["fieldValue"] = uploaded[ref]
                    prompt_node = self._first_prompt_node(workflow)
                    if prompt_node and workflow.llm_enhance and not context.get("skip_enhance"):
                        await journal.update(task_id, stage="enhancing", message="扩写提示词")
                        original = next((n["fieldValue"] for n in nodes if n["nodeId"] == prompt_node.node_id and n["fieldName"] == prompt_node.field_name), "")
                        enhanced = await self._enhance_text(workflow, original, stream_id=record["stream_id"],
                                                           actual_file_desc=context.get("actual_file_desc", ""))
                        nodes = self._patch_text_value(nodes, prompt_node.node_id, prompt_node.field_name, enhanced)
                    allowed, reason = self._check_access(record["user_id"], record["group_id"], check_quota=False)
                    if not allowed:
                        raise ValueError(reason)
                    if not await journal.update(task_id, status="submitting", stage="submitting", submitted_at=time.time(), message="提交 RunningHub",
                                                expected_statuses={"queued"}):
                        return
                    try:
                        remote = await client.submit(nodes, instance_type=workflow.instance_type, workflow_id=workflow.workflow_id)
                    except RunningHubTransportError:
                        await journal.update(task_id, status="unknown_submission", message="提交响应丢失，需核对平台任务，禁止自动重新生成")
                        await self._notify_job(journal.get(task_id))
                        return
                    except RunningHubError as exc:
                        await journal.update(task_id, status="failed", submitted_at=0, message=f"提交被拒绝：{exc}")
                        await self._notify_job(journal.get(task_id))
                        return
                    await journal.update(task_id, status="pending", stage="polling", remote_task_id=str(remote), message="等待平台结果")
                    try:
                        self._remember_workflow_run(task_id, workflow, nodes, record["stream_id"], context)
                    except Exception as exc:
                        self.logger.warning("任务已提交，但复用记录保存失败: %s", exc)
                    if journal.get(task_id).get("cancel_requested"):
                        await self._cancel_job(task_id, record["stream_id"])
                        if journal.get(task_id)["status"] != "pending":
                            return
                if record["remote_task_id"]:
                    await journal.update(task_id, status="pending", stage="polling", message="正在查询原平台任务",
                                         expected_statuses={"tracking_paused", "needs_attention"})
                await self._poll_and_send(task_id, record["stream_id"], client=client, kwargs=context)
        except asyncio.CancelledError:
            record = journal.get(task_id)
            if record["status"] == "submitting":
                await journal.update(task_id, status="unknown_submission", message="提交期间中断，需核对平台任务")
            raise
        except Exception as exc:
            record = journal.get(task_id)
            self.logger.exception("任务 %s 后台处理失败", task_id)
            status = record["status"]
            if status == "cancelled":
                return
            updates = {"message": f"{record.get('message') or '准备任务'}失败：{str(exc)[:300]}"}
            if status == "queued":
                updates.update(status="failed", submitted_at=0)
            elif status == "submitting":
                updates.update(status="unknown_submission", message="提交状态未确认，需核对平台任务，禁止自动重新生成")
            elif status != "success":
                updates.update(status="needs_attention")
            await journal.update(task_id, **updates)
            await self._notify_job(journal.get(task_id))
        finally:
            self._release_terminal_inputs(task_id)

    async def _poll_job(self, task_id, stream_id, *, client=None, kwargs=None):
        journal = await self._load_task_journal()
        record = journal.get(task_id)
        # Compatibility with callers tracking a known remote task directly.
        if record is None:
            context = {**(kwargs or {}), "stream_id": stream_id}
            meta = self._task_meta.get(task_id, {})
            await journal.update(task_id, status="pending", remote_task_id=task_id,
                                 workflow=meta.get("name", ""), region=meta.get("region", "overseas"),
                                 request={"context": context, "workflow": {}},
                                 **{k: context.get(k, "") for k in ("stream_id", "user_id", "group_id", "platform_id", "platform_name")})
            record = journal.get(task_id)
        client = client or self._get_client(record["region"])
        try:
            result = await client.wait_for_result(record["remote_task_id"])
        except (RunningHubError, TimeoutError) as exc:
            remote_status = getattr(exc, "task_status", "")
            status = ("cancelled" if remote_status.startswith("CANCEL") else "failed" if remote_status in {"FAILED", "ERROR"}
                      else "tracking_paused" if isinstance(exc, (RunningHubTransportError, TimeoutError)) else "needs_attention")
            if await journal.update(task_id, status=status, message=str(exc)[:300], expected_statuses=ACTIVE_STATUSES):
                await self._notify_job(journal.get(task_id))
            return
        outputs = []
        for item in result.get("results") or []:
            if not isinstance(item, dict):
                continue
            url = str(item.get("url") or item.get("outputUrl") or item.get("fileUrl") or "").strip()
            if url:
                kind = str(item.get("outputType") or item.get("fileType") or "")
                if not kind and self._detect_file_type_from_name(url) == "unknown":
                    kind = record["request"].get("workflow", {}).get("output_type", "auto")
                outputs.append({"url": url, "type": kind, "sent": False, "image_ref": ""})
        coins = self._consume_coins_from_result(result)
        if not await journal.mark_success(task_id, coins, outputs, expected_statuses=ACTIVE_STATUSES):
            return
        self._set_workflow_run_state(task_id, stream_id, kwargs or {}, "succeeded")
        try:
            await self._record_task_history(record["remote_task_id"], record["workflow"], coins)
        except Exception as exc:
            self.logger.warning("记录消耗失败: %s", exc)
        await self._deliver_job(journal.get(task_id), client)

    async def _deliver_job(self, job, client):
        if job["delivery_status"] in {"sending", "uncertain", "sent"}:
            return
        context = job["request"].get("context", {})
        record = {"owner": MediaStore.owner(job), "task_id": job["task_id"], "journal_id": job["task_id"],
                  "workflow": job["workflow"], "region": job["region"], "outputs": job["outputs"],
                  "saved": False, "platform_name": job["platform_name"], **context.get("result_context", {})}
        try:
            if record["owner"]:
                record["saved"] = True
                self._get_media_store().remember_delivery(**record)
        except Exception as exc:
            record["saved"] = False
            self.logger.warning("补发记录保存失败，结果仍保留在任务日志: %s", exc)
        if not record["outputs"]:
            await self._task_journal.update(job["task_id"], delivery_status="sent", message="平台已完成但没有可发送的结果，请检查输出节点")
            await self._notify_job(self._task_journal.get(job["task_id"]))
            return
        await self._deliver_saved_results(record, DeliveryTarget.from_dict(job), client)

    async def _notify_job(self, job, message=""):
        status = job["status"]
        context = job["request"].get("context", {})
        self._set_workflow_run_state(job["task_id"], job["stream_id"], context,
                                     "succeeded" if status == "success" else status, job.get("message", ""))
        key = f"result:{status}:{job['delivery_status']}"
        if not await self._task_journal.claim_notification(job["task_id"], key):
            return
        summary = message or f"任务 {job['task_id']}（{job['workflow']}）状态：{status}。{job.get('message', '')}"
        if status in {"tracking_paused", "needs_attention"}:
            summary += f" 保留平台编号 {job['remote_task_id']} 和运行名额；处理配置后用 /wf状态 {job['task_id']} 恢复查询，不要重复生成。"
        elif status == "unknown_submission":
            summary += f" 管理员请用 /wf核对 {job['task_id']} 平台任务ID（或 未创建）处理。"
        record = {"task_id": job["task_id"], "workflow": job["workflow"], "status": status, "outputs": job["outputs"],
                  "platform_name": job["platform_name"], **context.get("result_context", {})}
        target = DeliveryTarget.from_dict(job)
        notified = await self._notify_workflow_result(record, target, summary)
        if not notified:
            await self.delivery.send_text(target, summary)

    async def _cancel_job(self, task_id, stream_id, *, announce=True):
        journal = await self._load_task_journal()
        async def reply(message):
            if announce:
                await self._send_text(stream_id, message)
            latest = journal.get(task_id)
            return {"success": bool(latest and (latest["status"] == "cancelled" or (latest["status"] == "submitting" and latest.get("cancel_requested")))),
                    "status": latest["status"] if latest else "missing", "message": message}
        record = journal.get(task_id)
        if record is None:
            return await reply("任务不存在或已结束")
        status = record["status"]
        if status not in ACTIVE_STATUSES:
            return await reply("任务已结束，不再取消或重新生成")
        if status == "submitting":
            await journal.update(task_id, cancel_requested=True, expected_statuses={"submitting"})
            return await reply("正在提交，已记下取消请求；取得平台编号后再取消，请勿重复生成。")
        if status == "unknown_submission":
            return await reply(f"提交状态未知，保留名额；请管理员用 /wf核对 {task_id} 核对平台任务。")
        if status != "queued":
            client = self._get_client(record["region"])
            try:
                if client is None:
                    raise RunningHubError("对应区域客户端不可用")
                result = await client.cancel(record["remote_task_id"])
                if not isinstance(result, dict) or not result:
                    raise RunningHubTransportError("取消响应未确认")
                if result.get("code") not in (0, 200) or result.get("status") == "failed":
                    raise RunningHubError(f"平台拒绝取消：{str(result)[:200]}")
            except Exception as exc:
                # A late refusal cannot overwrite a concurrently observed success.
                latest = journal.get(task_id)
                if latest["status"] not in ACTIVE_STATUSES:
                    return await reply("任务已结束，已保留生成结果。")
                if not isinstance(exc, (RunningHubTransportError, TimeoutError)):
                    await journal.update(task_id, status="needs_attention", message=f"取消被拒绝：{str(exc)[:200]}", expected_statuses=ACTIVE_STATUSES)
                return await reply("平台取消未确认，任务可能继续运行；保留编号、查询和运行名额，请核对 RunningHub。")
        changed = await journal.update(task_id, status="cancelled", message="已取消", expected_statuses=ACTIVE_STATUSES)
        if not changed:
            return await reply("任务已完成，已保留生成结果。")
        worker = self._pending.get(task_id)
        if worker and worker is not asyncio.current_task():
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        self._task_meta.pop(task_id, None)
        self._release_terminal_inputs(task_id)
        await self._limiter.resize(self.config.generation.max_concurrent)
        if announce:
            await self._notify_job(journal.get(task_id))
        else:
            self._set_workflow_run_state(task_id, stream_id, record["request"].get("context", {}), "cancelled", "已取消")
        return {"success": True, "status": "cancelled", "message": f"任务 {task_id} 已取消。"}

    async def _task_status_command(self, event):
        context = self._event_ctx(event)
        target = DeliveryTarget.from_dict(context)
        allowed, message = self._check_access(context["user_id"], context["group_id"], check_quota=False)
        if not allowed:
            await self.delivery.send_text(target, message)
            return
        try:
            journal = await self._load_task_journal()
            requested = re.sub(r"^/?wf状态(?:\s|$)", "", self._extract_text_from_event(event)).strip()
            records = [r for r in journal.records() if (not requested or r["task_id"] == requested or r["remote_task_id"] == requested)
                       and (MediaStore.owner(r) == MediaStore.owner(context) or self._is_admin(context["user_id"]))][:10]
            labels = {"queued": "排队中", "submitting": "正在提交", "unknown_submission": "提交未确认，需核对",
                      "pending": "等待平台结果", "tracking_paused": "查询暂不可用", "needs_attention": "需处理配置或平台状态",
                      "success": "生成成功", "failed": "生成失败", "cancelled": "已取消"}
            lines = []
            for record in records:
                lines.append(f"{record['task_id']} · {record['workflow']} · {labels[record['status']]}"
                             + (f" · 平台 {record['remote_task_id']}" if record["remote_task_id"] else ""))
                if record["message"]:
                    lines.append(record["message"])
                if record["status"] in {"tracking_paused", "needs_attention", "pending"} and record["remote_task_id"]:
                    self._schedule_job(record)
                elif record["status"] == "queued":
                    self._schedule_job(record)
                elif record["status"] == "success":
                    lines.append(f"结果发送：{record['delivery_status']}；补发用 /wf补发 {record['task_id']}")
            await self._limiter.resize(self.config.generation.max_concurrent)
            await self.delivery.send_text(target, "\n".join(lines) or "当前没有可查看的任务。")
        except (OSError, ValueError) as exc:
            await self.delivery.send_text(target, f"无法读取任务日志：{exc}")

    async def _reconcile_task_command(self, event):
        context = self._event_ctx(event)
        target = DeliveryTarget.from_dict(context)
        if not self._is_admin(context["user_id"]):
            await self.delivery.send_text(target, "仅插件管理员可核对提交未确认的任务。")
            return
        tokens = self._extract_text_from_event(event).split()
        if len(tokens) != 3:
            await self.delivery.send_text(target, "用法：/wf核对 本地任务ID 平台任务ID；确认平台没有创建任务后填 未创建。")
            return
        try:
            async with self._request_lock:
                journal = await self._load_task_journal()
                record = journal.get(tokens[1])
                if not record or record["status"] != "unknown_submission":
                    raise ValueError("只有提交未确认的任务需要核对。")
                if tokens[2] == "未创建":
                    await journal.update(record["task_id"], status="cancelled", submitted_at=0,
                                         message="管理员已确认平台未创建任务", expected_statuses={"unknown_submission"})
                    self._release_terminal_inputs(record["task_id"])
                else:
                    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", tokens[2]):
                        raise ValueError("平台任务 ID 格式不正确。")
                    if any(r["remote_task_id"] == tokens[2] and r["region"] == record["region"] for r in journal.records()):
                        raise ValueError("此平台任务已绑定其他记录，不能重复绑定。")
                    await journal.update(record["task_id"], status="pending", remote_task_id=tokens[2], message="管理员已核对平台编号",
                                         expected_statuses={"unknown_submission"})
                    self._schedule_job(journal.get(record["task_id"]))
            await self._limiter.resize(self.config.generation.max_concurrent)
            await self.delivery.send_text(target, "已保存核对结果，未重新生成任务。")
        except (OSError, ValueError) as exc:
            await self.delivery.send_text(target, str(exc))
