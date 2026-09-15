"""Send and resend completed outputs without creating another generation task."""
from __future__ import annotations

import asyncio
import base64
import re
from typing import Any

from .delivery import DeliveryReceipt, DeliveryTarget
from .media_store import MAX_IMAGE_BYTES, MediaStore
from .file_source import trusted_local_file, decode_base64_bounded


class ResultDeliveryMixin:
    def _result_record(self, job):
        context = job.get("request", {}).get("context", {})
        record = {**context.get("result_context", {}), "owner": MediaStore.owner(job),
                  "task_id": job["task_id"], "journal_id": job["task_id"], "workflow": job["workflow"],
                  "region": job["region"], "outputs": job["outputs"], "created_at": job["created_at"],
                  "platform_name": job["platform_name"], "saved": False}
        try:
            self._get_media_store().get_delivery(record["owner"], record["task_id"])
            record["saved"] = True
        except ValueError:
            pass
        return record

    async def _recent_result_records(self, owner):
        journal = await self._load_task_journal()
        records = {r["task_id"]: r for r in self._get_media_store().recent_deliveries(owner)}
        for job in journal.records():
            if MediaStore.owner(job) == owner and job["status"] == "success" and job["outputs"]:
                records[job["task_id"]] = self._result_record(job)
        return sorted(records.values(), key=lambda r: r.get("created_at", 0), reverse=True)

    @staticmethod
    def _result_indices(record, selection=""):
        total = len(record["outputs"])
        if selection in ("全部", "all"):
            return list(range(total))
        if selection:
            parts = re.split(r"[,，、]+", selection)
            if not all(p.isdecimal() and 1 <= int(p) <= total for p in parts):
                raise ValueError(f"结果序号应为 1–{total}，例如：1,2")
            return list(dict.fromkeys(int(p) - 1 for p in parts))
        return [i for i, output in enumerate(record["outputs"]) if not output.get("sent")] or list(range(total))

    def _update_output(self, record: dict, index: int, **changes: Any) -> None:
        record["outputs"][index].update(changes)
        if record.get("saved"):
            try:
                self._get_media_store().update_delivery_output(record["owner"], record["task_id"], index, **changes)
            except Exception as exc:
                self.logger.warning("更新结果发送记录失败: %s", exc)

    async def _send_saved_output(self, record: dict, index: int, target: DeliveryTarget, client: Any) -> tuple[DeliveryReceipt, str]:
        output = record["outputs"][index]
        url, output_type = output["url"], output["type"]
        task_id = record["task_id"]
        context = {"user_id": target.user_id, "group_id": target.group_id, "platform_id": target.platform_id}
        recall = bool(self.config.feature.enable and self.config.feature.recall_seconds > 0)
        if self._is_image_url(url, output_type):
            data64 = ""
            ref = output.get("image_ref", "")
            try:
                if ref:
                    source = self._get_media_store().get(record["owner"], ref)["source"]
                    if not source.startswith(("https://", "http://")):
                        path = trusted_local_file(source, self._trusted_file_roots())
                        if path and path.stat().st_size <= MAX_IMAGE_BYTES:
                            data64 = base64.b64encode(await asyncio.to_thread(path.read_bytes)).decode("ascii")
            except (OSError, ValueError):
                pass
            if not data64:
                try:
                    data64 = await client.download_base64(url)
                except Exception as exc:
                    self.logger.warning("任务 %s 第 %d 项下载失败: %s", task_id, index + 1, exc)
                    return DeliveryReceipt(False), "下载失败（链接可能过期）"
            try:
                ref = self._remember_generated_image(task_id, index + 1, url, target.stream_id, context, data64)
                if ref:
                    self._update_output(record, index, image_ref=ref)
            except Exception as exc:
                self.logger.warning("缓存生成结果失败: %s", exc)
            receipt = await self.delivery.send_image_result(target, data64, need_message_id=recall)
            if receipt.success and ref:
                try:
                    await self._remember_image_usage(record["owner"], ref, decode_base64_bounded(data64, MAX_IMAGE_BYTES))
                except Exception as exc:
                    self.logger.warning("图片已发送，但最近图片保存失败: %s", exc)
        elif self._is_video_url(url, output_type):
            receipt = await self.delivery.send_video_result(target, url, need_message_id=recall)
        else:
            sent = await self.delivery.send_text(target, f"任务 {task_id} 结果 {index + 1}：{url}")
            receipt = DeliveryReceipt(sent, as_link=sent)
        if receipt.success and recall and receipt.message_id:
            self._schedule_recall(receipt.message_id, self.config.feature.recall_seconds, platform_id=target.platform_id)
        return receipt, "" if receipt.success else "发送未成功，请检查 QQ 连接后补发"

    async def _deliver_saved_results(self, record: dict, target: DeliveryTarget, client: Any,
                                     indices: list[int] | None = None, *, resend: bool = False, announce: bool = True):
        key = (record["owner"], record["task_id"])
        lock = self._result_send_locks.setdefault(key, asyncio.Lock())
        if lock.locked():
            message = "该任务的结果正在发送，请稍候，不必重复补发。"
            if announce:
                await self.delivery.send_text(target, message)
            return {"success": False, "message": message}
        failures = []
        sent = 0
        indices = list(range(len(record["outputs"]))) if indices is None else indices
        try:
            async with lock:
                journal = await self._load_task_journal() if record.get("journal_id") else None
                if journal:
                    record.update(self._result_record(journal.get(record["journal_id"])))
                elif record.get("saved"):
                    try:
                        record.update(self._get_media_store().get_delivery(record["owner"], record["task_id"]))
                    except ValueError:
                        pass
                if not resend:
                    indices = [i for i in indices if not record["outputs"][i].get("sent")]
                if any(type(i) is not int or i < 0 or i >= len(record["outputs"]) for i in indices):
                    raise ValueError("结果序号超出范围")
                if journal:
                    await journal.update(record["journal_id"], delivery_status="sending", stage="delivering")
                for index in indices:
                    try:
                        receipt, error = await self._send_saved_output(record, index, target, client)
                    except Exception as exc:
                        self.logger.exception("结果发送异常: %s", exc)
                        receipt, error = DeliveryReceipt(False), "发送异常"
                    self._update_output(record, index, sent=receipt.success, error=error)
                    if journal:
                        await journal.update(record["journal_id"], outputs=record["outputs"],
                                             delivered_indexes=[i for i, o in enumerate(record["outputs"]) if o.get("sent")])
                    if receipt.success:
                        sent += 1
                    else:
                        failures.append(f"第 {index + 1} 项：{error}")
                record["delivery_complete"] = True
                if journal:
                    await journal.update(record["journal_id"], delivery_status="sent" if all(o.get("sent") for o in record["outputs"]) else "partial", stage="finished")
                if record.get("saved"):
                    try:
                        self._get_media_store().update_delivery(record["owner"], record["task_id"], delivery_complete=True)
                    except Exception as exc:
                        self.logger.warning("结果发送完成，但状态保存失败: %s", exc)
                if failures:
                    prefix = f"任务 {record['task_id']} 已生成成功，本次已发送 {sent}/{len(indices)} 项。"
                    action = f"回复 /wf补发 {record['task_id']} 补发未成功项，不会重新生成。" if record.get("saved") or journal else "补发记录未能保存，请到 RunningHub 任务页面下载结果。"
                    summary = "\n".join([prefix, *failures, action])
                elif resend:
                    summary = f"任务 {record['task_id']} 已补发 {sent} 项，未重新生成。"
                else:
                    summary = f"工作流「{record['workflow']}」任务 {record['task_id']} 生成完成，已发送 {sent} 项。"
                if resend:
                    if announce:
                        await self.delivery.send_text(target, summary)
                else:
                    if journal:
                        if not await journal.claim_notification(record["journal_id"], "final_result"):
                            return
                    elif record.get("final_notice_attempted"):
                        return
                    record["final_notice_attempted"] = True
                    if record.get("saved"):
                        self._get_media_store().update_delivery(record["owner"], record["task_id"], final_notice_attempted=True)
                    notified = await self._notify_workflow_result(record, target, summary)
                    if not notified and (failures or self.config.feature.result_notice):
                        await self.delivery.send_text(target, summary)
                return {"success": not failures, "message": summary, "sent": sent, "requested": len(indices)}
        finally:
            self._result_send_locks.pop(key, None)

    async def _resend_result_command(self, event: Any) -> None:
        context = self._event_ctx(event)
        target = DeliveryTarget.from_event(event)
        allowed, message = self._check_access(context["user_id"], context["group_id"], check_quota=False)
        if not allowed:
            await self.delivery.send_text(target, message)
            return
        text = re.sub(r"^/?wf补发(?:\s|$)", "", self._extract_text_from_event(event).strip()).strip()
        tokens = text.split()
        if len(tokens) > 2:
            await self.delivery.send_text(target, "用法：/wf补发 [任务ID或最新] [结果序号或全部]；查看记录：/wf补发 列表")
            return
        try:
            store = self._get_media_store()
            owner = MediaStore.owner(context)
            recent = await self._recent_result_records(owner)
            if not recent:
                raise ValueError("本会话还没有可补发的生成结果，或记录已过期；补发不会重新生成任务。")
            if tokens and tokens[0] == "列表":
                lines = ["本会话近期结果（由新到旧）："]
                for entry in recent:
                    failed = sum(not o.get("sent") for o in entry["outputs"])
                    lines.append(f"{entry['task_id']} · {entry['workflow']} · {len(entry['outputs'])} 项，未发出 {failed} 项")
                lines.append("用法：/wf补发 任务ID；仅发第2项：/wf补发 任务ID 2")
                await self.delivery.send_text(target, "\n".join(lines))
                return
            task_id = recent[0]["task_id"] if not tokens or tokens[0] == "最新" else tokens[0]
            record = next((r for r in recent if r["task_id"] == task_id), None)
            if record is None:
                raise ValueError("本会话没有此任务的可补发结果")
            selection = tokens[1] if len(tokens) > 1 else ""
            indices = self._result_indices(record, selection)
            if record.get("journal_id"):
                result = await self._perform_task_action(record["journal_id"], "resend", selection, owner=owner,
                    request_key=f"chat:{owner}:{context['anchor_id']}" if context.get("anchor_id") else "")
                await self.delivery.send_text(target, result["message"])
                return
            client = self._get_client(record["region"])
            if client is None:
                self._rebuild_client()
                client = self._get_client(record["region"])
            if client is None:
                raise ValueError("结果下载客户端不可用，请重载插件后补发。")
            await self._deliver_saved_results(record, target, client, indices, resend=True)
        except (OSError, ValueError) as exc:
            await self.delivery.send_text(target, str(exc))
