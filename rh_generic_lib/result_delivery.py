"""Send and resend completed outputs without creating another generation task."""
from __future__ import annotations

import asyncio
import base64
import re
from pathlib import Path
from typing import Any

from .delivery import DeliveryReceipt, DeliveryTarget
from .media_store import MAX_IMAGE_BYTES, MediaStore


class ResultDeliveryMixin:
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
                        path = Path(source)
                        if path.stat().st_size <= MAX_IMAGE_BYTES:
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
        elif self._is_video_url(url, output_type):
            receipt = await self.delivery.send_video_result(target, url, need_message_id=recall)
        else:
            sent = await self.delivery.send_text(target, f"任务 {task_id} 结果 {index + 1}：{url}")
            receipt = DeliveryReceipt(sent, as_link=sent)
        if receipt.success and recall and receipt.message_id:
            self._schedule_recall(receipt.message_id, self.config.feature.recall_seconds, platform_id=target.platform_id)
        return receipt, "" if receipt.success else "发送未成功，请检查 QQ 连接后补发"

    async def _deliver_saved_results(self, record: dict, target: DeliveryTarget, client: Any,
                                     indices: list[int] | None = None, *, resend: bool = False) -> None:
        key = (record["owner"], record["task_id"])
        lock = self._result_send_locks.setdefault(key, asyncio.Lock())
        if lock.locked():
            await self.delivery.send_text(target, "该任务的结果正在发送，请稍候，不必重复补发。")
            return
        failures = []
        sent = 0
        indices = list(range(len(record["outputs"]))) if indices is None else indices
        try:
            async with lock:
                for index in indices:
                    try:
                        receipt, error = await self._send_saved_output(record, index, target, client)
                    except Exception as exc:
                        self.logger.exception("结果发送异常: %s", exc)
                        receipt, error = DeliveryReceipt(False), "发送异常"
                    self._update_output(record, index, sent=receipt.success, error=error)
                    if receipt.success:
                        sent += 1
                    else:
                        failures.append(f"第 {index + 1} 项：{error}")
                if failures:
                    prefix = f"任务 {record['task_id']} 已生成成功，本次已发送 {sent}/{len(indices)} 项。"
                    action = f"回复 /wf补发 {record['task_id']} 补发未成功项，不会重新生成。" if record.get("saved") else "补发记录未能保存，请到 RunningHub 任务页面下载结果。"
                    await self.delivery.send_text(target, "\n".join([prefix, *failures, action]))
                elif resend:
                    await self.delivery.send_text(target, f"任务 {record['task_id']} 已补发 {sent} 项，未重新生成。")
                elif self.config.feature.result_notice:
                    await self.delivery.send_text(target, f"任务 {record['task_id']} 生成完成，已发送 {sent} 项。")
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
            recent = store.recent_deliveries(owner)
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
            record = store.get_delivery(owner, task_id)
            selection = tokens[1] if len(tokens) > 1 else ""
            if selection == "全部":
                indices = list(range(len(record["outputs"])))
            elif selection:
                parts = re.split(r"[,，、]+", selection)
                if not all(p.isdecimal() and 1 <= int(p) <= len(record["outputs"]) for p in parts):
                    raise ValueError(f"结果序号应为 1–{len(record['outputs'])}，例如：/wf补发 {task_id} 1,2")
                indices = list(dict.fromkeys(int(p) - 1 for p in parts))
            else:
                indices = [i for i, output in enumerate(record["outputs"]) if not output.get("sent")]
                indices = indices or list(range(len(record["outputs"])))
            client = self._get_client(record["region"])
            if client is None:
                self._rebuild_client()
                client = self._get_client(record["region"])
            if client is None:
                raise ValueError("结果下载客户端不可用，请重载插件后补发。")
            await self._deliver_saved_results(record, target, client, indices, resend=True)
        except (OSError, ValueError) as exc:
            await self.delivery.send_text(target, str(exc))
