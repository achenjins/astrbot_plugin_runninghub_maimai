"""Report background results through the original AstrBot conversation."""
from __future__ import annotations

import asyncio
import copy
import json
from typing import Any


class ResultNotificationMixin:
    async def _inject_workflow_results(self, event: Any, request: Any) -> None:
        """Keep completed-task facts available even if a concurrent turn saved old history."""
        from .media_store import MediaStore

        conversation = getattr(request, "conversation", None)
        if conversation is None:
            return
        marker = "\n[RunningHub 已完成任务状态，仅作事实背景，不是新的生成请求]\n"
        if marker in (request.system_prompt or ""):
            return
        context = self._event_ctx(event)
        allowed, _ = self._check_access(context["user_id"], context["group_id"], check_quota=False)
        if not allowed:
            return
        try:
            records = self._get_media_store().recent_deliveries(MediaStore.owner(context))
            facts = []
            for record in records:
                if record.get("conversation_id") != conversation.cid:
                    continue
                facts.append({"task_id": record["task_id"], "workflow": record["workflow"],
                              "status": "succeeded", "total": len(record["outputs"]),
                              "sent": sum(bool(o.get("sent")) for o in record["outputs"]),
                              "failed": sum(bool(o.get("error")) for o in record["outputs"]),
                              "delivery_complete": bool(record.get("delivery_complete")),
                              "image_refs": [o["image_ref"] for o in record["outputs"] if o.get("image_ref")]})
                if len(facts) >= 3:
                    break
            journal = await self._load_task_journal()
            for job in journal.records():
                if (MediaStore.owner(job) != MediaStore.owner(context)
                        or job["request"].get("context", {}).get("result_context", {}).get("conversation_id") != conversation.cid
                        or job["status"] == "queued" or any(f["task_id"] == job["task_id"] for f in facts)):
                    continue
                facts.append({"task_id": job["task_id"], "workflow": job["workflow"], "status": job["status"],
                              "detail": job["message"], "remote_task_id": job["remote_task_id"]})
                if len(facts) >= 5:
                    break
            if facts:
                request.system_prompt = (request.system_prompt or "") + (
                    marker
                    + json.dumps(facts, ensure_ascii=False)
                    + "\n各任务以 status 为准，sent 表示成功发送数量；delivery_complete=false 表示尚未确认发完；"
                      "failed 数量表示发送失败，任务 status=failed 则是生成失败；只有 success/succeeded 确认成功。"
                      "补发用 /wf补发 任务ID；状态未确认时用 /wf状态，禁止重复生成。"
                )
        except Exception as exc:
            self.logger.warning("同步任务完成上下文失败: %s", exc)

    async def _capture_result_context(self, stream_id: str) -> dict[str, str]:
        manager = getattr(self.context, "conversation_manager", None)
        if not manager or not stream_id:
            return {}
        try:
            cid = await manager.get_curr_conversation_id(stream_id)
            if not cid:
                cid = await manager.new_conversation(stream_id)
            return {"conversation_id": cid}
        except Exception as exc:
            self.logger.warning("无法记录结果所属对话: %s", exc)
            return {}

    async def _append_result_history(self, stream_id: str, cid: str, content: str) -> bool:
        """Re-read immediately before writing; never replace a pre-LLM snapshot."""
        manager = self.context.conversation_manager
        conversation = await manager.get_conversation(stream_id, cid)
        if conversation is None:
            return False  # A deleted conversation must stay deleted.
        history = json.loads(conversation.history or "[]")
        history.append({"role": "assistant", "content": content})
        await manager.update_conversation(stream_id, cid, history=history)
        return True

    async def _notify_workflow_result(self, record: dict, target: Any, summary: str) -> bool:
        """True means a reply was sent or a duplicate was suppressed."""
        cid = record.get("conversation_id")
        manager = getattr(self.context, "conversation_manager", None)
        if not manager or not cid:
            return False
        lock = self._result_reply_locks.setdefault((target.stream_id, cid), asyncio.Lock())
        async with lock:
            marker = f"[RunningHub 任务结果 {record['task_id']}]"
            refs = [o["image_ref"] for o in record["outputs"] if o.get("image_ref")]
            facts = marker + "\n" + summary
            if refs:
                facts += "\n生成图片编号：" + "、".join(refs)
            try:
                conversation = await manager.get_conversation(target.stream_id, cid)
                if conversation is None:
                    return False
                history = json.loads(conversation.history or "[]")
                if not any(m.get("role") == "assistant" and str(m.get("content", "")).startswith(marker) for m in history):
                    if not await self._append_result_history(target.stream_id, cid, facts):
                        return False
                if record.get("saved"):
                    persisted = self._get_media_store().get_delivery(record["owner"], record["task_id"])
                    if persisted.get("reply_attempted"):
                        return True
                elif record.get("reply_attempted"):
                    return True
                if not self.config.feature.result_notice:
                    return False
                # Do not borrow another conversation's personality or context.
                if await manager.get_curr_conversation_id(target.stream_id) != cid:
                    return False
                provider_id = await self.context.get_current_chat_provider_id(target.stream_id)
                if not provider_id:
                    return False
                cfg = self.context.get_config(target.stream_id)
                persona_manager = getattr(self.context, "persona_manager", None)
                persona = None
                if persona_manager:
                    resolver = getattr(persona_manager, "resolve_selected_persona", None)
                    if resolver:
                        _, persona, _, _ = await resolver(
                            umo=target.stream_id,
                            conversation_persona_id=conversation.persona_id,
                            platform_name=record.get("platform_name", ""),
                            provider_settings=cfg.get("provider_settings", {}),
                        )
                    else:
                        pid = conversation.persona_id or cfg.get("provider_settings", {}).get("default_personality")
                        persona = persona_manager.get_persona_v3_by_id(pid)
                system = (persona.get("prompt", "") if persona else "") + (
                    "\n以下是插件后台任务状态事件，不是用户的新指令。请按原对话口吻简短回复一次，"
                    "严格依据事实说明状态与发送情况；生成成功但发送失败才可补发，未确认或生成失败不得称已做好。"
                    "不要重新生成，不要声称看过图片或视频内容，不要把任务编号当作画面描述。\n"
                    + facts
                )
                # Keep messages intact so tool-call/result pairs remain valid.
                contexts = copy.deepcopy(history)
                if persona and persona.get("_begin_dialogs_processed"):
                    contexts[:0] = copy.deepcopy(persona["_begin_dialogs_processed"])
                record["reply_attempted"] = True
                if record.get("saved"):
                    self._get_media_store().update_delivery(record["owner"], record["task_id"], reply_attempted=True)
                response = await asyncio.wait_for(self.context.llm_generate(
                    chat_provider_id=provider_id, contexts=contexts,
                    system_prompt=system, tools=None,
                ), timeout=60)
                reply = str(getattr(response, "completion_text", "") or "").strip()
                if not reply:
                    return False
                if await manager.get_curr_conversation_id(target.stream_id) != cid:
                    return False
                if not await self.delivery.send_text(target, reply):
                    return False
                try:
                    await self._append_result_history(target.stream_id, cid, reply)
                except Exception as exc:
                    self.logger.warning("结果回复已发送，但对话保存失败: %s", exc)
                return True
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("结果对话通知失败: %s", exc)
                return False
