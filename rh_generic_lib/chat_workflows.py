"""Natural-language workflow orchestration, independent of command parsing."""
from __future__ import annotations

import asyncio
import base64
import json
import time
from pathlib import Path
from typing import Any

from astrbot.api.message_components import Image, Reply

from .media_store import MAX_IMAGE_BYTES, MediaStore
from .validation import parameter_value


def node_key(node: Any) -> str:
    return f"{node.node_id}/{node.field_name}"




class ChatWorkflowMixin:
    def _get_media_store(self) -> MediaStore:
        if self._media_store is None:
            self._media_store = MediaStore(self._prompt_library_path().parent / "chat_media",
                                           ttl=self.config.feature.media_history_minutes * 60)
        self._media_store.ttl = self.config.feature.media_history_minutes * 60
        return self._media_store

    @staticmethod
    def _image_source(file: Any, url: Any = "") -> str:
        for value in (file, url):
            source = str(value or "").strip()
            if source.startswith(("https://", "http://", "base64://")):
                return source
            if source.startswith("file:///"):
                # file:///C:/... on Windows, file:///tmp/... on Linux.
                source = source[8:] if len(source) > 9 and source[9] == ":" else source[7:]
            try:
                if source and Path(source).is_file():
                    return source
            except OSError:
                pass
        return ""

    async def _remember_event_images(self, event: Any, *, include_reply: bool = True) -> dict[str, list[str]]:
        context = self._event_ctx(event)
        owner = MediaStore.owner(context)
        if not owner:
            return {"current": [], "reply": []}
        store = self._get_media_store()
        message_id = str(getattr(getattr(event, "message_obj", None), "message_id", "") or "")
        result = {"current": [], "reply": []}
        components = event.get_messages()
        for origin in ("current", "reply"):
            if origin == "reply" and not include_reply:
                continue
            cached = event.get_extra(f"rh_images_{origin}", None)
            if cached is not None:
                result[origin] = cached
                continue
            sources: list[tuple[str, str, int]] = []
            if origin == "current":
                for comp in components:
                    if isinstance(comp, Image):
                        source = self._image_source(comp.file, comp.url)
                        if source:
                            sources.append((source, message_id, len(sources) + 1))
            else:
                for comp in components:
                    if not isinstance(comp, Reply):
                        continue
                    chain = comp.chain or []
                    for item in chain:
                        if isinstance(item, Image):
                            source = self._image_source(item.file, item.url)
                            if source:
                                sources.append((source, str(comp.id), len(sources) + 1))
                    if not chain:
                        # AstrBot normally expands Reply.chain. Older adapters or
                        # a failed first lookup can leave only the message ID.
                        bot = self.delivery.get_onebot_client(self._delivery_target(event))
                        if bot is None:
                            continue
                        try:
                            raw = await asyncio.wait_for(bot.call_action("get_msg", message_id=int(comp.id)), 12)
                            if isinstance(raw, dict) and isinstance(raw.get("data"), dict):
                                raw = raw["data"]
                            if not isinstance(raw, dict):
                                continue
                            group = str(raw.get("group_id") or "")
                            if group and group != context["group_id"]:
                                continue
                            for segment in raw.get("message") or []:
                                if isinstance(segment, dict) and segment.get("type") == "image":
                                    data = segment.get("data") or {}
                                    source = self._image_source(data.get("file"), data.get("url"))
                                    if source:
                                        sources.append((source, str(comp.id), len(sources) + 1))
                        except Exception as exc:
                            self.logger.debug("引用图片读取失败: %s", exc)
            for source, mid, position in sources[:32]:
                try:
                    entry = store.remember(owner, source, message_id=mid, position=position,
                                           origin="upload" if origin == "current" else "reply")
                    result[origin].append(entry["ref"])
                except (OSError, ValueError) as exc:
                    self.logger.warning("记录聊天图片失败: %s", exc)
            event.set_extra(f"rh_images_{origin}", result[origin])
        return result

    def _delivery_target(self, event: Any):
        # Keep imports inside the plugin package namespace during reloads.
        from .delivery import DeliveryTarget
        return DeliveryTarget.from_event(event)

    async def _observe_chat_images(self, event: Any) -> None:
        if not any(isinstance(comp, Image) for comp in event.get_messages()):
            return
        context = self._event_ctx(event)
        allowed, _ = self._check_access(context["user_id"], context["group_id"], check_quota=False)
        if allowed:
            try:
                await self._remember_event_images(event, include_reply=False)
            except Exception as exc:
                # Observing an ordinary message must never consume or break it.
                self.logger.warning("图片记录不可用: %s", exc)

    def _natural_workflow_catalog(self) -> list[dict[str, Any]]:
        catalog = []
        for wf in self._workflows:
            if not self._is_llm_callable_workflow(wf):
                continue
            inputs = []
            for node in self._ordered_nodes(wf):
                kind = self._resolve_value_type(node)
                if kind == "default":
                    continue
                item = {"key": node_key(node), "role": node.label or node.field_name,
                        "type": kind, "required": node.required, "has_default": bool(node.field_value)}
                if kind == "text":
                    item.update(default=node.field_value[:512], param_type=node.param_type,
                                minimum=node.minimum, maximum=node.maximum, choices=node.choices)
                inputs.append(item)
            catalog.append(dict(name=wf.name, description=wf.description, inputs=inputs))
        return catalog

    async def _natural_context(self, event: Any) -> str:
        context = self._event_ctx(event)
        allowed, message = self._check_access(context["user_id"], context["group_id"], check_quota=False)
        if not allowed:
            return json.dumps({"success": False, "message": message}, ensure_ascii=False)
        refs = await self._remember_event_images(event)
        store = self._get_media_store()
        owner = store.owner(context)
        images = []
        for entry in store.recent(owner):
            ref = entry["ref"]
            where = "current" if ref in refs["current"] else "reply" if ref in refs["reply"] else "recent"
            images.append({"ref": ref, "source": where, "origin": entry["origin"],
                           "position": entry["position"], "task_id": entry["task_id"],
                           "age_seconds": max(0, int(time.time() - entry["created_at"]))})
        runs = [{"task_id": run["task_id"], "workflow": run["workflow"],
                 "prompt_summary": run.get("prompt", "")[:500],
                 "parameters": {k: str(v)[:512] for k, v in run.get("parameters", {}).items()},
                 "image_bindings": run.get("image_bindings", {})}
                for run in store.recent_runs(owner)[:5]]
        return json.dumps({"success": True, "workflows": self._natural_workflow_catalog(),
                           "images": images, "recent_runs": runs,
                           "instructions": "图片编号仅代表素材，未提供画面描述。按 current/reply/时间/position 选择；多图用途不明确时询问。"
                           "编辑生成结果选 origin=generated 的图片；沿用原输入和参数使用 reuse_task_id。只提交一次。"}, ensure_ascii=False)

    @staticmethod
    def _match_node(nodes: list[Any], name: str) -> Any:
        exact = [n for n in nodes if node_key(n) == name]
        matches = exact or [n for n in nodes if name in {n.label, n.field_name}]
        if len(matches) != 1:
            raise ValueError(f"输入「{name}」不存在或有重名，请使用工作流提供的 节点ID/字段名")
        return matches[0]

    async def _materialize_image(self, store: MediaStore, owner: str, ref: str, client: Any) -> tuple[bytes, str]:
        entry = store.get(owner, ref)
        source = entry["source"]
        if source.startswith(("https://", "http://")):
            data = await client.download_bytes(source, max_bytes=MAX_IMAGE_BYTES)
        else:
            path = Path(source)
            if path.stat().st_size > MAX_IMAGE_BYTES:
                raise ValueError("图片超过 20MB，请压缩后重新发送")
            data = await asyncio.to_thread(path.read_bytes)
        if not data or len(data) > MAX_IMAGE_BYTES:
            raise ValueError("图片为空或超过 20MB，请重新发送")
        store.cache(owner, ref, data)
        return data, self._guess_filename(source, "image", data)

    async def _run_natural_workflow(self, event: Any, workflow_name: str, prompt: str = "",
                                    image_refs: list[str] | None = None,
                                    image_bindings: dict[str, str] | None = None,
                                    parameters: dict[str, Any] | None = None,
                                    reuse_task_id: str = "") -> str:
        context = self._event_ctx(event)
        # Protect against repeated tool calls in one model turn (including a
        # second call while awaiting inputs). A new user event can start a new run.
        previous = event.get_extra("rh_natural_result", None)
        if previous is not None:
            return previous
        allowed, message = self._check_access(context["user_id"], context["group_id"])
        if not allowed:
            return "错误：" + message
        if event.get_extra("rh_natural_busy", False):
            return "本轮工作流正在准备，请勿重复提交。"
        event.set_extra("rh_natural_busy", True)
        try:
            response = await self._prepare_natural_run(event, context, workflow_name, prompt,
                                                       image_refs, image_bindings, parameters, reuse_task_id)
            if response.get("success"):
                if response.get("waiting"):
                    text = "尚未提交任务：" + response["message"] + "。请向用户说明需要补充的内容，然后结束本轮工具调用。"
                else:
                    text = "任务已提交，task_id=" + str(response.get("task_id") or "") + "。结果会自动发送，请勿重复调用或等待轮询。"
                event.set_extra("rh_natural_result", text)
                return text
            if response.get("submission_attempted"):
                text = "提交请求失败或未获确认，本轮不会自动重试，请核对 RunningHub 任务后再操作。" + response["message"]
                event.set_extra("rh_natural_result", text)
                return text
            return "未提交任务：" + response["message"]
        except asyncio.CancelledError:
            event.set_extra("rh_natural_result", "本轮工作流处理已中断，请先核对任务状态，勿自动重复提交。")
            raise
        except (ValueError, OSError) as exc:
            return "未提交任务：" + str(exc)
        except Exception as exc:
            self.logger.exception("自然语言任务准备失败")
            return "任务准备失败，请检查图片是否仍可下载或重新发送图片。详细原因已记录日志。"
        finally:
            event.set_extra("rh_natural_busy", False)

    async def _prepare_natural_run(self, event: Any, context: dict[str, str], workflow_name: str,
                                   prompt: str, image_refs: Any, image_bindings: Any,
                                   parameters: Any, reuse_task_id: str) -> dict[str, Any]:
        refs = await self._remember_event_images(event)
        store = self._get_media_store()
        owner = store.owner(context)
        if not owner:
            raise ValueError("无法识别当前用户或会话")
        if image_refs is not None and (not isinstance(image_refs, list) or any(not isinstance(r, str) for r in image_refs)):
            raise ValueError("image_refs 必须是图片编号数组")
        if image_bindings is not None and not isinstance(image_bindings, dict):
            raise ValueError("image_bindings 必须是输入名到图片编号的对象")
        if parameters is not None and not isinstance(parameters, dict):
            raise ValueError("parameters 必须是参数名到值的对象")
        if image_refs and image_bindings:
            raise ValueError("图片按顺序传入或按角色绑定请选择一种方式")
        parameters = dict(parameters or {})
        bindings = dict(image_bindings or {})
        inherited = None
        if reuse_task_id:
            inherited = store.get_run(owner, reuse_task_id)
            workflow_name = workflow_name or inherited["workflow"]
        wf = self._find_workflow(str(workflow_name or "").strip())
        if wf is None or not self._is_llm_callable_workflow(wf):
            raise ValueError("该工作流不存在或未开启自然语言调用，请先调用 get_workflow_context 选择可用工作流")
        if inherited:
            if inherited["workflow_id"] != wf.workflow_id or inherited["region"] != wf.region:
                raise ValueError("原任务与当前工作流不匹配，请重新选择图片和参数")
            parameters = {**inherited.get("parameters", {}), **parameters}
            if image_refs is None and image_bindings is None:
                bindings = dict(inherited.get("image_bindings", {}))
            prompt = ((inherited.get("prompt") or "") + "\n本次修改要求：" + prompt).strip() if prompt else inherited.get("prompt", "")
        nodes = self._ordered_nodes(wf)
        image_nodes = [n for n in nodes if self._resolve_value_type(n) == "image"]
        text_nodes = [n for n in nodes if self._resolve_value_type(n) == "text"]
        overrides: dict[str, str] = {}
        for name, value in parameters.items():
            node = self._match_node(text_nodes, str(name))
            value = parameter_value(node, value)
            overrides[node_key(node)] = value
        canonical_bindings = {}
        for name, ref in bindings.items():
            node = self._match_node(image_nodes, str(name))
            if not isinstance(ref, str):
                raise ValueError("图片绑定值必须是图片编号")
            canonical_bindings[node_key(node)] = ref
        if image_refs is None and image_bindings is None and not inherited and image_nodes:
            if refs["current"] and refs["reply"]:
                raise ValueError("当前消息和引用消息都有图片，请先查看图片编号并明确选择")
            image_refs = refs["current"] or refs["reply"]
        if image_refs:
            if len(image_refs) > len(image_nodes):
                raise ValueError(f"工作流最多接收 {len(image_nodes)} 张图片，请明确选择需要的图片")
            if len(image_nodes) > 1:
                raise ValueError("多图工作流请先查看各图片输入用途，使用 image_bindings 明确绑定角色")
            canonical_bindings = {node_key(n): ref for n, ref in zip(image_nodes, image_refs)}
        # Validate every reference and parameter before the first network/upload.
        for ref in canonical_bindings.values():
            store.get(owner, ref)
        for node in text_nodes:
            value = overrides.get(node_key(node), node.field_value)
            if value:
                overrides[node_key(node)] = parameter_value(node, value)
        client = self._get_client(wf.region)
        if client is None:
            self._rebuild_client()
            client = self._get_client(wf.region)
        region_key = self.config.server.api_key_cn if wf.region == "domestic" else self.config.server.api_key
        if client is None or not region_key:
            raise ValueError("工作流所在区域的 RunningHub API Key 未配置")
        uploaded: dict[str, str] = {}
        for key, ref in canonical_bindings.items():
            if ref not in uploaded:
                data, filename = await self._materialize_image(store, owner, ref, client)
                uploaded[ref] = await client.upload_file(data, filename)
            overrides[key] = uploaded[ref]
        context.update(trigger="tool", natural=True, node_overrides=overrides,
                       image_bindings=canonical_bindings)
        return await self._start_workflow(wf.name, str(prompt or ""), **context)

    def _remember_workflow_run(self, task_id: str, workflow: Any, node_info: list[dict[str, str]],
                               stream_id: str, context: dict[str, Any]) -> None:
        owner = MediaStore.owner({**context, "stream_id": stream_id})
        if not owner:
            return
        values = {f"{n['nodeId']}/{n['fieldName']}": n["fieldValue"] for n in node_info}
        params = {node_key(n): values[node_key(n)] for n in self._ordered_nodes(workflow)
                  if self._resolve_value_type(n) == "text" and node_key(n) in values}
        prompt = next((values.get(node_key(n), "") for n in self._ordered_nodes(workflow)
                       if self._resolve_value_type(n) == "prompt"), "")
        self._get_media_store().remember_run(owner, task_id, workflow=workflow.name,
                                            workflow_id=workflow.workflow_id, region=workflow.region,
                                            prompt=prompt[:16000], parameters=params,
                                            image_bindings=context.get("image_bindings", {}))

    def _remember_generated_image(self, task_id: str, position: int, url: str,
                                   stream_id: str, context: dict[str, Any], image_base64: str = "") -> str:
        owner = MediaStore.owner({**context, "stream_id": stream_id})
        if not owner:
            return
        data = None
        if image_base64 and len(image_base64) <= (MAX_IMAGE_BYTES + 2) // 3 * 4:
            data = base64.b64decode(image_base64, validate=True)
        entry = self._get_media_store().remember(owner, url, origin="generated", task_id=task_id,
                                                   position=position, data=data)
        return entry["ref"]
