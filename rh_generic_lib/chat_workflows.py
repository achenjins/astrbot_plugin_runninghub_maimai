"""Natural-language workflow orchestration, independent of command parsing."""
from __future__ import annotations

import asyncio
import base64
import json
import time
from typing import Any

from astrbot.api.message_components import File, Image, Reply

from .media_store import MAX_IMAGE_BYTES, MediaStore
from .validation import parameter_value
from .file_source import trusted_local_file


def node_key(node: Any) -> str:
    return f"{node.node_id}/{node.field_name}"




class ChatWorkflowMixin:
    def _get_media_store(self) -> MediaStore:
        if self._media_store is None:
            self._media_store = MediaStore(self._prompt_library_path().parent / "chat_media",
                                           ttl=self.config.feature.media_history_minutes * 60)
        self._media_store.ttl = self.config.feature.media_history_minutes * 60
        return self._media_store

    def _image_source(self, file: Any, url: Any = "") -> str:
        for value in (file, url):
            source = str(value or "").strip()
            if source.startswith(("https://", "http://", "base64://")):
                return source
            if path := trusted_local_file(source, self._trusted_file_roots()):
                return str(path)
        return ""

    def _is_image_component(self, comp):
        return isinstance(comp, Image) or (isinstance(comp, File)
                and self._detect_file_type_from_name(str(comp.name or comp.file or comp.url or "")) == "image")

    async def _remember_event_images(self, event: Any, *, include_reply: bool = True,
                                     observing: bool = False) -> dict[str, list[str]]:
        context = self._event_ctx(event)
        owner = MediaStore.owner(context)
        if not owner:
            return {"current": [], "reply": []}
        store = self._get_media_store()
        message_id = str(getattr(getattr(event, "message_obj", None), "message_id", "") or "")
        result = {"current": [], "reply": []}
        components = event.get_messages()
        deadline = asyncio.get_running_loop().time() + (1.5 if observing else 12)
        for origin in ("current", "reply"):
            if origin == "reply" and not include_reply:
                continue
            cached = event.get_extra(f"rh_images_{origin}", None)
            if cached and event.get_extra(f"rh_images_{origin}_complete", False):
                result[origin] = cached
                continue
            sources: list[tuple[str, str, int]] = []
            complete = True
            if origin == "current":
                position = 0
                for comp in components:
                    if self._is_image_component(comp):
                        position += 1
                        source = self._image_source(comp.file, comp.url)
                        if isinstance(comp, File) and "ftn.qq.com" in source:
                            source = ""
                        if source:
                            sources.append((source, message_id, position))
                        else:
                            complete = False
                raw = getattr(getattr(event, "message_obj", None), "raw_message", None)
                segments = raw.get("message", []) if isinstance(raw, dict) else []
                expected_positions = position
                position = 0
                for segment in segments if isinstance(segments, list) else []:
                    if not isinstance(segment, dict) or not isinstance(segment.get("data"), dict):
                        continue
                    data = segment["data"]
                    if segment.get("type") != "image" and not (segment.get("type") == "file" and self._detect_file_type_from_name(data.get("name", "")) == "image"):
                        continue
                    position += 1
                    source = self._image_source(data.get("file"), data.get("url"))
                    if segment.get("type") == "file" and (not source or "ftn.qq.com" in source):
                        try:
                            remaining = deadline - asyncio.get_running_loop().time()
                            source = await self._group_file_source(data.get("file_id") or data.get("id"), context, timeout=max(0.01, remaining)) if remaining > 0 else ""
                        except Exception as exc:
                            source = ""
                            self.logger.debug("群图片文件读取失败: %s", exc)
                    if source:
                        sources = [item for item in sources if item[2] != position]
                        sources.append((source, message_id, position))
                    else:
                        complete = False
                complete = all(i in {s[2] for s in sources} for i in range(1, max(expected_positions, position) + 1))
            else:
                for comp in components:
                    if not isinstance(comp, Reply):
                        continue
                    chain = comp.chain or []
                    reply_sources = []
                    position = 0
                    unresolved = False
                    for item in chain:
                        if self._is_image_component(item):
                            position += 1
                            source = self._image_source(item.file, item.url)
                            if isinstance(item, File) and "ftn.qq.com" in source:
                                source = ""
                            if source:
                                reply_sources.append((source, str(comp.id), position))
                            else:
                                unresolved = True
                    needs_lookup = not reply_sources or unresolved
                    # Ordinary text quotes need no network lookup. A later tool
                    # call can still inspect an incompletely expanded chain.
                    if observing and chain and not position:
                        complete = False
                        continue
                    if needs_lookup:
                        # AstrBot normally expands Reply.chain. Older adapters or
                        # a failed first lookup can leave only the message ID.
                        bot = self.delivery.get_onebot_client(self._delivery_target(event))
                        if bot is None:
                            complete = False
                            sources.extend(reply_sources)
                            continue
                        try:
                            remaining = deadline - asyncio.get_running_loop().time()
                            if remaining <= 0:
                                raise TimeoutError("引用读取预算已用完")
                            raw = await asyncio.wait_for(bot.call_action("get_msg", message_id=int(comp.id)), remaining)
                            if isinstance(raw, dict) and (raw.get("status") == "failed" or raw.get("retcode", 0) != 0):
                                raise ValueError("get_msg 返回失败")
                            if isinstance(raw, dict) and isinstance(raw.get("data"), dict):
                                raw = raw["data"]
                            if not isinstance(raw, dict) or not isinstance(raw.get("message"), list):
                                raise ValueError("get_msg 未返回消息组件")
                            group = str(raw.get("group_id") or "")
                            if group and group != context["group_id"]:
                                complete = False
                                continue
                            if not context["group_id"] and str(raw.get("sender", {}).get("user_id") or raw.get("user_id") or "") != context["user_id"]:
                                complete = False
                                continue
                            fetched = []
                            position = 0
                            for segment in raw.get("message") or []:
                                if (isinstance(segment, dict) and isinstance(segment.get("data"), dict)
                                        and (segment.get("type") == "image" or (segment.get("type") == "file"
                                        and self._detect_file_type_from_name(segment["data"].get("name", "")) == "image"))):
                                    position += 1
                                    data = segment.get("data") or {}
                                    source = self._image_source(data.get("file"), data.get("url"))
                                    if segment.get("type") == "file" and (not source or "ftn.qq.com" in source):
                                        remaining = deadline - asyncio.get_running_loop().time()
                                        source = await self._group_file_source(data.get("file_id") or data.get("id"), context, timeout=max(0.01, remaining)) if remaining > 0 else ""
                                    if source:
                                        fetched.append((source, str(comp.id), position))
                                    else:
                                        complete = False
                            if fetched:
                                merged = {item[2]: item for item in reply_sources}
                                merged.update({item[2]: item for item in fetched})
                                reply_sources = [merged[pos] for pos in sorted(merged)]
                        except Exception as exc:
                            complete = False
                            self.logger.debug("引用图片读取失败: %s", exc)
                    sources.extend(reply_sources)
            for source, mid, position in sources[:32]:
                try:
                    entry = store.remember(owner, source, message_id=mid, position=position,
                                           origin="upload" if origin == "current" else "reply")
                    result[origin].append(entry["ref"])
                except (OSError, ValueError) as exc:
                    complete = False
                    self.logger.warning("记录聊天图片失败: %s", exc)
            event.set_extra(f"rh_images_{origin}", result[origin])
            event.set_extra(f"rh_images_{origin}_complete", complete)
        self.logger.debug("图片采集 message_id=%s current=%d reply=%d", message_id,
                          len(result["current"]), len(result["reply"]))
        return result

    def _delivery_target(self, event: Any):
        # Keep imports inside the plugin package namespace during reloads.
        from .delivery import DeliveryTarget
        return DeliveryTarget.from_event(event)

    async def _observe_chat_images(self, event: Any) -> None:
        if not any(isinstance(comp, Reply) or self._is_image_component(comp) for comp in event.get_messages()):
            return
        context = self._event_ctx(event)
        allowed, _ = self._check_access(context["user_id"], context["group_id"], check_quota=False)
        if allowed:
            try:
                await self._remember_event_images(event, observing=True)
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
            catalog.append(dict(name=wf.name, description=wf.description, output_type=wf.output_type, inputs=inputs))
        return catalog

    async def _natural_context(self, event: Any) -> str:
        context = self._event_ctx(event)
        allowed, message = self._check_access(context["user_id"], context["group_id"], check_quota=False)
        if not allowed:
            return json.dumps({"success": False, "message": message}, ensure_ascii=False)
        refs = await self._remember_event_images(event)
        store = self._get_media_store()
        owner = store.owner(context)
        self._avatar_scopes[owner] = context
        while len(self._avatar_scopes) > 256:
            self._avatar_scopes.pop(next(iter(self._avatar_scopes)))
        images = []
        for entry in store.recent(owner):
            ref = entry["ref"]
            where = "current" if ref in refs["current"] else "reply" if ref in refs["reply"] else "recent"
            images.append({"ref": ref, "source": where, "origin": entry["origin"],
                           "position": entry["position"], "task_id": entry["task_id"],
                           "age_seconds": max(0, int(time.time() - entry["created_at"]))})
        for position, memory in enumerate(await self._memory_candidates(owner), 1):
            aliases = {memory["memory_id"], *memory["media_ids"]}
            matches = [i for i in images if i["ref"] in aliases]
            if matches:
                for item in matches:
                    item.update(description=memory["description"], recent_index=position)
            else:
                images.append({"ref": memory["memory_id"], "source": "memory", "origin": "memory",
                               "description": memory["description"], "recent_index": position,
                               "age_seconds": max(0, int(time.time() - memory["used_at"]))})
        runs = [{"task_id": run["task_id"], "workflow": run["workflow"],
                 "status": run.get("status", "unknown"), "status_detail": run.get("status_detail", ""),
                 "monitoring": run["task_id"] in self._pending and not self._pending[run["task_id"]].done(),
                 "prompt_summary": run.get("prompt", "")[:500],
                 "parameters": {k: str(v)[:512] for k, v in run.get("parameters", {}).items()},
                 "image_bindings": run.get("image_bindings", {})}
                for run in store.recent_runs(owner)[:5]]
        for run in runs:
            if run["status"] == "submitted" and not run["monitoring"]:
                run.update(status="unknown", status_detail="仅有提交记录，当前未跟踪平台状态")
            try:
                delivery = store.get_delivery(owner, run["task_id"])
            except ValueError:
                continue
            run["delivery"] = {"total": len(delivery["outputs"]),
                               "sent": sum(bool(o.get("sent")) for o in delivery["outputs"]),
                               "failed": sum(bool(o.get("error")) for o in delivery["outputs"]),
                               "complete": bool(delivery.get("delivery_complete"))}
        submitting_workflow = None
        try:
            journal = await self._load_task_journal()
            durable = [r for r in journal.records() if MediaStore.owner(r) == owner][:5]
            submitting_workflow = next((r["workflow"] for r in durable if r["status"] == "submitting"), None)
            by_id = {r["task_id"]: r for r in runs}
            for job in durable:
                run = by_id.get(job["task_id"])
                if run is None:
                    run = {"task_id": job["task_id"], "workflow": job["workflow"]}
                    runs.append(run)
                run.update(status="succeeded" if job["status"] == "success" else job["status"],
                           status_detail=job["message"], remote_task_id=job["remote_task_id"],
                           monitoring=job["task_id"] in self._pending and not self._pending[job["task_id"]].done())
                run["delivery"] = {"total": len(job["outputs"]), "sent": sum(bool(o.get("sent")) for o in job["outputs"]),
                                   "failed": sum(bool(o.get("error")) for o in job["outputs"]),
                                   "complete": job["delivery_status"] == "sent"}
        except (OSError, ValueError) as exc:
            self.logger.warning("读取任务日志失败: %s", exc)
        pending = self._natural_pending_input(context)
        return json.dumps({"success": True, "workflows": self._natural_workflow_catalog(),
                           "images": images, "avatars": self._avatar_candidates(context), "recent_runs": runs, "pending_input": pending,
                           "submitting_workflow": submitting_workflow,
                           "instructions": "优先使用已有 description 选图，无简介时不得凭编号猜画面；多图用途不明确时询问。"
                           "画我、画群头像可选择 avatars 候选；普通请求不自动用头像。"
                           "编辑生成结果选 origin=generated 的图片；沿用原输入和参数使用 reuse_task_id。只提交一次。"
                           "pending_input 是尚未提交的任务，补齐后会自动提交，无需再次 run_workflow。"
                           "recent_runs 是任务记录；submitted 仅代表已提交，monitoring 仅代表本地跟踪；只有 succeeded 确认生成成功。"
                           "取消或补发已有任务用 manage_workflow_task；结果序号从 1 开始，补发不要重新生成。"
                           "图片池为空不能推出登记延迟，也不要求纯图消息。不要编造原因或画面内容。"}, ensure_ascii=False)

    def _natural_pending_input(self, context: dict[str, Any]) -> dict[str, Any] | None:
        session = self._input_sessions.get(self._session_key(context["user_id"], context["stream_id"]))
        if session is None or str(session.chat_info.get("platform_id") or "") != context["platform_id"]:
            return None
        return {"workflow": session.workflow.name, "submitted": False,
                "phase": "uploading" if session.consume_lock.locked() else session.phase,
                "received": list(session.received_labels),
                "waiting_files": list(session.waiting_nodes),
                "needs_prompt": bool(session.text_node_id and not session.command_text),
                "waiting_parameters": [{"key": f"{n['node_id']}/{n['field_name']}",
                                        "role": n.get("label", ""), "required": n.get("required", False)}
                                       for n in session.editable_nodes],
                "expires_in_seconds": max(0, int(600 - (time.time() - session.created_at))),
                "instructions": "补齐输入后自动提交；取消用 manage_workflow_task(action=cancel, task_id=pending_input) 或 /wf中断。"}

    def _set_workflow_run_state(self, task_id: str, stream_id: str, context: dict[str, Any],
                                status: str, detail: str = "", *, only_if_submitted: bool = False) -> None:
        try:
            store = self._get_media_store()
            owner = store.owner({**context, "stream_id": stream_id})
            run = store.get_run(owner, task_id)
            if not only_if_submitted or run.get("status") == "submitted":
                store.update_run(owner, task_id, status=status, status_detail=detail)
        except ValueError:
            pass
        except Exception as exc:
            self.logger.warning("任务状态记录失败: %s", exc)

    def _match_node(self, nodes: list[Any], name: str) -> Any:
        name = name.strip()
        exact = [n for n in nodes if node_key(n) == name]
        normalized = name.casefold()
        kinds = {"图片": "image", "参考图": "image", "音频": "audio", "视频": "video"}
        matches = exact or [n for n in nodes if normalized in {n.label.strip().casefold(), n.field_name.strip().casefold()}
                           or (self._resolve_value_type(n) in {"image", "audio", "video"}
                               and self._resolve_value_type(n) == kinds.get(normalized, normalized))]
        if len(matches) != 1:
            candidates = "、".join(f"{node_key(n)}（{n.label or n.field_name}）" for n in (matches or nodes)) or "无"
            raise ValueError(f"输入「{name}」不存在或有歧义，请使用完整 key；可用输入：{candidates}")
        return matches[0]

    async def _materialize_image(self, store: MediaStore, owner: str, ref: str, client: Any, *, snapshot=None) -> tuple[bytes, str]:
        if ref.startswith(("im-", "avatar:")):
            data = await self._read_special_image(owner, ref, client)
            await self._remember_image_usage(owner, ref, data)
            return data, self._guess_filename("", "image", data)
        try:
            entry = store.get(owner, ref)
        except ValueError:
            if not snapshot:
                raise
            entry = snapshot
        source = entry["source"]
        if source.startswith(("https://", "http://")):
            data = await client.download_bytes(source, max_bytes=MAX_IMAGE_BYTES)
        else:
            path = trusted_local_file(source, self._trusted_file_roots())
            if path is None:
                raise ValueError("图片本地路径不在允许的缓存目录内，请重新发送图片")
            if path.stat().st_size > MAX_IMAGE_BYTES:
                raise ValueError("图片超过 20MB，请压缩后重新发送")
            data = await asyncio.to_thread(path.read_bytes)
        if not data or len(data) > MAX_IMAGE_BYTES:
            raise ValueError("图片为空或超过 20MB，请重新发送")
        try:
            store.cache(owner, ref, data)
        except ValueError:
            pass  # A queued request may outlive the short-lived media index.
        await self._remember_image_usage(owner, ref, data)
        return data, self._guess_filename(source, "image", data)

    async def _run_natural_workflow(self, event: Any, workflow_name: str, prompt: str = "",
                                    image_refs: list[str] | None = None,
                                    image_bindings: dict[str, str] | None = None,
                                    parameters: dict[str, Any] | None = None,
                                    reuse_task_id: str = "", output_type: str = "") -> str:
        context = self._event_ctx(event)
        # Protect against repeated tool calls in one model turn (including a
        # second call while awaiting inputs). A new user event can start a new run.
        previous = event.get_extra("rh_natural_result", None)
        if previous is not None:
            try:
                parsed = json.loads(previous)
                if parsed.get("task_id"):
                    job = (await self._load_task_journal()).get(parsed["task_id"])
                    if job and job["status"] != parsed.get("status"):
                        return self._job_tool_result(job)
            except (ValueError, OSError, TypeError):
                pass
            return previous
        allowed, message = self._check_access(context["user_id"], context["group_id"], check_quota=False)
        if not allowed:
            return "错误：" + message
        try:
            existing = await self._find_event_job(context)
        except (OSError, ValueError) as exc:
            return "未提交任务：无法读取任务日志，" + str(exc)
        if existing:
            return self._job_tool_result(existing)
        pending = self._natural_pending_input(context)
        if pending:
            return "尚未提交任务：已有待补充输入的工作流。请调用 get_workflow_context 查看 pending_input，补齐后会自动提交；更换任务请先 /wf中断。"
        if event.get_extra("rh_natural_busy", False):
            return "本轮工作流正在准备，请勿重复提交。"
        event.set_extra("rh_natural_busy", True)
        try:
            response = await self._prepare_natural_run(event, context, workflow_name, prompt,
                                                       image_refs, image_bindings, parameters, reuse_task_id, output_type)
            if response.get("success"):
                if response.get("waiting"):
                    text = "尚未提交任务：" + response["message"] + "。请向用户说明需要补充的内容，然后结束本轮工具调用。"
                else:
                    text = self._job_tool_result(response)
                event.set_extra("rh_natural_result", text)
                return text
            if response.get("submission_attempted"):
                text = "提交请求失败或未获确认，本轮不会自动重试，请核对 RunningHub 任务后再操作。" + response["message"]
                event.set_extra("rh_natural_result", text)
                return text
            return json.dumps({"success": False, "message": "未提交任务：" + response["message"],
                               "correction_context": json.loads(await self._natural_context(event))}, ensure_ascii=False)
        except asyncio.CancelledError:
            event.set_extra("rh_natural_result", "本轮工作流处理已中断，请先核对任务状态，勿自动重复提交。")
            raise
        except (ValueError, OSError) as exc:
            return json.dumps({"success": False, "message": "未提交任务：" + str(exc),
                               "correction_context": json.loads(await self._natural_context(event))}, ensure_ascii=False)
        except Exception as exc:
            self.logger.exception("自然语言任务准备失败")
            return "任务准备失败，请检查图片是否仍可下载或重新发送图片。详细原因已记录日志。"
        finally:
            event.set_extra("rh_natural_busy", False)

    async def _prepare_natural_run(self, event: Any, context: dict[str, str], workflow_name: str,
                                   prompt: str, image_refs: Any, image_bindings: Any,
                                   parameters: Any, reuse_task_id: str, output_type: str = "") -> dict[str, Any]:
        refs = await self._remember_event_images(event)
        store = self._get_media_store()
        owner = store.owner(context)
        if not owner:
            raise ValueError("无法识别当前用户或会话")
        self._avatar_scopes[owner] = context.copy()
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
        if output_type not in {"", "auto", "image", "video", "audio", "file"}:
            raise ValueError("output_type 应为 image、video、audio、file 或 auto")
        available = [w for w in self._workflows if self._is_llm_callable_workflow(w)
                     and (output_type in {"", "auto"} or w.output_type == output_type)]
        if not workflow_name and len(available) == 1:
            workflow_name = available[0].name
        wf = self._find_workflow(str(workflow_name or "").strip())
        if wf is None or not self._is_llm_callable_workflow(wf):
            raise ValueError("该工作流不存在或未开启自然语言调用，请先调用 get_workflow_context 选择可用工作流")
        if output_type not in {"", "auto"} and wf.output_type not in {"auto", output_type}:
            raise ValueError(f"工作流输出类型为 {wf.output_type}，与请求的 {output_type} 不符，请重新选择")
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
            if node_key(node) in overrides:
                raise ValueError(f"参数 {node_key(node)} 被重复指定，请只保留一个名称")
            value = parameter_value(node, value)
            overrides[node_key(node)] = value
        canonical_bindings = {}
        for name, ref in bindings.items():
            node = self._match_node(image_nodes, str(name))
            if not isinstance(ref, str):
                raise ValueError("图片绑定值必须是图片编号")
            if node_key(node) in canonical_bindings:
                raise ValueError(f"图片输入 {node_key(node)} 被重复绑定，请只保留一个名称")
            canonical_bindings[node_key(node)] = ref
        if image_refs is None and image_bindings is None and not inherited and image_nodes:
            if refs["current"] and refs["reply"]:
                raise ValueError("当前消息和引用消息都有图片，请先查看图片编号并明确选择")
            image_refs = refs["current"] or refs["reply"]
        if image_refs:
            if len(image_refs) > len(image_nodes):
                raise ValueError(f"工作流最多接收 {len(image_nodes)} 张图片，请明确选择需要的图片")
            empty_nodes = [n for n in image_nodes if not n.field_value.strip()]
            targets = image_nodes if len(image_nodes) == 1 else empty_nodes
            if len(targets) != 1:
                raise ValueError("多图工作流请先查看各图片输入用途，使用 image_bindings 明确绑定角色")
            if len(image_refs) != 1:
                raise ValueError("只有一个待填图片输入，请明确选择一张图片")
            canonical_bindings = {node_key(targets[0]): image_refs[0]}
        # Validate every reference and parameter before the first network/upload.
        for ref in canonical_bindings.values():
            await self._validate_image_ref(owner, ref)
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
        # Capture references now; downloads, vision, uploads and enhancement run in the worker.
        sources = {ref: dict(store.get(owner, ref)) for ref in canonical_bindings.values()
                   if not ref.startswith(("im-", "avatar:"))}
        for key, ref in canonical_bindings.items():
            overrides[key] = "rh-image-ref:" + ref
        context.update(trigger="tool", natural=True, node_overrides=overrides, image_sources=sources,
                       image_bindings=canonical_bindings, natural_event=event)
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
                                            status="submitted", status_detail="已提交，等待平台结果",
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
