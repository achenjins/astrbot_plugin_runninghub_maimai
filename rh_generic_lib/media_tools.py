"""AstrBot-native, owner-scoped media discovery and explicit image inspection.

Only host message components and journal outputs can create media references.
Tool arguments select those references; they never introduce URLs or local paths.
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import math
import time
from pathlib import Path
from typing import Any

from astrbot.api.message_components import File, Image, Record, Reply, Video

from .file_source import decode_base64_bounded, trusted_local_file
from .media_store import MAX_IMAGE_BYTES, MediaStore

MEDIA_TYPES = {"image", "audio", "video"}
MAX_MEDIA_BYTES = 512 * 1024 * 1024


def _timestamp(value):
    try:
        stamp = float(value or 0)
        return stamp if math.isfinite(stamp) else 0
    except (TypeError, ValueError):
        return 0


class ScopedMediaRegistry:
    """A small persistent registry for audio/video references, without media copies."""

    def __init__(self, path: Path, ttl: int):
        self.path, self.ttl = path, ttl
        self.entries: dict[str, dict[str, Any]] = {}
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                self.entries = {key: value for key, value in raw.items()
                                if isinstance(value, dict) and value.get("type") in {"audio", "video"}}
        except (OSError, ValueError):
            pass
        self.prune()

    def prune(self):
        counts, retained, encoded_size = {}, {}, 0
        for key, item in sorted(self.entries.items(), key=lambda pair: _timestamp(pair[1].get("created_at")), reverse=True):
            owner = str(item.get("owner") or "")
            if (not owner or _timestamp(item.get("created_at")) < time.time() - self.ttl
                    or counts.get(owner, 0) >= 32 or len(retained) >= 1024):
                continue
            # Base64 comes from a trusted component, but must not grow the index
            # without a bound. Large uploads should use a file/URL component.
            size = len(str(item.get("source") or ""))
            if encoded_size + size > 32 * 1024 * 1024:
                continue
            encoded_size += size
            retained[key] = item
            counts[owner] = counts.get(owner, 0) + 1
        self.entries = retained

    def _save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.entries, ensure_ascii=False), encoding="utf-8")
        temporary.replace(self.path)

    def remember(self, owner, kind, source, *, message_id, position, origin, filename="", file_id=""):
        if not owner or kind not in {"audio", "video"} or not (source or file_id):
            return None
        if source.startswith("base64://") and len(source) > (MAX_IMAGE_BYTES + 2) // 3 * 4 + 9:
            raise ValueError("直接编码素材超过 20MB，请作为文件重新发送")
        # Platform IDs make retrying discovery stable even if adapter URLs rotate.
        identity = [owner, message_id, position, kind, file_id or source]
        ref = "med_" + hashlib.sha256(json.dumps(identity, ensure_ascii=False).encode()).hexdigest()[:24]
        previous = self.entries.get(ref)
        item = dict(ref=ref, owner=owner, type=kind, source=source, message_id=message_id,
                    position=position, origin=origin, filename=filename, file_id=file_id,
                    created_at=previous["created_at"] if previous else time.time())
        self.entries[ref] = item
        self.prune()
        self._save()
        return copy.deepcopy(self.entries.get(ref))

    def recent(self, owner):
        self.prune()
        return [copy.deepcopy(item) for item in self.entries.values() if item.get("owner") == owner]

    def get(self, owner, ref):
        self.prune()
        item = self.entries.get(ref)
        if item is None or item.get("owner") != owner:
            raise ValueError("素材编号不存在、已过期或不属于当前用户会话，请重新发送或引用素材")
        return copy.deepcopy(item)


class MediaToolsMixin:
    def _get_other_media_registry(self):
        if getattr(self, "_other_media_registry", None) is None:
            self._other_media_registry = ScopedMediaRegistry(
                self._prompt_library_path().parent / "chat_media" / "media.json",
                self.config.feature.media_history_minutes * 60)
        self._other_media_registry.ttl = self.config.feature.media_history_minutes * 60
        return self._other_media_registry

    def _media_component_type(self, component):
        if isinstance(component, Image):
            return "image"
        if isinstance(component, Record):
            return "audio"
        if isinstance(component, Video):
            return "video"
        if isinstance(component, File):
            return self._detect_file_type_from_name(str(component.name or component.file or component.url or ""))
        return "unknown"

    def _platform_media_file_id(self, value):
        value = str(value or "")
        if (not value or len(value) > 1024 or value.startswith(("http://", "https://", "base64://"))
                or trusted_local_file(value, self._trusted_file_roots())):
            return ""
        return value

    async def _remember_event_media(self, event, *, include_reply=True, observing=False):
        """Capture trusted audio/video alongside the existing image registry."""
        image_refs = await self._remember_event_images(event, include_reply=include_reply, observing=observing)
        refs = {origin: list(items) for origin, items in image_refs.items()}
        context = self._event_ctx(event)
        owner = MediaStore.owner(context)
        if not owner:
            return refs
        registry = self._get_other_media_registry()
        message_id = str(context.get("anchor_id") or "")
        components = event.get_messages()
        deadline = asyncio.get_running_loop().time() + (1.5 if observing else 12)
        for origin in ("current", "reply"):
            if origin == "reply" and not include_reply:
                continue
            entries = []
            complete = True
            chains = [(message_id, components)] if origin == "current" else [
                (str(comp.id), comp.chain or []) for comp in components if isinstance(comp, Reply)]
            for mid, chain in chains:
                position = 0
                for component in chain:
                    kind = self._media_component_type(component)
                    if kind not in {"audio", "video"}:
                        continue
                    position += 1
                    source = self._image_source(getattr(component, "file", ""), getattr(component, "url", ""))
                    filename = str(getattr(component, "name", "") or "")
                    file_id = self._platform_media_file_id(getattr(component, "file", "")) if isinstance(component, (Record, Video)) else ""
                    if isinstance(component, File) and "ftn.qq.com" in source:
                        source = ""
                    if source or file_id:
                        entries.append((kind, source, mid, position, filename, file_id))
                    else:
                        complete = False
                # Expanded Reply.chain is native to AstrBot. Resolve an empty
                # quote only inside this same group/user, never model arguments.
                cached_quote = event.get_extra("rh_reply_raw:" + mid, None)
                if (origin == "reply" and not observing and
                        (not chain or cached_quote is not None or all(self._media_component_type(c) == "unknown" for c in chain))):
                    bot = self.delivery.get_onebot_client(self._delivery_target(event))
                    if bot is not None:
                        try:
                            remaining = deadline - asyncio.get_running_loop().time()
                            if remaining <= 0:
                                raise TimeoutError("引用读取预算已用完")
                            raw = cached_quote
                            if raw is None:
                                raw = await asyncio.wait_for(bot.call_action("get_msg", message_id=int(mid)), remaining)
                            if not isinstance(raw, dict) or raw.get("status") == "failed" or raw.get("retcode", 0) != 0:
                                raise ValueError("引用读取失败")
                            raw = raw.get("data", raw)
                            group = str(raw.get("group_id") or "")
                            sender = raw.get("sender") if isinstance(raw.get("sender"), dict) else {}
                            if context["group_id"] and group != context["group_id"]:
                                complete = False
                                continue
                            if not context["group_id"] and (group or str(sender.get("user_id") or raw.get("user_id") or "") != context["user_id"]):
                                complete = False
                                continue
                            position = 0
                            for segment in raw.get("message") or []:
                                if not isinstance(segment, dict) or not isinstance(segment.get("data"), dict):
                                    continue
                                data = segment["data"]
                                kind = {"record": "audio", "audio": "audio", "video": "video"}.get(segment.get("type"), "")
                                if segment.get("type") == "file":
                                    kind = self._detect_file_type_from_name(data.get("name", ""))
                                if kind in {"audio", "video"}:
                                    position += 1
                                    source = self._image_source(data.get("file"), data.get("url"))
                                    file_id = str(data.get("file_id") or data.get("id") or "")
                                    if segment.get("type") == "file" and (not source or "ftn.qq.com" in source):
                                        try:
                                            remaining = deadline - asyncio.get_running_loop().time()
                                            source = await self._group_file_source(file_id, context, timeout=max(.01, remaining)) if remaining > 0 else ""
                                        except Exception as exc:
                                            source = ""
                                            self.logger.debug("引用音视频文件读取失败: %s", exc)
                                    platform_id = (self._platform_media_file_id(data.get("file") or file_id)
                                                   if segment.get("type") in {"record", "audio", "video"} else "")
                                    if source or platform_id:
                                        entries = [item for item in entries if (item[2], item[3]) != (mid, position)]
                                        entries.append((kind, source, mid, position, str(data.get("name") or ""), platform_id))
                                    else:
                                        complete = False
                        except Exception as exc:
                            complete = False
                            self.logger.debug("引用音视频读取失败: %s", exc)
            # Current raw file segments may carry a group file ID which the
            # parsed File component does not retain.
            if origin == "current":
                raw = getattr(getattr(event, "message_obj", None), "raw_message", None)
                segments = raw.get("message", []) if isinstance(raw, dict) else []
                position = 0
                for segment in segments if isinstance(segments, list) else []:
                    if not isinstance(segment, dict) or not isinstance(segment.get("data"), dict):
                        continue
                    data = segment["data"]
                    kind = {"record": "audio", "audio": "audio", "video": "video"}.get(segment.get("type"), "")
                    if segment.get("type") == "file":
                        kind = self._detect_file_type_from_name(data.get("name", ""))
                    if kind not in {"audio", "video"}:
                        continue
                    position += 1
                    source = self._image_source(data.get("file"), data.get("url"))
                    file_id = str(data.get("file_id") or data.get("id") or "")
                    if segment.get("type") == "file" and (not source or "ftn.qq.com" in source):
                        try:
                            remaining = deadline - asyncio.get_running_loop().time()
                            source = await self._group_file_source(file_id, context, timeout=max(.01, remaining)) if remaining > 0 else ""
                        except Exception as exc:
                            source = ""
                            self.logger.debug("当前音视频文件读取失败: %s", exc)
                    platform_id = (self._platform_media_file_id(data.get("file") or file_id)
                                   if segment.get("type") in {"record", "audio", "video"} else "")
                    if source or platform_id:
                        entries = [item for item in entries if item[3] != position]
                        entries.append((kind, source, message_id, position, str(data.get("name") or ""), platform_id))
                    else:
                        complete = False
            for kind, source, mid, position, filename, file_id in entries[:32]:
                try:
                    item = registry.remember(owner, kind, source, message_id=mid, position=position,
                                             origin="upload" if origin == "current" else "reply",
                                             filename=filename, file_id=file_id)
                    if item:
                        refs[origin].append(item["ref"])
                except (OSError, ValueError) as exc:
                    complete = False
                    self.logger.debug("聊天音视频登记失败: %s", exc)
            event.set_extra(f"rh_media_{origin}_complete", complete)
        return refs

    async def _media_assets(self, event, source="all", media_type=""):
        if source not in {"all", "current", "reply", "recent", "memory", "result", "avatar"} or media_type not in {"", *MEDIA_TYPES}:
            raise ValueError("素材来源或类型无效")
        context = self._event_ctx(event)
        allowed, message = self._check_access(context["user_id"], context["group_id"], check_quota=False, natural_language=True)
        if not allowed:
            raise ValueError(message)
        owner = MediaStore.owner(context)
        if not owner:
            raise ValueError("无法识别当前用户或会话")
        self._avatar_scopes[owner] = context.copy()
        while len(self._avatar_scopes) > 256:
            self._avatar_scopes.pop(next(iter(self._avatar_scopes)))
        refs = {"current": [], "reply": []}
        warnings = []
        if source in {"all", "current", "reply", "recent"}:
            refs = await self._remember_event_media(event, include_reply=source != "current")
            for origin in ("current", "reply"):
                if origin == "reply" and source == "current":
                    continue
                if (not event.get_extra(f"rh_images_{origin}_complete", True) or
                        not event.get_extra(f"rh_media_{origin}_complete", True)):
                    warnings.append("部分引用素材无法读取，请检查引用或重新发送" if origin == "reply" else
                                    "部分当前素材无法读取，请重新发送素材")
        event.set_extra("rh_media_query_warnings", warnings)
        assets = []
        if source in {"all", "current", "reply", "recent"}:
            entries = [{**item, "type": "image"} for item in self._get_media_store().recent(owner)]
            entries.extend(self._get_other_media_registry().recent(owner))
            for item in entries:
                ref = item["ref"]
                where = "current" if ref in refs["current"] else "reply" if ref in refs["reply"] else "recent"
                if source not in {"all", where}:
                    continue
                assets.append({**item, "media_id": ref, "source_kind": where})
        memory_items = await self._memory_candidates(owner) if source in {"all", "memory", "current", "reply", "recent"} and media_type in {"", "image"} else []
        for position, item in enumerate(memory_items, 1):
            aliases = {item["memory_id"], *item.get("media_ids", [])}
            matches = [asset for asset in assets if asset["media_id"] in aliases]
            for asset in matches:
                asset.update(memory_id=item["memory_id"], description=item["description"], recent_index=position,
                             used_at=item["used_at"], expires_at=item.get("expires_at", 0))
            if source in {"all", "memory"} and not matches:
                assets.append({"media_id": item["memory_id"], "ref": item["memory_id"], "memory_id": item["memory_id"],
                               "type": "image", "origin": "memory", "source_kind": "memory", "description": item["description"],
                               "recent_index": position, "used_at": item["used_at"], "expires_at": item.get("expires_at", 0)})
        if source in {"all", "avatar"} and media_type in {"", "image"}:
            assets.extend({**item, "media_id": item["ref"], "type": "image", "source_kind": "avatar"}
                          for item in self._avatar_candidates(context))
        if source in {"all", "result"}:
            journal = await self._load_task_journal()
            for record in journal.records():
                if MediaStore.owner(record) != owner or record["status"] != "success":
                    continue
                stamp = float(record.get("finished_at") or record.get("updated_at") or 0)
                for position, output in enumerate(record.get("outputs", []), 1):
                    url = str(output.get("url") or "")
                    hint = str(output.get("type") or "")
                    kind = ("image" if self._is_image_url(url, hint) else "video" if self._is_video_url(url, hint)
                            else "audio" if hint == "audio" or hint.startswith("audio/") else self._detect_file_type_from_name(url))
                    if kind not in MEDIA_TYPES:
                        continue
                    ttl = 24 * 3600 if kind == "image" else self.config.feature.media_history_minutes * 60
                    if stamp < time.time() - ttl:
                        continue
                    ref = "result:" + record["task_id"] + ":" + str(position)
                    assets.append({"media_id": ref, "ref": ref, "type": kind, "origin": "generated", "source_kind": "result",
                                   "source": url, "position": position, "task_id": record["task_id"], "created_at": stamp,
                                   "description": f"{record['workflow']} 结果 {position}"})
        unique = {}
        for asset in assets:
            if media_type and asset["type"] != media_type:
                continue
            unique.setdefault(asset["media_id"], asset)
        return list(unique.values())[:64]

    async def _media_catalog_tool(self, event, source="all", media_type=""):
        try:
            assets = await self._media_assets(event, source, media_type)
            public = []
            for asset in assets:
                item = {key: asset[key] for key in ("media_id", "type", "description", "position", "task_id", "recent_index",
                                                   "memory_id", "created_at", "used_at", "expires_at") if asset.get(key)}
                item["source"] = asset["source_kind"]
                item["origin"] = asset["source_kind"]
                public.append(item)
            return json.dumps({"success": True, "media": public, "count": len(public),
                               "warnings": event.get_extra("rh_media_query_warnings", []),
                               "instructions": "优先按已有简介选图；无简介时不得猜画面。多份素材请按工作流用途绑定。"
                               "memory_id 是保存原图的稳定编号。视频和音频不做视觉解码，按用户描述选择。"}, ensure_ascii=False)
        except (ValueError, OSError) as exc:
            return json.dumps({"success": False, "message": str(exc)}, ensure_ascii=False)
        except Exception as exc:
            self.logger.warning("素材查询暂不可用: %s", exc)
            return json.dumps({"success": False, "message": "素材查询暂不可用，请重新发送素材或稍后重试"}, ensure_ascii=False)

    async def _media_entry(self, owner, ref):
        if ref.startswith("med_"):
            return self._get_other_media_registry().get(owner, ref)
        if ref.startswith("result:"):
            try:
                task_id, index = ref[len("result:"):].rsplit(":", 1)
                position = int(index)
            except (ValueError, TypeError):
                raise ValueError("结果素材编号无效") from None
            record = (await self._load_task_journal()).get(task_id)
            if not record or MediaStore.owner(record) != owner or record["status"] != "success" or not 1 <= position <= len(record["outputs"]):
                raise ValueError("结果素材不存在或不属于当前用户会话")
            output = record["outputs"][position - 1]
            url, hint = str(output.get("url") or ""), str(output.get("type") or "")
            kind = ("image" if self._is_image_url(url, hint) else "video" if self._is_video_url(url, hint)
                    else "audio" if hint == "audio" or hint.startswith("audio/") else self._detect_file_type_from_name(url))
            ttl = 24 * 3600 if kind == "image" else self.config.feature.media_history_minutes * 60
            stamp = float(record.get("finished_at") or record.get("updated_at") or 0)
            if kind not in MEDIA_TYPES or stamp < time.time() - ttl:
                raise ValueError("结果素材已过期，请按任务编号补发或重新提供素材")
            return {"ref": ref, "type": kind, "source": url, "task_id": task_id, "position": position}
        if ref.startswith("avatar:"):
            await self._validate_image_ref(owner, ref)
            return {"ref": ref, "type": "image"}
        # Saved aliases remain usable when the short-lived message cache expired.
        memory = await self._find_image_memory(owner, ref)
        if memory is not None:
            return {"ref": ref, "type": "image", "memory_id": memory["memory_id"], "description": memory["description"]}
        return {**self._get_media_store().get(owner, ref), "type": "image"}

    async def _validate_media_ref(self, owner, ref, kind=""):
        if not isinstance(ref, str) or not ref:
            raise ValueError("媒体绑定值必须是素材编号")
        entry = await self._media_entry(owner, ref)
        if kind and entry["type"] != kind:
            raise ValueError(f"素材类型是 {entry['type']}，无法绑定到 {kind} 输入")
        return entry

    async def _read_media_source(self, source, client, limit):
        if source.startswith(("https://", "http://")):
            data = await client.download_bytes(source, max_bytes=limit)
        elif source.startswith("base64://"):
            data = decode_base64_bounded(source[9:], limit)
        else:
            path = trusted_local_file(source, self._trusted_file_roots())
            if path is None:
                raise ValueError("素材本地路径不在允许的缓存目录内，请重新发送")
            if path.stat().st_size > limit:
                raise ValueError("素材超过大小上限")
            data = await asyncio.to_thread(path.read_bytes)
        if not data or len(data) > limit:
            raise ValueError("素材为空或超过大小上限")
        return data

    async def _adapter_media_bytes(self, response, client, limit):
        if not isinstance(response, dict) or response.get("status") == "failed" or response.get("retcode", 0) != 0:
            return None
        payload = response.get("data", response)
        if isinstance(payload, str):
            source = self._image_source(payload)
            return await self._read_media_source(source, client, limit) if source else None
        if not isinstance(payload, dict):
            return None
        for field in ("base64", "b64", "binary_data_base64"):
            if payload.get(field):
                return decode_base64_bounded(str(payload[field]).removeprefix("base64://"), limit)
        for field in ("file", "path", "file_path", "url", "file_url", "download_url"):
            if source := self._image_source(payload.get(field)):
                return await self._read_media_source(source, client, limit)
        return None

    async def _materialize_media(self, store, owner, ref, client, *, snapshot=None):
        try:
            entry = await self._media_entry(owner, ref)
        except ValueError:
            if snapshot is None:
                raise
            entry = snapshot
        kind = str(entry.get("type") or "image")
        if kind == "image" and not ref.startswith("result:"):
            return await self._materialize_image(store, owner, ref, client, snapshot=entry)
        limit = MAX_IMAGE_BYTES if kind == "image" else min(MAX_MEDIA_BYTES, self.config.generation.max_file_mb * 1024 * 1024)
        context = self._avatar_scopes.get(owner, {})
        # QQ voice URLs can be SILK; ask its trusted adapter to convert to MP3.
        if kind in {"audio", "video"} and entry.get("file_id"):
            from .delivery import DeliveryTarget
            bot = self.delivery.get_onebot_client(DeliveryTarget.from_dict(context))
            if bot is not None:
                try:
                    response = await asyncio.wait_for(
                        bot.call_action("get_record", file=entry["file_id"], out_format="mp3") if kind == "audio" else
                        bot.call_action("get_file", file=entry["file_id"]), 15)
                    data = await self._adapter_media_bytes(response, client, limit)
                    if data:
                        return data, "input_audio.mp3" if kind == "audio" else "input_video.mp4"
                except Exception as exc:
                    self.logger.debug("语音转码暂不可用，尝试原素材: %s", exc)
        source = str(entry.get("source") or "")
        data = await self._read_media_source(source, client, limit)
        if kind == "image":
            await self._remember_image_usage(owner, ref, data, origin="reference")
        return data, self._guess_filename(entry.get("filename") or source, kind, data)

    async def _inspect_media_tool(self, event, media_id=""):
        try:
            context = self._event_ctx(event)
            allowed, message = self._check_access(context["user_id"], context["group_id"], check_quota=False, natural_language=True)
            if not allowed:
                raise ValueError(message)
            if not media_id:
                raise ValueError("请指定素材编号")
            owner = MediaStore.owner(context)
            self._avatar_scopes[owner] = context.copy()
            while len(self._avatar_scopes) > 256:
                self._avatar_scopes.pop(next(iter(self._avatar_scopes)))
            entry = await self._validate_media_ref(owner, media_id)
            if entry["type"] != "image":
                return json.dumps({"success": True, "media_id": media_id, "type": entry["type"],
                                   "message": "此工具不解码视频或音频，请依据用户描述绑定用途"}, ensure_ascii=False)
            saved = await self._find_image_memory(owner, media_id)
            if saved is not None and saved["description"]:
                memory = await self._load_image_memory()
                await memory.touch(owner, "images", saved["memory_id"])
                return json.dumps({"success": True, "media_id": saved["memory_id"], "type": "image", "cached": True,
                                   "description": saved["description"], "message": saved["description"]}, ensure_ascii=False)
            client = self._client or self._client_cn
            if client is None:
                raise ValueError("RunningHub 客户端未初始化")
            # Materialization may have produced an optional automatic summary.
            data, _ = await self._materialize_media(self._get_media_store(), owner, media_id, client, snapshot=entry)
            saved = await self._find_image_memory(owner, media_id)
            description = saved.get("description", "") if saved else ""
            if not description:
                try:
                    description = await self._describe_image(owner, data)
                except Exception as exc:
                    self.logger.debug("图片显式观察失败: %s", exc)
                    description = ""
                saved = await self._remember_image_usage(owner, media_id, data, description=description)
            return json.dumps({"success": bool(description), "media_id": saved["memory_id"] if saved else media_id,
                               "type": "image", "description": description,
                               "message": description or "图片简介暂不可用，请依据对话或询问用户；不要猜画面或重复观察"}, ensure_ascii=False)
        except (ValueError, OSError) as exc:
            return json.dumps({"success": False, "message": str(exc)}, ensure_ascii=False)
        except Exception as exc:
            self.logger.warning("图片观察暂不可用: %s", exc)
            return json.dumps({"success": False, "message": "素材读取失败，请重新发送参考图；不要编造画面内容"}, ensure_ascii=False)
