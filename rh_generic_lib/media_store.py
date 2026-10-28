"""Bounded, session-owned references for chat images and reusable workflow runs.

Only references from this registry are exposed to tools. Remote URLs and local
paths never come from model arguments. Sources expire; cached bytes are capped.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any

MAX_IMAGE_BYTES = 20 * 1024 * 1024
MAX_CACHE_BYTES = 256 * 1024 * 1024


class MediaStore:
    def __init__(self, directory: Path, ttl: int = 3600):
        self.directory = directory
        self.ttl = ttl
        self.images: dict[str, dict[str, Any]] = {}
        self.runs: dict[str, dict[str, Any]] = {}
        self.deliveries: dict[str, dict[str, Any]] = {}
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / "index.json"
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                self.images = {k: v for k, v in data.get("images", {}).items() if isinstance(v, dict)}
                self.runs = {k: v for k, v in data.get("runs", {}).items() if isinstance(v, dict)}
                self.deliveries = {k: v for k, v in data.get("deliveries", {}).items() if isinstance(v, dict)}
        except (OSError, ValueError, AttributeError):
            pass
        self.prune()

    @staticmethod
    def owner(context: dict[str, Any]) -> str:
        uid = str(context.get("user_id") or "")
        sid = str(context.get("stream_id") or "")
        if not uid or not sid:
            return ""
        return json.dumps([str(context.get("platform_id") or ""), sid, uid], ensure_ascii=False)

    def _save(self) -> None:
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps({"images": self.images, "runs": self.runs, "deliveries": self.deliveries}, ensure_ascii=False), encoding="utf-8")
        temporary.replace(self.path)

    def prune(self) -> None:
        cutoff = time.time() - self.ttl
        removed = False
        for collection, per_owner, total in ((self.images, 32, 1024), (self.runs, 10, 256), (self.deliveries, 10, 256)):
            counts: dict[str, int] = {}
            retained = []
            for key, entry in sorted(collection.items(), key=lambda pair: float(pair[1].get("created_at", 0)), reverse=True):
                owner = str(entry.get("owner") or "")
                counts[owner] = counts.get(owner, 0) + 1
                if float(entry.get("created_at", 0)) >= cutoff and counts[owner] <= per_owner:
                    retained.append(key)
            keep = set(retained[:total])
            for key in list(collection):
                if key not in keep:
                    del collection[key]
                    removed = True
        # Only this store's generated cache filenames are eligible for cleanup.
        referenced = {str(v.get("cache") or "") for v in self.images.values()}
        files = sorted(self.directory.glob("img_*.bin"), key=lambda p: p.stat().st_mtime, reverse=True)
        used = 0
        for path in files:
            size = path.stat().st_size
            if path.name not in referenced or used + size > MAX_CACHE_BYTES:
                path.unlink(missing_ok=True)
            else:
                used += size
        if removed:
            self._save()

    def _cache_path(self, entry: dict[str, Any]) -> Path | None:
        name = str(entry.get("cache") or "")
        if not name or Path(name).name != name or not name.startswith("img_") or not name.endswith(".bin"):
            return None
        path = self.directory / name
        return path if path.is_file() else None

    def remember(self, owner: str, source: str, *, message_id: str = "", position: int = 1,
                 origin: str = "upload", task_id: str = "", data: bytes | None = None) -> dict[str, Any]:
        if not owner:
            raise ValueError("无法识别图片所属用户或会话")
        # Deduplicate repeated processing of an event, not identical images in
        # distinct messages/slots: users may intentionally use the same image twice.
        identity = json.dumps([owner, message_id, position, origin, task_id, source])
        fingerprint = hashlib.sha256(identity.encode()).hexdigest()
        self.prune()
        for entry in self.images.values():
            if entry.get("fingerprint") == fingerprint:
                if data is not None:
                    self.cache(owner, entry["ref"], data)
                return copy.deepcopy(entry)
        if source.startswith("base64://"):
            encoded = source.removeprefix("base64://")
            if len(encoded) > (MAX_IMAGE_BYTES + 2) // 3 * 4:
                raise ValueError("图片超过 20MB，请压缩后重新发送")
            data = base64.b64decode(encoded, validate=True)
            source = ""
        if data is not None and len(data) > MAX_IMAGE_BYTES:
            raise ValueError("图片超过 20MB，请压缩后重新发送")
        ref = "img_" + uuid.uuid4().hex[:16]
        entry = dict(ref=ref, owner=owner, source=source, message_id=message_id,
                     position=position, origin=origin, task_id=task_id,
                     created_at=time.time(), fingerprint=fingerprint)
        if data is not None:
            path = self.directory / (ref + ".bin")
            path.write_bytes(data)
            entry["cache"] = path.name
        self.images[ref] = entry
        self.prune()
        self._save()
        return copy.deepcopy(entry)

    def get(self, owner: str, ref: str) -> dict[str, Any]:
        self.prune()
        entry = self.images.get(ref)
        if not entry or entry.get("owner") != owner:
            raise ValueError("图片编号不存在、已过期或不属于当前用户会话，请重新发送或引用图片")
        result = copy.deepcopy(entry)
        cached = self._cache_path(entry)
        if cached:
            result["source"] = str(cached)
        if not result.get("source"):
            raise ValueError("图片缓存已过期，请重新发送或引用图片")
        return result

    def cache(self, owner: str, ref: str, data: bytes) -> None:
        self.get(owner, ref)
        if len(data) > MAX_IMAGE_BYTES:
            raise ValueError("图片超过 20MB")
        path = self.directory / (ref + ".bin")
        path.write_bytes(data)
        self.images[ref]["cache"] = path.name
        self.prune()
        self._save()

    def recent(self, owner: str) -> list[dict[str, Any]]:
        self.prune()
        return [copy.deepcopy(v) for v in sorted(self.images.values(), key=lambda v: v["created_at"], reverse=True)
                if v.get("owner") == owner]

    def remember_run(self, owner: str, task_id: str, **values: Any) -> None:
        if not owner:
            return
        self.runs[task_id] = dict(owner=owner, task_id=task_id, created_at=time.time(), **copy.deepcopy(values))
        self.prune()
        self._save()

    def get_run(self, owner: str, task_id: str) -> dict[str, Any]:
        self.prune()
        run = self.runs.get(task_id)
        if not run or run.get("owner") != owner:
            raise ValueError("任务记录已过期或不属于当前用户会话，请先查看可复用任务")
        return copy.deepcopy(run)

    def recent_runs(self, owner: str) -> list[dict[str, Any]]:
        self.prune()
        return [copy.deepcopy(v) for v in sorted(self.runs.values(), key=lambda v: v["created_at"], reverse=True)
                if v.get("owner") == owner]

    def update_run(self, owner: str, task_id: str, **changes: Any) -> None:
        run = self.runs.get(task_id)
        if run and run.get("owner") == owner:
            run.update(copy.deepcopy(changes), updated_at=time.time())
            self._save()


    def remember_delivery(self, owner: str, task_id: str, **values: Any) -> dict[str, Any]:
        if not owner:
            raise ValueError("无法识别结果所属用户或会话")
        record = dict(owner=owner, task_id=task_id, created_at=time.time(), **copy.deepcopy(values))
        self.deliveries[task_id] = record
        self.prune()
        self._save()
        return copy.deepcopy(record)

    def recent_deliveries(self, owner: str) -> list[dict[str, Any]]:
        self.prune()
        return [copy.deepcopy(v) for v in sorted(self.deliveries.values(), key=lambda v: v["created_at"], reverse=True)
                if v.get("owner") == owner]

    def get_delivery(self, owner: str, task_id: str) -> dict[str, Any]:
        self.prune()
        record = self.deliveries.get(task_id)
        if not record or record.get("owner") != owner:
            raise ValueError("结果记录不存在、已过期或不属于当前用户会话。可发送 /wf补发 查看本会话可用结果。")
        return copy.deepcopy(record)

    def update_delivery_output(self, owner: str, task_id: str, index: int, **changes: Any) -> None:
        record = self.deliveries.get(task_id)
        if record and record.get("owner") == owner:
            record["outputs"][index].update(changes)
            self._save()

    def update_delivery(self, owner: str, task_id: str, **changes: Any) -> None:
        record = self.deliveries.get(task_id)
        if record and record.get("owner") == owner:
            record.update(copy.deepcopy(changes))
            self._save()
