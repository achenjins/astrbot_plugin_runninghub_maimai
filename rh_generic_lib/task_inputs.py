"""Immutable task-owned image copies, independent of chat/memory expiry."""
from __future__ import annotations

import hashlib
import re
import threading
from pathlib import Path

from .media_store import MAX_IMAGE_BYTES
from .upstream.background_io import finish_in_thread


class TaskInputs:
    MAX_TOTAL_BYTES = 1024 * 1024 * 1024
    MAX_TASK_BYTES = 256 * 1024 * 1024

    def __init__(self, root: Path):
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()

    async def put_async(self, task_id, data, filename, *, max_bytes=MAX_IMAGE_BYTES):
        return await finish_in_thread(lambda: self.put(task_id, data, filename, max_bytes=max_bytes))

    async def read_async(self, task_id, item):
        return await finish_in_thread(self.read, task_id, item)

    async def release_async(self, task_id):
        return await finish_in_thread(self.release, task_id)

    async def sweep_async(self, active_ids):
        return await finish_in_thread(self.sweep, active_ids)

    def directory(self, task_id: str) -> Path:
        if not re.fullmatch(r"rh-[A-Za-z0-9_-]{1,64}", task_id):
            raise ValueError("任务素材目录编号无效")
        directory = self.root / task_id
        if directory.is_symlink() or directory.resolve().parent != self.root:
            raise ValueError("任务素材目录不在允许范围内")
        return directory

    def put(self, task_id: str, data: bytes, filename: str, *, max_bytes=MAX_IMAGE_BYTES) -> dict:
        with self._lock:
            return self._put(task_id, data, filename, max_bytes=max_bytes)

    def _put(self, task_id, data, filename, *, max_bytes):
        if not data or len(data) > max_bytes:
            raise ValueError(f"参考素材为空或超过 {max_bytes // (1024 * 1024)}MB")
        directory = self.directory(task_id)
        digest = hashlib.sha256(data).hexdigest()
        path = directory / (digest + ".bin")
        if path.is_symlink():
            raise ValueError("任务素材文件不可为符号链接")
        if not path.exists():
            files = [p for p in self.root.glob("rh-*/*.bin") if not p.is_symlink() and p.resolve().is_relative_to(self.root)]
            if sum(p.stat().st_size for p in files) + len(data) > self.MAX_TOTAL_BYTES:
                raise ValueError("未完成任务的素材缓存已满，请先处理或取消旧任务")
            if sum(p.stat().st_size for p in files if p.parent == directory) + len(data) > self.MAX_TASK_BYTES:
                raise ValueError("单个任务的参考素材总大小超过 256MB")
            directory.mkdir(exist_ok=True)
            temporary = path.with_suffix(".tmp")
            if temporary.is_symlink():
                raise ValueError("任务素材临时文件不可为符号链接")
            try:
                temporary.write_bytes(data)
                temporary.replace(path)
            finally:
                temporary.unlink(missing_ok=True)
        return {"sha256": digest, "size": len(data), "filename": str(filename).replace("\\", "/").rsplit("/", 1)[-1], "max_bytes": max_bytes}

    def read(self, task_id: str, item: dict) -> bytes:
        with self._lock:
            return self._read(task_id, item)

    def _read(self, task_id, item):
        digest = str(item.get("sha256", ""))
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("任务素材校验信息无效")
        path = self.directory(task_id) / (digest + ".bin")
        max_bytes = min(512 * 1024 * 1024, int(item.get("max_bytes") or MAX_IMAGE_BYTES))
        if path.is_symlink() or not path.is_file() or path.stat().st_size > max_bytes:
            raise ValueError("任务保留的参考图片不可用，请取消后重新发送图片")
        data = path.read_bytes()
        if len(data) != item.get("size") or hashlib.sha256(data).hexdigest() != digest:
            raise ValueError("任务保留的参考图片校验失败")
        return data

    def release(self, task_id: str) -> None:
        with self._lock:
            self._release(task_id)

    def _release(self, task_id):
        directory = self.directory(task_id)
        if not directory.is_dir():
            return
        # No recursive removal: only our own content-addressed files qualify.
        for path in directory.iterdir():
            if re.fullmatch(r"[0-9a-f]{64}\.(bin|tmp)", path.name):
                path.unlink(missing_ok=True)
        if not any(directory.iterdir()):
            directory.rmdir()

    def sweep(self, active_ids: set[str]) -> None:
        with self._lock:
            for directory in self.root.glob("rh-*"):
                if directory.name not in active_ids and re.fullmatch(r"rh-[A-Za-z0-9_-]{1,64}", directory.name):
                    self.release(directory.name)
