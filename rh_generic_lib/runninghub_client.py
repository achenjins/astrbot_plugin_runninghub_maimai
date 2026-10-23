"""AstrBot compatibility surface for the unmodified upstream HTTP client."""
from __future__ import annotations

import asyncio
import base64
import copy
from typing import Any

import requests

from .upstream.runninghub_client import (
    RunningHubBusyError, RunningHubClient as _Client, RunningHubError,
    RunningHubRateLimitError, RunningHubTaskError, RunningHubTransportError,
)

__all__ = ["RunningHubClient", "RunningHubError", "RunningHubTransportError",
           "RunningHubBusyError", "RunningHubTaskError", "RunningHubRateLimitError"]

MAX_IMAGE_BYTES = 20 * 1024 * 1024


class RunningHubClient(_Client):
    """Keep AstrBot's account query and per-download limits without forking HTTP logic."""

    async def account_status(self) -> dict[str, Any]:
        if not self.api_key:
            raise RunningHubError("未配置 API Key")
        result = await self._post("/uc/openapi/accountStatus", {"apikey": self.api_key})
        if not isinstance(result, dict):
            raise RunningHubError("API 响应不是 JSON 对象")
        if result.get("code") not in (0, 200, None):
            raise RunningHubError(f"获取账户信息失败: {result.get('msg') or result.get('message') or result}")
        if not isinstance(result.get("data"), dict):
            raise RunningHubError("获取账户信息失败: 响应缺少 data")
        return result["data"]

    async def download_bytes(self, url: str, *, max_bytes: int | None = None) -> bytes:
        # A per-call view shares the Session, without racing another download's limit.
        view = copy.copy(self)
        if max_bytes is not None:
            view.max_file_bytes = min(self.max_file_bytes, max(1, int(max_bytes)))
        return await _Client.download_bytes(view, url)

    async def download_base64(self, url: str) -> str:
        data = await self.download_bytes(url, max_bytes=MAX_IMAGE_BYTES)
        return base64.b64encode(data).decode("ascii")

    async def wait_for_result(self, task_id: str, *, poll_interval: int | None = None,
                              max_wait: int | None = None) -> dict[str, Any]:
        try:
            return await super().wait_for_result(task_id, poll_interval=poll_interval, max_wait=max_wait)
        except RunningHubTaskError as exc:
            # Older AstrBot callers read task_status; upstream owns status and polling.
            exc.task_status = exc.status
            raise

    async def get_workflow_json(self, workflow_id: str) -> dict[str, Any]:
        try:
            return await super().get_workflow_json(workflow_id)
        except (AttributeError, TypeError, ValueError) as exc:
            raise RunningHubError("获取工作流失败: 响应结构异常") from exc
