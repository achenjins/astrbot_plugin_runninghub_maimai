"""AstrBot metadata adapter over the upstream task journal and safe persistence."""
from __future__ import annotations

import copy
import time

from .upstream.task_journal import (
    ACTIVE_STATUSES, STATUS_CANCELLED, STATUS_FAILED, STATUS_PENDING, STATUS_SUCCESS,
    TaskJournal as _TaskJournal,
)

__all__ = ["TaskJournal", "ACTIVE_STATUSES", "STATUS_PENDING", "STATUS_SUCCESS",
           "STATUS_FAILED", "STATUS_CANCELLED"]


class TaskJournal(_TaskJournal):
    """Preserve AstrBot delivery targets, queued input pins, action dedup and panel state."""

    async def load(self):
        if self.loaded:
            return
        await super().load()
        # The upstream recovery hint names MaiBot's command. Keep storage logic
        # shared while presenting the command registered by this host.
        self._records = [{**record, "message": str(record.get("message", "")).replace("/rh补发", "/wf补发")}
                         for record in self._records]

    def _normalize_record(self, raw):
        record = super()._normalize_record(raw)
        for name in ("platform_id", "platform_name", "stage"):
            record[name] = str(raw.get(name) or "")
        for name in ("inputs", "actions"):
            record[name] = copy.deepcopy(raw[name]) if isinstance(raw.get(name), dict) else {}
        record["submitted_nodes"] = copy.deepcopy(raw["submitted_nodes"]) if isinstance(raw.get("submitted_nodes"), list) else []
        for name in ("inputs_ready", "hidden", "enhancement_attempted"):
            record[name] = bool(raw.get(name))
        for name in ("started_at", "finished_at", "busy_since"):
            record[name] = raw.get(name) or 0
        if record["status"] in {STATUS_SUCCESS, STATUS_FAILED, STATUS_CANCELLED} and not record["finished_at"]:
            record["finished_at"] = raw.get("updated_at") or time.time()
        return record

    async def mark_cancelled(self, task_id):
        await self.update(task_id, status=STATUS_CANCELLED, message="用户取消")

    def find_anchor_request(self, user_id, stream_id, anchor_id, workflow_id, region):
        """Retain the previous AstrBot lookup API; runtime request dedup is host-specific."""
        if not anchor_id:
            return None
        return next((record for record in self.records()
                     if record["user_id"] == user_id and record["stream_id"] == stream_id
                     and record.get("anchor_id") == anchor_id and record["region"] == region
                     and record["request"].get("workflow", {}).get("workflow_id") == workflow_id), None)
