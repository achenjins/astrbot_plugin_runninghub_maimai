"""Bounded file reads with explicit trusted cache roots."""
from __future__ import annotations

import base64
import os
import re
import tempfile
from pathlib import Path
from urllib.parse import unquote, urlparse


def trusted_local_file(value, roots=()) -> Path | None:
    raw = str(value or "").strip()
    if not raw or "\x00" in raw:
        return None
    if raw.lower().startswith("file://"):
        parsed = urlparse(raw)
        if parsed.netloc not in ("", "localhost"):
            return None
        raw = unquote(parsed.path)
        if os.name == "nt" and re.match(r"^/[A-Za-z]:/", raw):
            raw = raw[1:]
    if raw.replace("/", "\\").startswith("\\\\"):
        return None
    try:
        path = Path(raw).resolve(strict=True)
        if not path.is_file():
            return None
        for root in [Path(tempfile.gettempdir()), *roots]:
            if path.is_relative_to(Path(root).resolve()):
                return path
    except (OSError, ValueError, RuntimeError):
        pass
    return None


def decode_base64_bounded(encoded: str, limit: int) -> bytes:
    if len(encoded) > ((limit + 2) // 3) * 4:
        raise ValueError("文件超过大小上限")
    data = base64.b64decode(encoded, validate=True)
    if len(data) > limit:
        raise ValueError("文件超过大小上限")
    return data


def image_mime(data: bytes) -> str:
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"\xff\xd8"):
        return "image/jpeg"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    return ""
