"""AstrBot image-size policy over the shared persistent image memory."""
from __future__ import annotations

from .upstream.image_memory import ImageMemory as _ImageMemory, short_description

MAX_IMAGE_BYTES = 20 * 1024 * 1024

__all__ = ["ImageMemory", "short_description"]


class ImageMemory(_ImageMemory):
    async def remember(self, user_id, stream_id, asset, data, description, limit):
        if isinstance(data, bytes) and len(data) > MAX_IMAGE_BYTES:
            raise ValueError("图片超过 20MB 保存上限")
        return await super().remember(user_id, stream_id, asset, data, description, limit)
