"""消息投递与撤回通道。

设计目标：把「发文本 / 发图 / 发视频 / 撤回」从插件业务代码中剥离出来。

- ``DeliveryTarget``：描述一条消息要发到哪里（会话、群号、用户号、平台、事件）。
- ``BaseDeliveryChannel``：通道抽象接口。
- ``GenericAstrBotChannel``：所有平台的首选发送通道，使用 ``Context.send_message``。
- ``OneBotChannel``：OneBot v11（NapCat / Lagrange / go-cqhttp 等）原生通道，
  直接调用 ``send_group_msg`` / ``send_private_msg`` / ``delete_msg`` 并返回 ``message_id``，
  从而支持 AstrBot 通用接口不提供的撤回能力。
- ``Delivery``：根据目标自动选择通道。

maibot 原插件因为宿主没有撤回接口，只能在插件里硬编码多套 NapCat API 命名空间并
直发消息获取 message_id；迁移到 AstrBot 后，这一层收敛到本模块：发送优先走
AstrBot 通用通道，仅在需要 message_id 做撤回时才使用 OneBot 直发。后续适配其他
平台只需新增 Channel 实现并扩展 ``Delivery.channel_for``。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api.message_components import Video as VideoComponent
from astrbot.api.star import Context


@dataclass
class DeliveryTarget:
    """一条消息的目标会话。"""

    stream_id: str = ""
    group_id: str = ""
    user_id: str = ""
    platform_id: str = ""
    event: AstrMessageEvent | None = field(default=None, repr=False)

    @classmethod
    def from_event(cls, event: AstrMessageEvent) -> "DeliveryTarget":
        return cls(
            stream_id=str(event.unified_msg_origin or ""),
            group_id=str(event.get_group_id() or ""),
            user_id=str(event.get_sender_id() or ""),
            platform_id=str(event.get_platform_id() or ""),
            event=event,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "DeliveryTarget":
        data = data or {}
        return cls(
            stream_id=str(data.get("stream_id") or ""),
            group_id=str(data.get("group_id") or ""),
            user_id=str(data.get("user_id") or ""),
            platform_id=str(data.get("platform_id") or ""),
            event=data.get("event") if isinstance(data.get("event"), AstrMessageEvent) else None,
        )

    @classmethod
    def from_stream_id(cls, stream_id: str) -> "DeliveryTarget":
        return cls(stream_id=str(stream_id or ""))


class BaseDeliveryChannel(ABC):
    """消息投递通道抽象。所有发送方法失败时都应当自行消化异常并返回失败值。"""

    name: str = "base"

    @abstractmethod
    async def send_text(self, target: DeliveryTarget, text: str) -> bool:
        """发送文本。返回是否成功。"""

    @abstractmethod
    async def send_image(self, target: DeliveryTarget, image_base64: str) -> str:
        """发送图片。返回平台 message_id；拿不到时返回空字符串。"""

    @abstractmethod
    async def send_video(self, target: DeliveryTarget, video_url: str) -> str:
        """发送视频。返回平台 message_id；拿不到时返回空字符串。"""

    @abstractmethod
    async def recall(self, target: DeliveryTarget, message_id: str) -> bool:
        """撤回消息。返回是否成功。"""

    def supports_recall(self) -> bool:
        return False


class GenericAstrBotChannel(BaseDeliveryChannel):
    """AstrBot 通用通道：支持所有已接入平台，但拿不到 message_id、不能撤回。"""

    name = "astrbot_generic"

    def __init__(self, context: Context, logger: Any) -> None:
        self.context = context
        self.logger = logger

    async def send_text(self, target: DeliveryTarget, text: str) -> bool:
        if not target.stream_id or not str(text or ""):
            return False
        try:
            return bool(
                await self.context.send_message(
                    target.stream_id, MessageChain().message(str(text))
                )
            )
        except Exception as exc:
            self.logger.warning("通用通道发送文本失败: %s", exc)
            return False

    async def send_image(self, target: DeliveryTarget, image_base64: str) -> str:
        if not target.stream_id or not str(image_base64 or ""):
            return ""
        try:
            await self.context.send_message(
                target.stream_id, MessageChain().base64_image(str(image_base64))
            )
            return ""
        except Exception as exc:
            self.logger.warning("通用通道发送图片失败: %s", exc)
            return ""

    async def send_video(self, target: DeliveryTarget, video_url: str) -> str:
        if not target.stream_id or not str(video_url or ""):
            return ""
        try:
            await self.context.send_message(
                target.stream_id,
                MessageChain([VideoComponent.fromURL(str(video_url))]),
            )
            return ""
        except Exception as exc:
            self.logger.warning("通用通道发送视频失败，回退发送链接: %s", exc)
        try:
            await self.context.send_message(
                target.stream_id, MessageChain().message(str(video_url))
            )
            return ""
        except Exception as exc:
            self.logger.warning("通用通道发送视频链接失败: %s", exc)
            return ""

    async def recall(self, target: DeliveryTarget, message_id: str) -> bool:
        self.logger.debug("当前平台不支持通用撤回: message_id=%s", message_id)
        return False


class OneBotChannel(BaseDeliveryChannel):
    """OneBot v11 原生通道。

    通过 AstrBot aiocqhttp 适配器暴露的 ``CQHttp.call_action`` 直接与协议端通信，
    因此可以拿到发送返回的 message_id，并用 ``delete_msg`` 撤回。
    """

    name = "onebot_v11"

    def __init__(self, bot: Any, logger: Any) -> None:
        self.bot = bot
        self.logger = logger

    @staticmethod
    def _is_failed(response: Any) -> bool:
        if not isinstance(response, dict):
            return False
        retcode = response.get("retcode")
        if retcode is not None:
            try:
                if int(retcode) != 0:
                    return True
            except (TypeError, ValueError):
                pass
        status = str(response.get("status") or "").strip().lower()
        return status in {"failed", "error"}

    @staticmethod
    def _extract_message_id(response: Any) -> str:
        if not isinstance(response, dict):
            return ""
        data = response.get("data")
        if isinstance(data, dict):
            mid = data.get("message_id") or data.get("msg_id")
            if mid:
                return str(mid)
        result = response.get("result")
        if isinstance(result, dict):
            mid = result.get("message_id") or result.get("msg_id")
            if mid:
                return str(mid)
        mid = response.get("message_id") or response.get("msg_id")
        return str(mid) if mid else ""

    async def _call_action(self, action: str, params: dict) -> Any:
        try:
            call_action = getattr(self.bot, "call_action", None)
            if call_action is None:
                self.logger.warning("OneBot 客户端不支持 call_action: %s", action)
                return None
            response = await call_action(action, **params)
        except Exception as exc:
            self.logger.warning("OneBot 调用 %s 失败: %s", action, exc)
            return None
        if self._is_failed(response):
            self.logger.warning("OneBot 调用 %s 业务失败: %s", action, str(response)[:200])
            return None
        return response

    async def _send(self, target: DeliveryTarget, segments: list[dict]) -> str:
        group_id = str(target.group_id or "")
        user_id = str(target.user_id or "")
        try:
            if group_id:
                action = "send_group_msg"
                params: dict[str, Any] = {
                    "group_id": int(group_id),
                    "message": segments,
                }
            elif user_id:
                action = "send_private_msg"
                params = {"user_id": int(user_id), "message": segments}
            else:
                self.logger.warning("OneBot 直发缺少 group_id/user_id，改用通用通道")
                return ""
        except (TypeError, ValueError):
            self.logger.warning("group_id/user_id 不是数字，无法 OneBot 直发")
            return ""
        response = await self._call_action(action, params)
        return self._extract_message_id(response)

    async def send_text(self, target: DeliveryTarget, text: str) -> bool:
        if not str(text or ""):
            return False
        message_id = await self._send(
            target, [{"type": "text", "data": {"text": str(text)}}]
        )
        return bool(message_id)

    async def send_image(self, target: DeliveryTarget, image_base64: str) -> str:
        if not str(image_base64 or ""):
            return ""
        return await self._send(
            target,
            [{"type": "image", "data": {"file": f"base64://{image_base64}"}}],
        )

    async def send_video(self, target: DeliveryTarget, video_url: str) -> str:
        if not str(video_url or ""):
            return ""
        return await self._send(
            target, [{"type": "video", "data": {"file": str(video_url)}}]
        )

    async def recall(self, target: DeliveryTarget, message_id: str) -> bool:
        try:
            params = {"message_id": int(message_id)}
        except (TypeError, ValueError):
            self.logger.warning("message_id 不是数字，无法撤回: %s", message_id)
            return False
        try:
            call_action = getattr(self.bot, "call_action", None)
            if call_action is None:
                self.logger.warning("OneBot 客户端不支持 call_action: delete_msg")
                return False
            # aiocqhttp 的 call_action 成功后返回 data 字段；delete_msg 的 data 为 None，
            # 因此这里按“是否抛出 ActionFailed/网络异常”判断成功，而不是看返回值。
            await call_action("delete_msg", **params)
            return True
        except Exception as exc:
            self.logger.warning("OneBot delete_msg 调用失败: %s", exc)
            return False

    def supports_recall(self) -> bool:
        return True


class Delivery:
    """消息投递门面：发送优先 AstrBot 通用通道；需要 message_id 时选择 OneBot 通道。"""

    def __init__(self, context: Context, logger: Any) -> None:
        self.context = context
        self.logger = logger
        self._generic = GenericAstrBotChannel(context, logger)

    def get_onebot_client(self, target: DeliveryTarget) -> Any | None:
        """从目标事件或平台 ID 解析 OneBot 客户端；没有则返回 None。"""
        if target.event is not None:
            bot = getattr(target.event, "bot", None)
            if bot is not None:
                return bot
        platform_id = target.platform_id or (
            target.event.get_platform_id() if target.event is not None else ""
        )
        if not platform_id:
            return None
        try:
            platform = self.context.get_platform_inst(platform_id)
        except Exception:
            return None
        if platform is None:
            return None
        get_client = getattr(platform, "get_client", None)
        if get_client is None:
            return None
        try:
            return get_client()
        except Exception:
            return None

    def channel_for(self, target: DeliveryTarget) -> BaseDeliveryChannel:
        """通道选择逻辑集中在这里，后续新增平台只需扩展此方法。"""
        bot = self.get_onebot_client(target)
        if bot is not None:
            return OneBotChannel(bot, self.logger)
        return self._generic

    async def send_text(self, target: DeliveryTarget, text: str) -> bool:
        """发送文本。

        AstrBot 通用通道是首选（支持所有已接入平台）；通用通道失败时，
        且目标带 OneBot 群号/用户号，再尝试 OneBot 直发兜底。
        """
        if await self._generic.send_text(target, text):
            return True
        bot = self.get_onebot_client(target)
        if bot is not None and (target.group_id or target.user_id):
            return await OneBotChannel(bot, self.logger).send_text(target, text)
        return False

    async def send_image(
        self, target: DeliveryTarget, image_base64: str, *, need_message_id: bool = False
    ) -> str:
        """发送图片并返回 message_id。

        ``need_message_id=True``（需要撤回）时才走 OneBot 直发；否则优先
        AstrBot 通用通道。直发失败自动回退通用通道。
        """
        bot = self.get_onebot_client(target)
        if need_message_id and bot is not None and (target.group_id or target.user_id):
            message_id = await OneBotChannel(bot, self.logger).send_image(
                target, image_base64
            )
            if message_id:
                return message_id
            self.logger.debug("OneBot 直发图片失败或未返回 message_id，回退通用通道")
        await self._generic.send_image(target, image_base64)
        return ""

    async def send_video(
        self, target: DeliveryTarget, video_url: str, *, need_message_id: bool = False
    ) -> str:
        """发送视频并返回 message_id。

        ``need_message_id=True``（需要撤回）时才走 OneBot 直发；否则优先
        AstrBot 通用通道。直发失败自动回退通用通道。
        """
        bot = self.get_onebot_client(target)
        if need_message_id and bot is not None and (target.group_id or target.user_id):
            message_id = await OneBotChannel(bot, self.logger).send_video(
                target, video_url
            )
            if message_id:
                return message_id
            self.logger.debug("OneBot 直发视频失败或未返回 message_id，回退通用通道")
        await self._generic.send_video(target, video_url)
        return ""

    async def recall(self, target: DeliveryTarget, message_id: str) -> bool:
        """撤回消息；不支持撤回的通道返回 False。"""
        channel = self.channel_for(target)
        return await channel.recall(target, message_id)
