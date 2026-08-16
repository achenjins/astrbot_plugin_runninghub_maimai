"""麦麦画师 · RunningHub（AstrBot 版通用工作流适配插件）。

由 maibot 插件迁移而来，适配 NapCat / OneBot v11 QQ 平台。

通过 AstrBot 配置适配 RunningHub 的大部分工作流：
- 可配置多个工作流（工作流 ID + 设备类型）
- 每个工作流可自由配置输入节点（节点 ID / 字段名 / 默认值 / 类型）
- 节点类型：prompt 主提示词 / text 可编辑配置 / default 固定默认值 / image / audio / video
- 文字节点可开启 LLM 扩写（可配置扩写模板文件）
- 图片/语音/视频节点支持交互式收集，可只传部分、发「跳过剩余」直接开始
- 可编辑配置（text 类型）固定在上传后询问用户确认/修改
- 命令 / LLM 工具 / Web API 三种触发方式，自动撤回保留（仅 NapCat 适配器生效）

命令：
- /wf运行 <工作流名> [描述文本]
- /wf工作流
- /wf国外工作流 <工作流ID> [名称] / /wf国内工作流 <工作流ID> [名称]
- /wf详细国外工作流 <工作流ID> [名称] / /wf详细国内工作流 <工作流ID> [名称]
- /wf中断
LLM 工具：run_workflow
Web API：POST /api/plug/runninghub_workflow_adapter/run_workflow_api
"""

from __future__ import annotations

import asyncio
import base64
import importlib
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import File as FileComponent
from astrbot.api.message_components import Image as ImageComponent
from astrbot.api.message_components import Record as RecordComponent
from astrbot.api.message_components import Video as VideoComponent
from astrbot.api.star import Context, Star
from astrbot.core.utils.astrbot_path import get_astrbot_data_path

_PLUGIN_DIR = Path(__file__).resolve().parent
_PLUGIN_PACKAGE = __package__ or ""

# AstrBot 重载插件时只会清理 ``data.plugins.<插件目录>.*`` 命名空间，不会清理
# 顶层 ``rh_generic_lib.*``。如果继续从顶层导入，升级后旧版配置模块会一直留在
# sys.modules 里，导致识别结果写入后 ``workflow_nodes`` 为空。
# 因此在 AstrBot 内运行时，统一从插件自身的包命名空间加载本地库，保证每次
# 重载/升级都取到当前插件目录里的代码；本地直接运行 / pytest 时回退顶层导入。
# 仅本地直跑时需要把插件目录放进 sys.path，AstrBot 下不污染其他插件的顶层导入。
if not _PLUGIN_PACKAGE and str(_PLUGIN_DIR) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_DIR))


def _load_local_module(module_name: str):
    """加载插件本地库模块（AstrBot 下使用插件包命名空间）。"""
    if _PLUGIN_PACKAGE:
        return importlib.import_module(f"{_PLUGIN_PACKAGE}.rh_generic_lib.{module_name}")
    return importlib.import_module(f"rh_generic_lib.{module_name}")


_config_lib = _load_local_module("config")
GenericConfig = _config_lib.GenericConfig
InputNodeSection = _config_lib.InputNodeSection
WorkflowItemSection = _config_lib.WorkflowItemSection
build_config_model = _config_lib.build_config_model
build_workflow_items = _config_lib.build_workflow_items
dump_config_dict = _config_lib.dump_config_dict
dump_workflow_items = _config_lib.dump_workflow_items

_delivery_lib = _load_local_module("delivery")
Delivery = _delivery_lib.Delivery
DeliveryTarget = _delivery_lib.DeliveryTarget

_legacy_lib = _load_local_module("legacy_config")
parse_legacy_toml = _legacy_lib.parse_legacy_toml

_client_lib = _load_local_module("runninghub_client")
RunningHubClient = _client_lib.RunningHubClient
RunningHubError = _client_lib.RunningHubError

_detect_lib = _load_local_module("workflow_detect")
LLM_DETECT_KEY_PROMPT = _detect_lib.LLM_DETECT_KEY_PROMPT
LLM_DETECT_PROMPT = _detect_lib.LLM_DETECT_PROMPT
detect_input_nodes = _detect_lib.detect_input_nodes
detect_key_nodes = _detect_lib.detect_key_nodes
describe_workflow_for_llm = _detect_lib.describe_workflow_for_llm
parse_llm_nodes = _detect_lib.parse_llm_nodes


__all__ = ["RunningHubGenericPlugin"]

# 交互式收集的等待超时（秒）
_INPUT_WAIT_TIMEOUT = 600

# 单个工作流的输入/配置节点总数上限（含参考图、配置节点，原 8 个对多参考图工作流不够）
_MAX_NODES = 32

# 上传/下载单个文件的最大字节数（512MB），防止异常或恶意超大内容撑爆内存
_MAX_FILE_BYTES = 512 * 1024 * 1024

# 交互收集会话中，用于"跳过剩余文件、直接开始运行"的触发词
_FINISH_KEYWORDS = {
    "完成", "开始", "开始运行", "运行", "提交", "结束",
    "跳过", "跳过剩余", "直接开始", "直接运行", "好了",
    "ok", "go", "done", "finish", "start",
}



@dataclass
class InputSession:
    """一次命令触发的交互式输入收集会话。"""

    user_id: str
    stream_id: str
    workflow: WorkflowItemSection
    waiting_nodes: list[dict[str, str]] = field(default_factory=list)
    collected: list[dict[str, str]] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    expire_task: asyncio.Task | None = None
    # 文字扩写延后到收集完成：记录原始文本、文字节点身份与实际上传的文件数量
    command_text: str = ""
    text_node_id: str = ""
    text_field_name: str = ""
    uploaded_images: int = 0
    uploaded_audios: int = 0
    uploaded_videos: int = 0
    # 收集阶段：files=等待文件上传；config=等待用户确认/修改可编辑配置
    phase: str = "files"
    editable_nodes: list[dict[str, str]] = field(default_factory=list)
    # 触发时的会话上下文（group_id/user_id），提交后用于 NapCat 直发与自动撤回
    chat_info: dict[str, str] = field(default_factory=dict)


class RunningHubGenericPlugin(Star):
    """麦麦画师 · RunningHub（AstrBot）插件主体。"""

    def __init__(self, context: Context, config: AstrBotConfig | None = None) -> None:
        super().__init__(context)
        self.logger = logger
        self.delivery = Delivery(context, logger)
        self._astrbot_config: AstrBotConfig | None = config
        self.config: GenericConfig = GenericConfig()
        self._apply_config_dict({} if config is None else dict(config))
        self._client: RunningHubClient | None = None
        self._client_cn: RunningHubClient | None = None
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(2)
        self._pending: dict[str, asyncio.Task] = {}
        self._recall_tasks: set[asyncio.Task] = set()
        self._input_sessions: dict[str, InputSession] = {}
        self._input_session_keys_by_stream: dict[str, set[str]] = {}
        self._input_session_keys_by_user: dict[str, set[str]] = {}
        self._config_write_lock: asyncio.Lock = asyncio.Lock()
        self._cleanup_task: asyncio.Task | None = None
        self._cache_dir: Path | None = None
        self._workflows: list[WorkflowItemSection] = []
        self._user_requests: dict[str, list[float]] = {}
        self._task_meta: dict[str, dict[str, str]] = {}
        self._cancel_choices: dict[str, list[str]] = {}

    def _event_ctx(self, event: AstrMessageEvent) -> dict[str, str]:
        """把 AstrBot 事件转换为业务层使用的扁平上下文。"""
        return {
            "stream_id": str(event.unified_msg_origin or ""),
            "user_id": str(event.get_sender_id() or ""),
            "group_id": str(event.get_group_id() or ""),
            "platform_id": str(event.get_platform_id() or ""),
        }

    def _mark_handled(self, event: AstrMessageEvent) -> None:
        """标记事件已被插件消费，阻止默认 LLM 流程继续响应。"""
        event.set_extra("runninghub_consumed", True)
        event.should_call_llm(True)
        event.stop_event()

    @staticmethod
    def _is_consumed(event: AstrMessageEvent) -> bool:
        """消息是否已被输入收集器等前置处理消费。"""
        return bool(event.get_extra("runninghub_consumed", False))

    # ── AstrBot 配置归一化 / 落盘 ────────────────────────────────

    @staticmethod
    def _normalise_workflow_items(raw_workflows: Any) -> list[dict[str, Any]]:
        """兼容别名：真实实现位于 rh_generic_lib.config。"""
        return build_workflow_items(raw_workflows)

    @staticmethod
    def _dump_workflow_items(items: list[WorkflowItemSection]) -> list[dict[str, Any]]:
        """兼容别名：真实实现位于 rh_generic_lib.config。"""
        return dump_workflow_items(items)

    def _dump_config_dict(self) -> dict[str, Any]:
        """生成与 _conf_schema.json 对齐的完整配置字典。"""
        return dump_config_dict(self.config)

    def _apply_config_dict(self, data: dict[str, Any]) -> None:
        """把 AstrBotConfig 字典归一化为强类型 GenericConfig。"""
        try:
            self.config = build_config_model(data)
        except Exception as exc:
            self.logger.warning("AstrBot 配置解析失败，使用默认配置: %s", exc)
            self.config = GenericConfig()

    async def _reload_from_context_config(self) -> None:
        if self._astrbot_config is not None:
            self._apply_config_dict(dict(self._astrbot_config))
        self._workflows = list(self.config.workflows.items)

    # ── 消息发送 / LLM 辅助 ────────────────────────────────────────

    async def _send_text(
        self, stream_id: str, text: str, chat_info: dict[str, Any] | None = None
    ) -> bool:
        """发送文本（统一走 Delivery 层）。"""
        target_data: dict[str, Any] = {"stream_id": stream_id}
        if chat_info:
            target_data.update(chat_info)
        return await self.delivery.send_text(DeliveryTarget.from_dict(target_data), text)

    async def _send_image(self, stream_id: str, image_base64: str) -> str:
        """发送图片，返回 message_id（通用平台为 ''）。"""
        return await self.delivery.send_image(
            DeliveryTarget.from_stream_id(stream_id), image_base64
        )

    async def _resolve_provider_id(self, stream_id: str, preferred: str) -> str:
        preferred = str(preferred or "").strip()
        if preferred:
            return preferred
        try:
            if stream_id:
                return await self.context.get_current_chat_provider_id(stream_id)
            provider = self.context.get_using_provider()
            return str(provider.meta().id) if provider is not None else ""
        except Exception as exc:
            self.logger.debug("解析 AstrBot 模型提供商失败: %s", exc)
            return ""

    async def _llm_generate(
        self,
        prompt: str,
        *,
        stream_id: str = "",
        provider_id: str = "",
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> dict[str, Any]:
        """调用 AstrBot LLM，返回与 maibot ctx.llm.generate 对齐的字典。"""
        provider = await self._resolve_provider_id(stream_id, provider_id)
        if not provider:
            return {"success": False, "response": "", "reasoning": "", "model": "", "error": "没有可用的聊天模型"}
        try:
            kwargs: dict[str, Any] = {}
            if temperature is not None:
                kwargs["temperature"] = temperature
            if max_tokens is not None:
                kwargs["max_tokens"] = max_tokens
            result = await self.context.llm_generate(
                chat_provider_id=provider,
                prompt=str(prompt),
                **kwargs,
            )
            return {
                "success": True,
                "response": str(getattr(result, "completion_text", "") or ""),
                "reasoning": str(getattr(result, "reasoning_content", "") or ""),
                "model": provider,
            }
        except Exception as exc:
            self.logger.warning("LLM 调用失败: %s", exc)
            return {"success": False, "response": "", "reasoning": "", "model": provider, "error": str(exc)}

    # ── 工作流配置访问 ────────────────────────────────────────────

    def _refresh_workflows(self) -> None:
        """从结构化配置同步工作流列表（pydantic 已校验，无需 TOML 解析）。"""
        self._workflows = list(self.config.workflows.items)
        self.logger.info("[配置] 已加载 %d 个工作流", len(self._workflows))

    def _workflow_names(self) -> list[str]:
        """返回当前已配置的工作流名称列表（配置未就绪时回退缓存）。"""
        try:
            return [str(w.name or "").strip() for w in self.config.workflows.items if str(w.name or "").strip()]
        except Exception:
            return [str(w.name or "").strip() for w in self._workflows if str(w.name or "").strip()]

    def _is_llm_callable_workflow(self, workflow: WorkflowItemSection) -> bool:
        """判断工作流是否支持 LLM 工具调用。

        仅支持「只有主提示词 + 可选固定默认值」的工作流：无文件节点（图片/音频/视频），
        无可编辑配置节点（text）。
        """
        prompt_count = 0
        for node in self._ordered_nodes(workflow):
            vtype = self._resolve_value_type(node)
            if vtype == "prompt":
                prompt_count += 1
            elif vtype in ("image", "audio", "video", "text"):
                return False
        return prompt_count == 1

    def _llm_callable_workflow_names(self) -> list[str]:
        """返回支持 LLM 工具调用的工作流名称列表。"""
        try:
            workflows = list(self.config.workflows.items)
        except Exception:
            workflows = list(self._workflows)
        return [
            str(w.name or "").strip()
            for w in workflows
            if str(w.name or "").strip() and self._is_llm_callable_workflow(w)
        ]

    # ── 生命周期 ──────────────────────────────────────────────────

    def _describe_workflows(self) -> list[str]:
        """生成当前配置的工作流摘要（供日志输出）。"""
        lines: list[str] = []
        if not self._workflows:
            return ["  （无）"]
        for workflow in self._workflows:
            nodes = [n for n in workflow.input_nodes if str(n.node_id or "").strip()]
            lines.append(
                f"  - {workflow.name}（id={workflow.workflow_id} 设备={workflow.instance_type} 节点={len(nodes)}）"
            )
            for node in nodes:
                vtype = self._resolve_value_type(node)
                lines.append(
                    f"      node={node.node_id} field={node.field_name} type={vtype} value={node.field_value!r}"
                )
        return lines

    def _import_legacy_config_if_needed(self) -> None:
        """首次部署时，如果插件目录里还带着旧 config.toml，自动迁入 AstrBot 配置。"""
        if self._astrbot_config is None:
            return
        legacy_path = _PLUGIN_DIR / "config.toml"
        if not legacy_path.is_file():
            return
        current = dict(self._astrbot_config)
        server = current.get("server") if isinstance(current.get("server"), dict) else {}
        has_workflows = bool(current.get("workflows"))
        has_keys = bool(server.get("api_key") or server.get("api_key_cn"))
        first_deploy = bool(getattr(self._astrbot_config, "first_deploy", False))
        if not first_deploy and (has_workflows or has_keys):
            return
        try:
            converted = parse_legacy_toml(legacy_path)
            self._astrbot_config.save_config(converted)
            self.logger.info(
                "[配置] 检测到旧版 config.toml，已自动迁移到 AstrBot 配置（%d 个工作流）",
                len(converted.get("workflows") or []),
            )
        except Exception as exc:
            self.logger.warning("[配置] 旧版 config.toml 迁移失败，请手动在 WebUI 配置: %s", exc)

    def _migrate_embedded_nodes_to_workflow_nodes(self) -> None:
        """把旧版嵌入在 workflow 项里的输入节点迁移为 workflow_nodes 配置。"""
        if self._astrbot_config is None:
            return
        workflows = self._astrbot_config.get("workflows")
        if not isinstance(workflows, list):
            return
        has_embedded = any(
            isinstance(wf, dict)
            and (
                "input_nodes" in wf
                or "input_nodes_extra" in wf
                or any(f"input_node_{index}" in wf for index in range(1, 9))
            )
            for wf in workflows
        )
        if not has_embedded or self._astrbot_config.get("workflow_nodes"):
            return
        migrated = dump_config_dict(self.config)
        self._astrbot_config.save_config(migrated)
        self._apply_config_dict(migrated)
        self.logger.info(
            "[配置] 已将旧版嵌入输入节点迁移为 workflow_nodes（%d 条）",
            len(migrated.get("workflow_nodes") or []),
        )

    async def initialize(self) -> None:
        """插件激活时调用（对应 maibot 的 on_load）。"""
        self._import_legacy_config_if_needed()
        await self._reload_from_context_config()
        self._migrate_embedded_nodes_to_workflow_nodes()
        self.logger.info(
            "[配置] 本地配置库已加载: package=%s file=%s",
            _PLUGIN_PACKAGE or "(顶层)",
            _config_lib.__file__,
        )
        cfg = self.config
        self._semaphore = asyncio.Semaphore(max(1, cfg.generation.max_concurrent))
        self._rebuild_client()
        self._refresh_workflows()
        self._refresh_llm_tool_description()


        if not cfg.server.api_key and not cfg.server.api_key_cn:
            self.logger.warning("未配置 RunningHub API Key，请在 AstrBot 插件配置中填写 server.api_key")
        self._validate_workflows()

        self._cleanup_task = asyncio.create_task(self._cleanup_cache_loop())
        self.context.register_web_api(
            "/runninghub_workflow_adapter/run_workflow_api",
            self.handle_run_workflow_api,
            ["POST"],
            "运行配置好的 RunningHub 工作流（供其他插件 / WebUI 调用）",
        )
        self.context.register_web_api(
            "/runninghub_workflow_adapter/page/config",
            self.handle_page_get_config,
            ["GET"],
            "可视化页面：读取工作流与输入节点",
        )
        self.context.register_web_api(
            "/runninghub_workflow_adapter/page/config",
            self.handle_page_save_config,
            ["POST"],
            "可视化页面：保存工作流与输入节点",
        )
        self.context.register_web_api(
            "/runninghub_workflow_adapter/page/analyze",
            self.handle_page_analyze_workflow,
            ["POST"],
            "可视化页面：拉取并识别工作流输入节点",
        )
        self.context.register_web_api(
            "/runninghub_workflow_adapter/page/prompt-templates",
            self.handle_page_list_prompt_templates,
            ["GET"],
            "可视化页面：列出扩写提示词模板",
        )
        self.context.register_web_api(
            "/runninghub_workflow_adapter/page/prompt-templates",
            self.handle_page_upload_prompt_template,
            ["POST"],
            "可视化页面：上传扩写提示词模板",
        )
        self.context.register_web_api(
            "/runninghub_workflow_adapter/page/prompt-template",
            self.handle_page_read_prompt_template,
            ["GET"],
            "可视化页面：读取扩写提示词模板内容",
        )

        self.logger.info(
            "麦麦画师插件已加载：base_url=%s 工作流数量=%d",
            cfg.server.base_url,
            len(self._workflows),
        )
        for line in self._describe_workflows():
            self.logger.info("[配置] %s", line)

    async def terminate(self) -> None:
        """插件停用 / 重载时调用（对应 maibot 的 on_unload）。"""
        cleanup_task = self._cleanup_task
        self._cleanup_task = None

        poll_tasks = list(self._pending.values())
        recall_tasks = list(self._recall_tasks)
        expire_tasks = [
            session.expire_task
            for session in self._input_sessions.values()
            if session.expire_task is not None
        ]
        tasks_to_stop = poll_tasks + recall_tasks + expire_tasks
        if cleanup_task is not None:
            tasks_to_stop.append(cleanup_task)
        for task in tasks_to_stop:
            task.cancel()
        if tasks_to_stop:
            await asyncio.gather(*tasks_to_stop, return_exceptions=True)

        self._pending.clear()
        self._recall_tasks.clear()
        self._input_sessions.clear()
        self._input_session_keys_by_stream.clear()
        self._input_session_keys_by_user.clear()
        self._task_meta.clear()
        self._cancel_choices.clear()
        self._client = None
        self._client_cn = None
        self.logger.info("麦麦画师插件已卸载")

    # ── 缓存清理 ──────────────────────────────────────────────────

    def _get_cache_dir(self) -> Path | None:
        """返回插件临时缓存目录（AstrBot data/temp 下），不可用时返回 None。"""
        if self._cache_dir is not None:
            return self._cache_dir
        try:
            cache_dir = Path(get_astrbot_data_path()) / "temp" / "runninghub_workflow_adapter"
            cache_dir.mkdir(parents=True, exist_ok=True)
            self._cache_dir = cache_dir
        except Exception as exc:
            self.logger.warning("创建缓存目录失败，缓存清理将跳过: %s", exc)
            self._cache_dir = None
        return self._cache_dir

    async def _cleanup_cache_loop(self) -> None:
        """定时清理缓存目录（保留 24 小时内文件，每 6 小时执行一次）。"""
        interval = 6 * 3600
        max_age = 24 * 3600
        while True:
            try:
                await asyncio.sleep(5)
                self._cleanup_cache_once(max_age_seconds=max_age)
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("缓存清理异常: %s", exc)
                await asyncio.sleep(interval)

    def _cleanup_cache_once(self, *, max_age_seconds: int) -> None:
        """清理缓存目录中超过保留时间的文件（同步，经 to_thread 调用更佳）。"""
        cache_dir = self._get_cache_dir()
        if cache_dir is None:
            return
        try:
            now = time.time()
            removed = 0
            for item in cache_dir.iterdir():
                try:
                    if item.is_file() and now - item.stat().st_mtime > max_age_seconds:
                        item.unlink()
                        removed += 1
                except OSError:
                    continue
            if removed:
                self.logger.info("缓存清理完成，删除 %d 个过期文件", removed)
        except OSError as exc:
            self.logger.warning("缓存清理失败: %s", exc)

    # ── 配置校验 ──────────────────────────────────────────────────

    def _validate_workflows(self) -> None:
        """校验配置约束：总节点最多 32 个、无默认值的文字节点仅一个生效。"""
        for workflow in self._workflows:
            nodes = [n for n in workflow.input_nodes if str(n.node_id or "").strip()]
            if len(nodes) > _MAX_NODES:
                self.logger.warning(
                    "工作流 %s 输入节点 %d 个，超过 %d 个上限，多余节点将被忽略",
                    workflow.name, len(nodes), _MAX_NODES,
                )
            empty_text_nodes = [
                n for n in nodes
                if not str(n.field_value or "").strip()
                and self._resolve_value_type(n) == "text"
            ]
            if len(empty_text_nodes) > 1:
                self.logger.warning(
                    "工作流 %s 有 %d 个无默认值的文字节点，仅第一个接收命令文本，其余将被跳过",
                    workflow.name,
                    len(empty_text_nodes),
                )

    # ── 内部工具方法 ──────────────────────────────────────────────

    def _rebuild_client(self) -> None:
        cfg = self.config
        kwargs = {
            "workflow_id": "",
            "timeout": cfg.generation.download_timeout,
            "poll_interval": cfg.generation.poll_interval,
            "max_wait": cfg.generation.max_wait,
        }
        self._client = RunningHubClient(
            base_url=cfg.server.base_url, api_key=cfg.server.api_key, **kwargs
        )
        self._client_cn = RunningHubClient(
            base_url=cfg.server.base_url_cn, api_key=cfg.server.api_key_cn, **kwargs
        )

    def _get_client(self, region: str) -> RunningHubClient | None:
        """按区域返回对应客户端（overseas/domestic）。"""
        if region == "domestic":
            return self._client_cn
        return self._client

    def _find_workflow(self, name: str) -> WorkflowItemSection | None:
        """按名称查找工作流配置。"""
        name = str(name or "").strip()
        if not name:
            return None
        for workflow in self._workflows:
            if workflow.name.strip() == name:
                return workflow
        return None

    def _check_access(self, user_id: str, group_id: str) -> tuple[bool, str]:
        """访问控制：白名单 + 每用户每小时频率限制。

        默认（未配置任何限制）返回 (True, "")，与旧版行为完全一致；
        配置后才按白名单/频率拦截，返回 (False, 提示信息)。
        """
        cfg = self.config.access
        uid = str(user_id or "").strip()
        gid = str(group_id or "").strip()

        if cfg.allow_users:
            allowed_users = {str(u).strip() for u in cfg.allow_users if str(u).strip()}
            if not uid or uid not in allowed_users:
                return False, "你没有使用本插件的权限"

        if cfg.allow_groups and gid:
            allowed_groups = {str(g).strip() for g in cfg.allow_groups if str(g).strip()}
            if gid not in allowed_groups:
                return False, "当前群组没有使用本插件的权限"

        if cfg.max_per_user_per_hour > 0:
            if not uid:
                return False, "无法识别用户身份，已阻止本次请求（已开启频率限制）"
            now = time.time()
            bucket = self._user_requests.setdefault(uid, [])
            bucket[:] = [t for t in bucket if now - t < 3600]
            if len(bucket) >= cfg.max_per_user_per_hour:
                return False, "你本小时的生成次数已达上限，请稍后再试"
            # 计数移到提交成功后（_submit_and_poll），失败 / 识别等非生成请求不占额度
            # 桶数超阈值时清理空桶，避免一次性用户导致字典无限增长
            if len(self._user_requests) > 128:
                self._user_requests = {k: v for k, v in self._user_requests.items() if v}

        return True, ""

    def _check_access_from_kwargs(self, kwargs: dict[str, Any]) -> tuple[bool, str]:
        """从命令 kwargs 提取 user_id/group_id 并做访问控制检查。"""
        chat_info = self._extract_chat_info(kwargs)
        group_id = str(chat_info.get("group_id") or "")
        user_id = str(kwargs.get("user_id") or chat_info.get("user_id") or "")
        return self._check_access(user_id, group_id)

    def _is_admin(self, user_id: str) -> bool:
        """判断用户是否为管理员（可中断所有人的任务）。"""
        uid = str(user_id or "").strip()
        if not uid:
            return False
        admins = {str(a).strip() for a in self.config.access.admin_users if str(a).strip()}
        return uid in admins

    def _ordered_nodes(self, workflow: WorkflowItemSection) -> list[InputNodeSection]:
        """按配置顺序返回有效节点（最多 _MAX_NODES 个）。"""
        return [n for n in workflow.input_nodes if str(n.node_id or "").strip()][:_MAX_NODES]

    def _load_llm_template(self, workflow: WorkflowItemSection) -> str:
        """读取工作流配置的 LLM 扩写模板。"""
        template_path = str(workflow.llm_template_path or "").strip()
        if not template_path:
            return ""
        resolved = Path(template_path)
        if not resolved.is_absolute():
            resolved = _PLUGIN_DIR / resolved
        try:
            return resolved.read_text(encoding="utf-8")
        except OSError as exc:
            self.logger.warning("读取扩写模板失败: %s（%s）", resolved, exc)
            return ""

    def _describe_file_inputs(self, workflow: WorkflowItemSection) -> str:
        """汇总该工作流需要用户上传的文件输入（图片/音频/视频的种类与数量）。

        仅在未提供实际上传数量时作为兜底，告知工作流所需的文件节点。
        """
        images = [
            n for n in workflow.input_nodes
            if not str(n.field_value or "").strip() and self._resolve_value_type(n) == "image"
        ]
        audios = [
            n for n in workflow.input_nodes
            if not str(n.field_value or "").strip() and self._resolve_value_type(n) == "audio"
        ]
        videos = [
            n for n in workflow.input_nodes
            if not str(n.field_value or "").strip() and self._resolve_value_type(n) == "video"
        ]
        parts: list[str] = []
        if images:
            labels = "、".join(str(n.label or "").strip() or str(n.node_id) for n in images)
            parts.append(f"参考图片 {len(images)} 张（{labels}）")
        if audios:
            labels = "、".join(str(n.label or "").strip() or str(n.node_id) for n in audios)
            parts.append(f"参考音频 {len(audios)} 段（{labels}）")
        if videos:
            labels = "、".join(str(n.label or "").strip() or str(n.node_id) for n in videos)
            parts.append(f"参考视频 {len(videos)} 段（{labels}）")
        return "；".join(parts) if parts else ""

    @staticmethod
    def _format_file_counts(images: int, audios: int, videos: int = 0) -> str:
        """按实际上传数量生成简短描述（0 的类别省略）。"""
        parts: list[str] = []
        if images:
            parts.append(f"参考图片 {images} 张")
        if audios:
            parts.append(f"参考音频 {audios} 段")
        if videos:
            parts.append(f"参考视频 {videos} 段")
        return "；".join(parts)

    def _prompt_nodes(self, workflow: WorkflowItemSection) -> list[InputNodeSection]:
        """返回所有主提示词节点（prompt 类型，最多允许一个）。"""
        return [n for n in self._ordered_nodes(workflow) if self._resolve_value_type(n) == "prompt"]

    def _first_prompt_node(self, workflow: WorkflowItemSection) -> InputNodeSection | None:
        """返回第一个 prompt 节点（用户文本/扩写结果的回填目标，不关心是否有默认值）。"""
        for node in self._ordered_nodes(workflow):
            if self._resolve_value_type(node) == "prompt":
                return node
        return None

    def _primary_prompt_node(self, workflow: WorkflowItemSection) -> InputNodeSection | None:
        """返回第一个无默认值的主提示词节点（接收命令/扩写文本的节点）。"""
        for node in self._ordered_nodes(workflow):
            if self._resolve_value_type(node) == "prompt" and not str(node.field_value or "").strip():
                return node
        return None

    @staticmethod
    def _patch_text_value(
        node_info_list: list[dict[str, str]],
        node_id: str,
        field_name: str,
        text: str,
    ) -> list[dict[str, str]]:
        """回填文字节点的 fieldValue；列表中不存在该节点时追加（如交互补充的描述）。"""
        for entry in node_info_list:
            if entry.get("nodeId") == node_id and entry.get("fieldName") == field_name:
                entry["fieldValue"] = text
                return node_info_list
        node_info_list.append(
            {"nodeId": node_id, "fieldName": field_name, "fieldValue": text}
        )
        return node_info_list

    async def _enhance_text(
        self,
        workflow: WorkflowItemSection,
        text: str,
        *,
        actual_file_desc: str | None = None,
        stream_id: str = "",
    ) -> str:
        """按工作流配置对文字进行 LLM 扩写（失败回退原文）。

        actual_file_desc 传实际的"参考图片 N 张；参考音频 M 段"描述；
        为 None 时回退为工作流配置的文件节点汇总。
        """
        text = str(text or "").strip()
        if not text or not workflow.llm_enhance:
            return text
        template = self._load_llm_template(workflow)
        if not template:
            self.logger.warning("工作流 %s 开启 LLM 扩写但模板为空，使用原文", workflow.name)
            return text
        if actual_file_desc is None:
            actual_file_desc = self._describe_file_inputs(workflow)
        input_context = (
            f"本次任务将使用以下文件输入：{actual_file_desc}。"
            if actual_file_desc
            else "本次任务无额外文件输入。"
        )
        prompt_text = (
            f"{template}\n\n"
            f"{input_context}\n\n"
            f"<USER_REQUIREMENT>\n{text}\n</USER_REQUIREMENT>\n"
            "请严格按模板输出最终内容，不要输出任何额外解释"
        )
        try:
            result = await self._llm_generate(
                prompt=prompt_text,
                stream_id=stream_id,
                provider_id=self.config.feature.enhance_model,
            )
        except Exception as exc:
            self.logger.warning("LLM 扩写失败，回退原文: %s", exc)
            return text
        if not isinstance(result, dict) or not result.get("success"):
            return text
        return str(result.get("response") or "").strip() or text

    @staticmethod
    def _resolve_value_type(node: InputNodeSection) -> str:
        """解析节点类型：显式选择优先，留空时按字段名自动推断。"""
        explicit = str(node.value_type or "").strip().lower()
        if explicit in ("default", "text", "image", "audio", "video", "prompt"):
            return explicit
        field_name = str(node.field_name or "").lower()
        if any(k in field_name for k in ("image", "pic", "photo", "img")):
            return "image"
        if any(k in field_name for k in ("audio", "voice", "sound", "music", "speech")):
            return "audio"
        if any(k in field_name for k in ("video", "mp4", "mov", "webm", "clip")):
            return "video"
        return "text"

    def _build_node_info_list(
        self,
        workflow: WorkflowItemSection,
        command_text: str,
        *,
        enhanced_text: str | None = None,
    ) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
        """构建 nodeInfoList 并返回需要等待用户输入的节点列表。

        规则（新节点类型语义）：
        - prompt：主提示词，优先使用命令/扩写文本；没有输入时回退节点默认值；仅第一个接收文本
        - text：可编辑配置，先用默认值（可为空），上传文件后询问用户修改
        - default：固定默认值，不询问；无默认值时跳过
        - image / audio：有默认值直接使用；无默认值按顺序等待上传

        Returns:
            (node_info_list, waiting_nodes)：已确定的节点参数与待收集节点
            （waiting 元素为 dict：node/field_name/value_type/label）。
        """
        nodes = self._ordered_nodes(workflow)
        text_to_fill = enhanced_text if enhanced_text is not None else command_text
        text_filled = False

        node_info_list: list[dict[str, str]] = []
        waiting: list[dict[str, Any]] = []
        for node in nodes:
            field_value = str(node.field_value or "")
            vtype = self._resolve_value_type(node)
            node_id = node.node_id.strip()
            field_name = node.field_name.strip() or "prompt"

            if vtype == "prompt":
                # 主提示词：用户输入/扩写文本优先，没有输入时回退到节点默认值；
                # 文本只填第一个 prompt 节点，后续 prompt 节点按默认值处理。
                if not text_filled and text_to_fill:
                    node_info_list.append(
                        {"nodeId": node_id, "fieldName": field_name, "fieldValue": text_to_fill}
                    )
                    text_filled = True
                elif field_value:
                    node_info_list.append(
                        {"nodeId": node_id, "fieldName": field_name, "fieldValue": field_value}
                    )
                else:
                    self.logger.info("主提示词节点 %s 未接收文本且无默认值，已跳过", node_id)
                continue

            if vtype == "text":
                # 可编辑配置：先用默认值（可为空），上传文件后询问用户修改
                node_info_list.append(
                    {"nodeId": node_id, "fieldName": field_name, "fieldValue": field_value}
                )
                continue

            if vtype == "default":
                # 固定默认值：有值直接使用，无值跳过
                if field_value:
                    node_info_list.append(
                        {"nodeId": node_id, "fieldName": field_name, "fieldValue": field_value}
                    )
                else:
                    self.logger.info("节点 %s 类型为默认值但未填写输入内容，已跳过", node_id)
                continue

            # image / audio
            if field_value:
                node_info_list.append(
                    {"nodeId": node_id, "fieldName": field_name, "fieldValue": field_value}
                )
                continue
            waiting.append(
                {
                    "node": node,
                    "node_id": node_id,
                    "field_name": field_name,
                    "value_type": vtype,
                    "label": node.label.strip() or node_id,
                }
            )
        return node_info_list, waiting

    def _editable_config_nodes(self, workflow: WorkflowItemSection) -> list[dict[str, str]]:
        """返回上传文件后需要询问用户修改的可编辑配置节点（text 类型）。"""
        result: list[dict[str, str]] = []
        for node in self._ordered_nodes(workflow):
            if self._resolve_value_type(node) != "text":
                continue
            node_id = node.node_id.strip()
            result.append(
                {
                    "node_id": node_id,
                    "field_name": node.field_name.strip() or "prompt",
                    "field_value": str(node.field_value or ""),
                    "label": str(node.label or "").strip() or node_id,
                }
            )
        return result

    async def _start_workflow(
        self,
        workflow_name: str,
        command_text: str = "",
        **kwargs: Any,
    ) -> dict[str, Any]:
        """查找工作流，构建节点参数，提交任务或进入交互式收集。"""
        stream_id = str(kwargs.pop("stream_id", "") or "")
        command_text = str(command_text or "").strip()
        chat_info = self._extract_chat_info(kwargs)
        group_id = str(chat_info.get("group_id") or "")
        user_id = str(kwargs.get("user_id") or chat_info.get("user_id") or "")
        # 回填 kwargs，保证下游（任务元信息 / 撤回 / 中断权限）能拿到正确上下文
        kwargs["user_id"] = user_id
        kwargs["group_id"] = group_id
        kwargs["platform_id"] = str(kwargs.get("platform_id") or chat_info.get("platform_id") or "")
        kwargs.setdefault("trigger", "command")
        allowed, deny_msg = self._check_access(user_id, group_id)
        if not allowed:
            return {"success": False, "message": deny_msg}

        workflow = self._find_workflow(workflow_name)
        if workflow is None:
            available = "、".join(w.name for w in self._workflows if w.name) or "（空）"
            return {"success": False, "message": f"未找到工作流「{workflow_name}」，已配置：{available}"}

        if not workflow.workflow_id.strip():
            return {"success": False, "message": f"工作流「{workflow.name}」未配置 workflow_id"}

        # 按工作流区域选择对应客户端（国外/国内）
        region = str(workflow.region or "overseas").strip()
        client = self._get_client(region)
        if client is None:
            self._rebuild_client()
            client = self._get_client(region)
        if client is None:
            return {"success": False, "message": "插件客户端未初始化，请检查配置"}

        if not self.config.server.api_key and not self.config.server.api_key_cn:
            return {"success": False, "message": "未配置 RunningHub API Key，请在 AstrBot 插件配置中填写 server.api_key"}
        region_label = "国内" if region == "domestic" else "国外"
        region_key = self.config.server.api_key_cn if region == "domestic" else self.config.server.api_key
        if not region_key:
            return {"success": False, "message": f"该工作流为{region_label}，但对应 API Key 未填写，请检查配置"}

        prompt_nodes = self._prompt_nodes(workflow)
        if len(prompt_nodes) > 1:
            return {
                "success": False,
                "message": f"工作流「{workflow.name}」配置了 {len(prompt_nodes)} 个主提示词节点（prompt 类型），仅允许一个",
            }

        text_node = self._primary_prompt_node(workflow)
        text_target = self._first_prompt_node(workflow)
        editable_nodes = self._editable_config_nodes(workflow)
        # 存在无默认值的 prompt 节点且用户没给描述：不能直接提交，先交互收集描述
        missing_prompt_text = bool(text_node is not None and not command_text)
        # 会话中需要回填文字的目标节点：给了文本或需要补文本时才记录
        session_text_node = text_target if (command_text or missing_prompt_text) else None

        # 先用原始文本构建节点参数（文字节点暂填原文，扩写见下）
        node_info_list, waiting = self._build_node_info_list(workflow, command_text)

        if not node_info_list and not waiting and not editable_nodes and not missing_prompt_text:
            return {"success": False, "message": f"工作流「{workflow.name}」未配置任何输入节点"}

        if missing_prompt_text:
            # 有文件先收文件（收完再问描述）；没有文件则直接进入描述输入阶段
            session = self._create_input_session(
                user_id=user_id,
                stream_id=stream_id,
                workflow=workflow,
                waiting_nodes=waiting,
                collected=node_info_list,
                command_text=command_text,
                text_node_id=session_text_node.node_id.strip() if session_text_node else "",
                text_field_name=session_text_node.field_name.strip() if session_text_node else "",
                editable_nodes=editable_nodes,
                chat_info=chat_info,
                phase="files" if waiting else "text",
            )
            if waiting:
                tips = self._build_waiting_tips(waiting)
                required_files = [
                    {"type": item["value_type"], "label": item["label"]}
                    for item in waiting
                ]
                return {
                    "success": True,
                    "waiting": True,
                    "required_files": required_files,
                    "message": f"请上传：{tips}（可只传部分，发「跳过剩余」直接开始；上传后还需补充描述文本）",
                }
            return {
                "success": True,
                "waiting": True,
                "required_files": [],
                "message": f"工作流「{workflow.name}」需要描述文本，请直接发送要生成的内容",
            }

        if waiting or editable_nodes:
            # 固定流程：有文件先收文件，收完（或直接）进入可编辑配置确认
            session = self._create_input_session(
                user_id=user_id,
                stream_id=stream_id,
                workflow=workflow,
                waiting_nodes=waiting,
                collected=node_info_list,
                command_text=command_text,
                text_node_id=session_text_node.node_id.strip() if session_text_node else "",
                text_field_name=session_text_node.field_name.strip() if session_text_node else "",
                editable_nodes=editable_nodes,
                chat_info=chat_info,
            )
            if waiting:
                tips = self._build_waiting_tips(waiting)
                required_files = [
                    {"type": item["value_type"], "label": item["label"]}
                    for item in waiting
                ]
                return {
                    "success": True,
                    "waiting": True,
                    "required_files": required_files,
                    "message": f"请上传：{tips}（可只传部分，发「跳过剩余」直接开始）",
                }
            # 无文件但需确认可编辑配置：直接进入配置确认
            await self._ask_config_edit(session, stream_id)
            return {"success": True, "waiting": True, "required_files": [], "message": "请确认配置"}

        # 无文件、无可编辑配置：立即扩写并回填文字节点（用户输入优先，目标为第一个 prompt 节点）
        if command_text and text_target and workflow.llm_enhance:
            enhanced_text = await self._enhance_text(workflow, command_text, stream_id=stream_id)
            self._patch_text_value(
                node_info_list,
                text_target.node_id.strip(),
                text_target.field_name.strip(),
                enhanced_text,
            )

        return await self._submit_and_poll(client, workflow, node_info_list, stream_id, kwargs)

    async def _submit_and_poll(
        self,
        client: RunningHubClient,
        workflow: WorkflowItemSection,
        node_info_list: list[dict[str, str]],
        stream_id: str,
        kwargs: dict,
    ) -> dict[str, Any]:
        """提交任务并启动后台轮询。"""
        try:
            await self._semaphore.acquire()
            task_id = await client.submit(
                node_info_list,
                instance_type=workflow.instance_type,
                workflow_id=workflow.workflow_id.strip(),
            )
        except RunningHubError as exc:
            self._semaphore.release()
            self.logger.error("提交任务失败: %s", exc)
            return {"success": False, "message": f"提交任务失败：{exc}"}
        except Exception as exc:
            self._semaphore.release()
            self.logger.error("提交任务异常: %s", exc, exc_info=True)
            return {"success": False, "message": f"提交任务异常：{exc}"}

        # 提交成功才计入每用户每小时频率（失败 / 识别等非生成请求不占额度）
        if self.config.access.max_per_user_per_hour > 0:
            uid = str(kwargs.get("user_id") or "").strip()
            if uid:
                now = time.time()
                bucket = self._user_requests.setdefault(uid, [])
                bucket[:] = [t for t in bucket if now - t < 3600]
                bucket.append(now)

        self.logger.info(
            "任务已提交: task_id=%s workflow=%s nodes=%d",
            task_id,
            workflow.name,
            len(node_info_list),
        )
        poll_task = asyncio.create_task(
            self._poll_and_send(task_id, stream_id, client=client, kwargs=kwargs)
        )
        self._pending[task_id] = poll_task
        self._task_meta[task_id] = {
            "name": str(workflow.name or workflow.workflow_id),
            "stream_id": stream_id,
            "region": str(workflow.region or "overseas").strip(),
            "user_id": str(kwargs.get("user_id") or ""),
            "platform_id": str(kwargs.get("platform_id") or ""),
        }
        return {
            "success": True,
            "task_id": task_id,
            "message": "好的，任务已开始运行，请稍等",
        }

    # ── 交互式输入收集 ────────────────────────────────────────────

    @staticmethod
    def _build_waiting_tips(waiting: list[dict[str, Any]]) -> str:
        """构建等待上传的提示文本（按类型汇总剩余数量与说明）。"""
        return RunningHubGenericPlugin._format_waiting_summary(waiting)

    @staticmethod
    def _format_waiting_summary(waiting: list[dict[str, Any]]) -> str:
        _NAME_UNIT = {"image": ("图片", "张"), "audio": ("音频", "段"), "video": ("视频", "段")}
        counts: dict[str, int] = {}
        order: list[str] = []
        for item in waiting:
            vtype = item["value_type"]
            if vtype not in counts:
                counts[vtype] = 0
                order.append(vtype)
            counts[vtype] += 1
        parts: list[str] = []
        for vtype in order:
            name, unit = _NAME_UNIT.get(vtype, (vtype, "个"))
            parts.append(f"{name} {counts[vtype]} {unit}")
        return "、".join(parts)

    def _create_input_session(
        self,
        *,
        user_id: str,
        stream_id: str,
        workflow: WorkflowItemSection,
        waiting_nodes: list[dict[str, Any]],
        collected: list[dict[str, str]],
        command_text: str = "",
        text_node_id: str = "",
        text_field_name: str = "",
        editable_nodes: list[dict[str, str]] | None = None,
        chat_info: dict[str, str] | None = None,
        phase: str = "files",
    ) -> InputSession:
        """创建交互式收集会话（同一用户可在不同会话各有一份，工具路径回退按 stream 定位）。"""
        session = InputSession(
            user_id=user_id,
            stream_id=stream_id,
            workflow=workflow,
            waiting_nodes=[
                {
                    "node_id": item["node_id"],
                    "field_name": item["field_name"],
                    "value_type": item["value_type"],
                    "label": item["label"],
                }
                for item in waiting_nodes
            ],
            collected=collected,
            command_text=command_text,
            text_node_id=text_node_id,
            text_field_name=text_field_name,
            editable_nodes=editable_nodes or [],
            chat_info=chat_info or {},
            phase=phase,
        )
        key = self._register_input_session(session)

        async def _expire() -> None:
            await asyncio.sleep(_INPUT_WAIT_TIMEOUT)
            if self._input_sessions.get(key) is session:
                self._remove_input_session(key)
                if stream_id:
                    try:
                        await self._send_text(stream_id, "输入等待已超时，本次任务已取消")
                    except Exception:
                        pass

        session.expire_task = asyncio.create_task(_expire())
        return session

    @staticmethod
    def _session_key(user_id: str, stream_id: str) -> str:
        """会话键：user_id + stream_id 共同区分，避免同用户跨会话、同群多用户互相覆盖。"""
        uid = str(user_id or "").strip()
        sid = str(stream_id or "").strip()
        if uid and sid:
            return f"{uid}:{sid}"
        if sid:
            return f"stream:{sid}"
        if uid:
            return f"user:{uid}"
        return "anonymous"

    def _register_input_session(self, session: InputSession) -> str:
        """把会话写入主表与 user/stream 索引，返回会话键。"""
        key = self._session_key(session.user_id, session.stream_id)
        old_session = self._input_sessions.get(key)
        if old_session is not None and old_session is not session and old_session.expire_task is not None:
            old_session.expire_task.cancel()
        # 重新插入以更新注册顺序，保证“最近会话”回退按最新触发优先
        self._input_sessions.pop(key, None)
        self._input_sessions[key] = session
        if session.stream_id:
            self._input_session_keys_by_stream.setdefault(session.stream_id, set()).add(key)
        if session.user_id:
            self._input_session_keys_by_user.setdefault(session.user_id, set()).add(key)
        return key

    def _remove_input_session(self, key: str) -> InputSession | None:
        """从主表与索引中删除会话，保持三张表一致。"""
        session = self._input_sessions.pop(key, None)
        if session is None:
            return None
        if session.stream_id:
            stream_keys = self._input_session_keys_by_stream.get(session.stream_id)
            if stream_keys is not None:
                stream_keys.discard(key)
                if not stream_keys:
                    self._input_session_keys_by_stream.pop(session.stream_id, None)
        if session.user_id:
            user_keys = self._input_session_keys_by_user.get(session.user_id)
            if user_keys is not None:
                user_keys.discard(key)
                if not user_keys:
                    self._input_session_keys_by_user.pop(session.user_id, None)
        return session

    def _latest_session_for_keys(self, keys: set[str]) -> InputSession | None:
        """从会话键集合中返回最近注册的会话（注册顺序，避免同秒时间戳不稳定）。"""
        for key in reversed(self._input_sessions):
            if key in keys:
                return self._input_sessions[key]
        return None

    def _find_input_session(self, user_id: str, stream_id: str) -> InputSession | None:
        """按 user_id + stream_id 精确查找；降级时不得跨用户取同群其他人的会话。"""
        user_id = str(user_id or "").strip()
        stream_id = str(stream_id or "").strip()

        if user_id and stream_id:
            session = self._input_sessions.get(self._session_key(user_id, stream_id))
            if session is not None:
                return session
            # 该流里存在匿名会话（工具/API 路径创建）时允许按 stream 命中；
            # 否则不跨会话/跨用户回退，避免把文件误投到其他会话
            anonymous_key = f"stream:{stream_id}"
            if anonymous_key in self._input_sessions:
                return self._input_sessions[anonymous_key]
            return None
        if stream_id:
            stream_keys = self._input_session_keys_by_stream.get(stream_id)
            if stream_keys:
                # 工具路径创建的匿名流会话优先精确命中
                anonymous_key = f"stream:{stream_id}"
                if anonymous_key in stream_keys:
                    return self._input_sessions.get(anonymous_key)
                return self._latest_session_for_keys(stream_keys)
        if user_id:
            user_keys = self._input_session_keys_by_user.get(user_id)
            if user_keys:
                return self._latest_session_for_keys(user_keys)
        return None

    async def _handle_incoming_files(
        self, user_id: str, stream_id: str, event: AstrMessageEvent
    ) -> bool:
        """处理交互式收集中的文件消息，返回是否已消费该消息。"""
        session = self._find_input_session(user_id, stream_id)
        if session is None:
            return False
        key = self._session_key(session.user_id, session.stream_id)

        files = await self._extract_files_from_event(event)
        if not files:
            await self._send_text(
                stream_id,
                "未识别到图片、语音或视频文件，请直接发送文件（不要带文字）；"
                "或发送「跳过剩余」直接开始运行",
            )
            return True

        region = str(session.workflow.region or "overseas").strip()
        client = self._get_client(region)
        if client is None:
            self._rebuild_client()
            client = self._get_client(region)
        if client is None:
            self._cancel_input_session(key)
            await self._send_text(stream_id, "插件客户端未初始化，已取消本次任务")
            return True

        return await self._consume_files(session, key, files, stream_id, client)

    async def _consume_files(
        self,
        session: InputSession,
        key: str,
        files: list[tuple[str, str]],
        stream_id: str,
        client: RunningHubClient,
    ) -> bool:
        """把 files 列表按类型分配到等待节点并上传，返回是否已消费。"""
        for file_type, source in files:
            index = next(
                (i for i, n in enumerate(session.waiting_nodes) if n["value_type"] == file_type),
                None,
            )
            if index is None:
                await self._send_text(stream_id, f"当前已不需要{type_name_of(file_type)}文件，已忽略")
                continue
            node = session.waiting_nodes.pop(index)
            try:
                file_data = await self._fetch_file_bytes(source)
                filename = self._guess_filename(source, file_type, file_data)
                file_name = await client.upload_file(file_data, filename)
            except Exception as exc:
                self.logger.error("上传文件到 RunningHub 失败: %s", exc)
                await self._send_text(stream_id, f"文件上传失败：{exc}")
                session.waiting_nodes.insert(index, node)
                continue
            session.collected.append(
                {
                    "nodeId": node["node_id"],
                    "fieldName": node["field_name"],
                    "fieldValue": file_name,
                }
            )
            if file_type == "image":
                session.uploaded_images += 1
            elif file_type == "audio":
                session.uploaded_audios += 1
            elif file_type == "video":
                session.uploaded_videos += 1
            self.logger.info("已接收输入 %s: %s", node["label"], file_name)

        if session.waiting_nodes:
            await self._send_text(
                stream_id,
                f"已收到，还剩余：{self._build_waiting_tips_from_dicts(session.waiting_nodes)}（或发「跳过剩余」）",
            )
            return True

        await self._after_files_collected(session, key, stream_id, client, "输入已收齐")
        return True

    async def _ask_config_edit(self, session: InputSession, stream_id: str, notice: str = "") -> None:
        """进入可编辑配置确认阶段并向用户发确认提示。"""
        session.phase = "config"
        tips = self._build_config_edit_tips(session.editable_nodes)
        prefix = f"{notice}。" if notice else ""
        await self._send_text(
            stream_id,
            f"{prefix}可修改：\n{tips}\n（回复新值，如「512 16:9」，- 保持默认，「不变」全默认）",
        )

    async def _after_files_collected(
        self,
        session: InputSession,
        key: str,
        stream_id: str,
        client: RunningHubClient,
        notice: str,
    ) -> None:
        """文件/描述收集结束后：需要描述先问描述，再有可编辑配置则进入确认，否则提交。"""
        if not session.command_text and session.text_node_id:
            session.phase = "text"
            await self._send_text(
                stream_id,
                f"{notice}。请补充描述文本（将填入提示词节点，直接发送文字即可）：",
            )
            return
        if session.editable_nodes:
            await self._ask_config_edit(session, stream_id, notice)
            return
        await self._submit_collected_session(session, key, stream_id, client, notice + "，开始运行")

    @staticmethod
    def _build_config_edit_tips(editable_nodes: list[dict[str, str]]) -> str:
        """构建可编辑配置的确认提示。"""
        lines = []
        for index, node in enumerate(editable_nodes, 1):
            lines.append(f"{index}.{node['label']}：{node['field_value']}")
        return "\n".join(lines)

    @staticmethod
    def _parse_config_edit(text: str, count: int) -> list[str | None]:
        """解析用户对可编辑配置的回复，返回与 editable_nodes 对齐的值列表。

        元素为 None 表示保持默认；回复「不变/跳过/默认」等返回空列表（全部保持默认）。
        """
        normalized = str(text or "").strip()
        if normalized in ("", "不变", "跳过", "跳过剩余", "默认", "确认", "ok", "go", "好了", "不修改"):
            return []
        tokens = re.split(r"[\s,，、]+", normalized)
        values: list[str | None] = []
        for token in tokens[:count]:
            if token in ("-", "不变", "默认", "保持", "跳过"):
                values.append(None)
            else:
                values.append(token)
        return values

    async def _handle_text_input(
        self, session: InputSession, stream_id: str, event: AstrMessageEvent
    ) -> None:
        """处理描述文本输入阶段：文本写入命令文本，然后继续配置确认或提交。"""
        text = self._extract_text_from_event(event).strip()
        if not text:
            if await self._extract_files_from_event(event):
                await self._send_text(
                    stream_id,
                    "现在是描述输入阶段，请先发送要生成的内容文字；参考文件等描述确认后再传",
                )
            else:
                await self._send_text(stream_id, "请直接发送要生成的描述文本（例如：一只在窗边的猫）")
            return
        if self._is_finish_signal(text):
            await self._send_text(
                stream_id,
                "该工作流需要描述文本才能运行，不能跳过；请直接发送要生成的内容",
            )
            return

        session.command_text = text
        region = str(session.workflow.region or "overseas").strip()
        client = self._get_client(region)
        if client is None:
            self._rebuild_client()
            client = self._get_client(region)
        if client is None:
            key = self._session_key(session.user_id, session.stream_id)
            self._cancel_input_session(key)
            await self._send_text(stream_id, "插件客户端未初始化，已取消本次任务")
            return
        key = self._session_key(session.user_id, session.stream_id)
        await self._after_files_collected(session, key, stream_id, client, "描述已更新")

    async def _handle_config_edit(
        self, session: InputSession, stream_id: str, event: AstrMessageEvent
    ) -> None:
        """处理可编辑配置的确认/修改回复。"""
        text = self._extract_text_from_event(event)
        # 配置阶段只接受文字：误发文件（无文字）时提示，不要当成「不变」直接提交
        if not text.strip() and await self._extract_files_from_event(event):
            await self._send_text(
                stream_id,
                "现在是配置确认阶段，请回复数值（如「512 16:9」）或「不变」；图片等参考文件请留到下次任务再传",
            )
            return
        values = self._parse_config_edit(text, len(session.editable_nodes))
        for index, node in enumerate(session.editable_nodes):
            if index < len(values) and values[index] is not None:
                self._patch_text_value(
                    session.collected, node["node_id"], node["field_name"], values[index]
                )
        region = str(session.workflow.region or "overseas").strip()
        client = self._get_client(region)
        if client is None:
            self._rebuild_client()
            client = self._get_client(region)
        if client is None:
            key = self._session_key(session.user_id, session.stream_id)
            self._cancel_input_session(key)
            await self._send_text(stream_id, "插件客户端未初始化，已取消本次任务")
            return
        key = self._session_key(session.user_id, session.stream_id)
        await self._submit_collected_session(session, key, stream_id, client, "配置已更新，开始运行")

    async def _submit_collected_session(
        self,
        session: InputSession,
        key: str,
        stream_id: str,
        client: RunningHubClient,
        notice: str,
    ) -> None:
        """提交已收集的输入（会话已从 _input_sessions 移除）。"""
        self._remove_input_session(key)
        if session.expire_task is not None:
            session.expire_task.cancel()

        # 文字扩写延后到此刻：用实际上传的文件数量重新扩写并回填文字节点；
        # 交互补充的描述此时可能还没有对应条目，_patch_text_value 会自动追加。
        if session.command_text and session.text_node_id:
            enhanced = session.command_text
            if session.workflow.llm_enhance:
                actual_desc = self._format_file_counts(
                    session.uploaded_images, session.uploaded_audios, session.uploaded_videos
                )
                enhanced = await self._enhance_text(
                    session.workflow,
                    session.command_text,
                    actual_file_desc=actual_desc,
                    stream_id=stream_id,
                )
            session.collected = self._patch_text_value(
                session.collected,
                session.text_node_id,
                session.text_field_name,
                enhanced,
            )

        await self._send_text(stream_id, notice)
        # 用触发时的 chat_info 构造扁平 kwargs，_extract_chat_info 能识别，恢复 NapCat 直发与自动撤回
        kwargs = {
            "group_id": str(session.chat_info.get("group_id") or ""),
            "user_id": str(session.chat_info.get("user_id") or ""),
            "platform_id": str(session.chat_info.get("platform_id") or ""),
        }
        result = await self._submit_and_poll(
            client, session.workflow, session.collected, stream_id, kwargs
        )
        if not result["success"]:
            await self._send_text(stream_id, result["message"])
        else:
            await self._send_text(stream_id, result["message"])

    async def _finish_input_session(
        self,
        user_id: str,
        stream_id: str,
        *,
        skip_remaining: bool = True,
    ) -> bool:
        """跳过剩余文件节点，用已收集的输入直接提交；返回是否已消费该消息。"""
        session = self._find_input_session(user_id, stream_id)
        if session is None:
            return False
        key = self._session_key(session.user_id, session.stream_id)
        region = str(session.workflow.region or "overseas").strip()
        client = self._get_client(region)
        if client is None:
            self._rebuild_client()
            client = self._get_client(region)
        if client is None:
            self._cancel_input_session(key)
            await self._send_text(stream_id, "插件客户端未初始化，已取消本次任务")
            return True
        skipped = len(session.waiting_nodes)
        if skipped:
            notice = f"已跳过剩余 {skipped} 个文件"
        else:
            notice = "输入已收齐"
        await self._after_files_collected(session, key, stream_id, client, notice)
        return True

    def _cancel_input_session(self, key: str) -> None:
        session = self._remove_input_session(key)
        if session is not None and session.expire_task is not None:
            session.expire_task.cancel()

    def _build_waiting_tips_from_dicts(self, waiting: list[dict[str, str]]) -> str:
        return self._format_waiting_summary(waiting)

    async def _extract_files_from_event(self, event: AstrMessageEvent) -> list[tuple[str, str]]:
        """从 AstrBot 消息事件提取文件，返回 [(类型 image/audio/video, 来源)]。

        图片/语音优先取 base64，其次 URL / 本地路径；文件消息按文件名推断类型。
        """
        files: list[tuple[str, str]] = []
        for comp in event.get_messages():
            try:
                if isinstance(comp, ImageComponent):
                    source = str(comp.file or comp.url or "").strip()
                    if source:
                        source = source.removeprefix("file:///")
                        if source.startswith("base64://") or source.startswith(("http://", "https://")) or Path(source).is_file():
                            files.append(("image", source))
                        else:
                            b64 = await comp.convert_to_base64()
                            files.append(("image", "base64://" + b64))
                elif isinstance(comp, RecordComponent):
                    source = str(comp.file or comp.url or "").strip()
                    if source:
                        source = source.removeprefix("file:///")
                        if source.startswith("base64://") or source.startswith(("http://", "https://")) or Path(source).is_file():
                            files.append(("audio", source))
                        else:
                            b64 = await comp.convert_to_base64()
                            files.append(("audio", "base64://" + b64))
                elif isinstance(comp, VideoComponent):
                    source = str(comp.file or "").strip()
                    if source:
                        source = source.removeprefix("file:///")
                        files.append(("video", source))
                elif isinstance(comp, FileComponent):
                    name = str(comp.name or "").strip()
                    source = await comp.get_file(allow_return_url=True)
                    source = source.removeprefix("file:///")
                    file_type = self._detect_file_type_from_name(name or source)
                    if source:
                        files.append((file_type, source))
            except Exception as exc:
                self.logger.warning("解析消息中的文件失败: %s", exc)
        return files

    @staticmethod
    def _detect_file_type_from_name(name: str) -> str:
        """根据文件名 / URL 的扩展名推断文件类型（image / audio / video）。

        QQ「文件」消息（type=file）不区分图片 / 音频 / 视频，统一走这里按扩展名判断，
        否则以文件形式发的图片 / 音频会被当成视频而匹配不到对应节点。
        """
        path = str(name or "").split("?", 1)[0].strip().lower()
        if path.endswith((
            ".png", ".jpg", ".jpeg", ".jpe", ".jfif", ".webp", ".gif", ".bmp",
            ".tif", ".tiff", ".ico", ".heic", ".heif", ".avif", ".jxl", ".svg", ".raw", ".dib",
        )):
            return "image"
        if path.endswith((
            ".mp3", ".wav", ".flac", ".aac", ".m4a", ".m4r", ".ogg", ".oga", ".opus",
            ".wma", ".amr", ".silk", ".aiff", ".aif", ".ape", ".alac", ".wv",
            ".mp2", ".mpga", ".ac3", ".mka", ".mid", ".midi",
        )):
            return "audio"
        return "video"

    @staticmethod
    def _extract_text_from_event(event: AstrMessageEvent) -> str:
        """从 AstrBot 消息事件提取纯文本内容。"""
        return str(getattr(event, "message_str", "") or "").strip()

    @staticmethod
    def _is_finish_signal(text: str) -> bool:
        """判断文本是否为"跳过剩余文件、直接开始运行"的触发词。

        去掉前导斜杠、中文引号/括号等包裹符后再匹配，兼容「跳过剩余」/『跳过剩余』/（跳过剩余）等写法。
        """
        _STRIP = "/「」『』【】()（）[]\"'，。！!?？：: "
        normalized = str(text or "").strip().strip(_STRIP).lower()
        if not normalized:
            return False
        if normalized in _FINISH_KEYWORDS:
            return True
        return normalized.startswith("跳过") or normalized.startswith("开始运行")

    async def _fetch_file_bytes(self, source: str) -> bytes:
        """从 base64 数据、URL 或本地路径获取文件字节（带大小上限）。

        注意：本地路径（如适配器传入的 /data/voice.amr 或缓存文件）是合法来源，
        必须保留；这里只限制大小，不限制来源类型。
        """
        if source.startswith("base64://"):
            import base64

            encoded = source[len("base64://"):]
            # base64 解码后约 3/4 大小，先按编码长度预估，避免解码超大内容
            if len(encoded) > _MAX_FILE_BYTES * 4 // 3:
                raise RunningHubError(f"上传内容超过 {_MAX_FILE_BYTES} 字节上限，已拒绝")
            return base64.b64decode(encoded)
        if source.startswith(("http://", "https://")):
            client = self._client or self._client_cn
            if client is None:
                raise RunningHubError("客户端未初始化")
            data = await client.download_bytes(source)
            return data
        path = Path(source)
        if path.is_file():
            if path.stat().st_size > _MAX_FILE_BYTES:
                raise RunningHubError(f"文件超过 {_MAX_FILE_BYTES} 字节上限，已拒绝: {source}")
            return await asyncio.to_thread(path.read_bytes)
        raise RunningHubError(f"无法读取文件: {source}")

    @staticmethod
    def _guess_filename(source: str, file_type: str, file_data: bytes | None = None) -> str:
        """根据来源/字节猜测文件名（含扩展名，图片按魔数识别真实格式）。"""
        base = source.split("?", 1)[0].rsplit("/", 1)[-1]
        if base and "." in base and not base.startswith("base64:"):
            return base
        ext = ""
        if file_type == "image" and file_data:
            if file_data[:3] == b"\xff\xd8\xff":
                ext = ".jpg"
            elif file_data[:8] == b"\x89PNG\r\n\x1a\n":
                ext = ".png"
            elif len(file_data) >= 12 and file_data[:4] == b"RIFF" and file_data[8:12] == b"WEBP":
                ext = ".webp"
            elif file_data[:6] in (b"GIF87a", b"GIF89a"):
                ext = ".gif"
        if not ext:
            ext = {"image": ".png", "audio": ".mp3", "video": ".mp4"}.get(file_type, ".bin")
        return f"input_{file_type}_{int(time.time())}{ext}"




    # ── 轮询发送 / 撤回 ──────────────────────────────────────────

    async def _poll_and_send(
        self,
        task_id: str,
        stream_id: str,
        *,
        client: RunningHubClient | None = None,
        kwargs: dict | None = None,
    ) -> None:
        """后台轮询任务状态，完成后下载并发送结果；按配置定时撤回。

        结果按类型分流：图片直接发送；其他类型（视频等）发送下载链接。
        """
        client = client or self._client
        chat_info = self._extract_chat_info(kwargs or {})
        try:
            try:
                result = await client.wait_for_result(task_id)
            except (RunningHubError, TimeoutError) as exc:
                self.logger.error("任务 %s 未成功完成: %s", task_id, exc)
                if stream_id:
                    await self._send_text(stream_id, "哦不好意思，任务运行失败了")
                return

            result_items: list[tuple[str, str]] = []
            for item in result.get("results") or []:
                if not isinstance(item, dict):
                    continue
                url = str(item.get("url") or item.get("outputUrl") or item.get("fileUrl") or "").strip()
                if not url:
                    continue
                output_type = str(
                    item.get("outputType") or item.get("fileType") or ""
                ).strip().lower()
                result_items.append((url, output_type))
            if not result_items:
                if stream_id:
                    await self._send_text(stream_id, "哦不好意思，任务没有返回结果")
                return

            cleanup_cfg = self.config.feature
            recall_seconds = cleanup_cfg.recall_seconds
            should_cleanup = bool(cleanup_cfg.enable and recall_seconds and recall_seconds > 0)

            appended_result = False
            for index, (url, output_type) in enumerate(result_items):
                if self._is_image_url(url, output_type):
                    try:
                        image_base64 = await client.download_base64(url)
                    except Exception as exc:
                        self.logger.error("下载结果失败 %s: %s", url, exc)
                        if stream_id:
                            await self._send_text(stream_id, f"第 {index + 1} 个结果下载失败：{exc}")
                        continue
                    if stream_id:
                        message_id = await self._send_image_with_id(
                            image_base64,
                            stream_id,
                            chat_info=chat_info,
                            need_message_id=should_cleanup,
                        )
                        self.logger.info(
                            "已发送结果 %d/%d (task_id=%s message_id=%s)",
                            index + 1,
                            len(result_items),
                            task_id,
                            message_id or "无",
                        )
                        if should_cleanup and message_id:
                            self._schedule_recall(
                                message_id, recall_seconds, platform_id=chat_info.get("platform_id")
                            )
                        # 追加到 LLM 聊天上下文：让 LLM 能看到并记住自己生成的图片
                        await self._append_result_to_llm_context(
                            stream_id,
                            [{"type": "image", "binary_data_base64": image_base64, "description": "RunningHub 生成结果"}],
                            visible_text="[生成结果] 图片已生成",
                        )
                        appended_result = True
                elif self._is_video_url(url, output_type) and stream_id:
                    video_message_id = await self._send_video_with_id(
                        url, stream_id, chat_info=chat_info, need_message_id=should_cleanup
                    )
                    if should_cleanup and video_message_id:
                        self._schedule_recall(
                            video_message_id, recall_seconds, platform_id=chat_info.get("platform_id")
                        )
                    # 视频无法直接给 LLM 看，追加链接文本，让 LLM 知道生成了什么
                    await self._append_result_to_llm_context(
                        stream_id,
                        [{"type": "text", "data": url}],
                        visible_text=f"[生成结果] 视频已生成：{url}",
                    )
                    appended_result = True
                elif stream_id:
                    await self._send_text(stream_id, f"任务结果 {index + 1}：{url}")

            # 命令路径追加一条确认消息；LLM 工具路径由 Agent 继续生成回复
            if appended_result and str(kwargs.get("trigger") or "") != "tool":
                await self._trigger_llm_result_reply(stream_id)
        except asyncio.CancelledError:
            self.logger.info("任务 %s 已被取消", task_id)
            raise
        except Exception as exc:
            self.logger.error("任务 %s 处理异常: %s", task_id, exc, exc_info=True)
            if stream_id:
                await self._send_text(stream_id, "哦不好意思，处理结果时出了点问题")
        finally:
            self._pending.pop(task_id, None)
            self._task_meta.pop(task_id, None)
            self._semaphore.release()

    async def _append_result_to_llm_context(
        self, stream_id: str, segments: list[dict[str, Any]], visible_text: str
    ) -> None:
        """AstrBot 没有 Maisaka context.append 能力，这里仅记录日志，不打断结果发送。"""
        self.logger.debug("[上下文] %s (stream=%s)", visible_text, stream_id)

    async def _trigger_llm_result_reply(self, stream_id: str) -> None:
        """生成结果全部发出后追加一条确认消息。

        maibot 原版通过 Maisaka 主动回复让 LLM 用角色口吻确认；AstrBot 侧
        简化为插件直接发一句确认，避免与 Agent 回复打架。可通过 feature.result_notice 关闭。
        """
        if not stream_id or not self.config.feature.result_notice:
            return
        await self._send_text(stream_id, "生成好啦，请查收～")

    @staticmethod
    def _is_image_url(url: str, output_type: str = "") -> bool:
        """判断结果是否指向图片：优先信 RunningHub 的 outputType，否则按扩展名粗判。"""
        normalized = str(output_type or "").strip().lower()
        if normalized in ("image", "png", "jpg", "jpeg", "webp", "gif", "bmp"):
            return True
        if normalized in ("video", "mp4", "mov", "webm", "avi", "mkv", "flv", "m4v", "mpg", "mpeg", "3gp", "wmv"):
            return False
        path = str(url or "").split("?", 1)[0].lower()
        return path.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp"))

    @staticmethod
    def _is_video_url(url: str, output_type: str = "") -> bool:
        """判断结果是否指向视频：优先信 RunningHub 的 outputType，否则按扩展名粗判。"""
        normalized = str(output_type or "").strip().lower()
        if normalized in ("video", "mp4", "mov", "webm", "avi", "mkv", "flv", "m4v", "mpg", "mpeg", "3gp", "wmv"):
            return True
        if normalized in ("image", "png", "jpg", "jpeg", "webp", "gif", "bmp"):
            return False
        path = str(url or "").split("?", 1)[0].lower()
        return path.endswith((".mp4", ".mov", ".webm", ".avi", ".mkv", ".flv", ".m4v", ".mpg", ".mpeg", ".3gp", ".wmv"))

    @staticmethod
    def _extract_chat_info(kwargs: dict) -> dict:
        """从调用参数中提取群号/用户号/平台 ID，用于 OneBot 直发与撤回。"""
        group_id = str(kwargs.get("group_id") or "")
        user_id = str(kwargs.get("user_id") or "")
        platform_id = str(kwargs.get("platform_id") or "")
        return {
            "group_id": group_id,
            "user_id": user_id,
            "chat_type": "group" if group_id else "private",
            "platform_id": platform_id,
        }

    async def _send_image_with_id(
        self,
        image_base64: str,
        stream_id: str,
        *,
        chat_info: dict,
        need_message_id: bool = False,
    ) -> str:
        """发送图片；仅 need_message_id=True 时走 OneBot 直发拿 message_id。"""
        target_data: dict[str, Any] = {"stream_id": stream_id}
        target_data.update(chat_info or {})
        return await self.delivery.send_image(
            DeliveryTarget.from_dict(target_data),
            image_base64,
            need_message_id=need_message_id,
        )

    async def _send_video_with_id(
        self,
        video_url: str,
        stream_id: str,
        *,
        chat_info: dict,
        need_message_id: bool = False,
    ) -> str:
        """发送视频；仅 need_message_id=True 时走 OneBot 直发拿 message_id。"""
        target_data: dict[str, Any] = {"stream_id": stream_id}
        target_data.update(chat_info or {})
        return await self.delivery.send_video(
            DeliveryTarget.from_dict(target_data),
            video_url,
            need_message_id=need_message_id,
        )

    def _schedule_recall(self, message_id: str, delay_seconds: int, *, platform_id: str = "") -> None:
        """调度一个延时撤回任务，并保存引用防止被回收。"""
        task = asyncio.create_task(
            self._delayed_recall(message_id, delay_seconds, platform_id=platform_id)
        )
        self._recall_tasks.add(task)
        task.add_done_callback(self._recall_tasks.discard)

    async def _delayed_recall(
        self, message_id: str, delay_seconds: int, *, platform_id: str = ""
    ) -> None:
        """延迟指定秒数后撤回消息（仅 OneBot 通道生效），失败时重试一次。"""
        target = DeliveryTarget(stream_id="", platform_id=platform_id)
        await asyncio.sleep(delay_seconds)
        self.logger.info("开始撤回消息: message_id=%s", message_id)
        try:
            for attempt in (1, 2):
                ok = await self.delivery.recall(target, message_id)
                if ok:
                    self.logger.info("已撤回消息 %s", message_id)
                    return
                self.logger.warning(
                    "撤回消息 %s 失败（第 %d 次，通道不支持或调用失败）", message_id, attempt
                )
                if attempt == 1:
                    await asyncio.sleep(5)
            self.logger.error("撤回消息 %s 两次尝试均失败", message_id)
        except asyncio.CancelledError:
            self.logger.info("撤回任务已取消: message_id=%s", message_id)
            raise
        except Exception as exc:
            self.logger.warning("撤回消息 %s 失败: %s", message_id, exc)

    # ── 命令 / 工具 / API 组件 ────────────────────────────────────

    @filter.event_message_type(filter.EventMessageType.ALL, priority=9999)
    async def handle_input_collector(self, event: AstrMessageEvent) -> None:
        """拦截交互式输入会话中的文件 / 控制词消息，阻止其继续进入 LLM。"""
        user_id = str(event.get_sender_id() or "")
        stream_id = str(event.unified_msg_origin or "")
        session = self._find_input_session(user_id, stream_id)
        if session is None:
            choice_key = user_id or stream_id
            cancel_tasks = self._cancel_choices.get(choice_key)
            if cancel_tasks:
                text = self._extract_text_from_event(event)
                indices = self._parse_cancel_indices(text, len(cancel_tasks))
                if indices:
                    for idx in indices:
                        await self._cancel_task(cancel_tasks[idx], stream_id)
                    self._cancel_choices.pop(choice_key, None)
                    self._mark_handled(event)
            return
        stream_id = stream_id or session.stream_id
        if session.phase == "text":
            await self._handle_text_input(session, stream_id, event)
            self._mark_handled(event)
            return
        if session.phase == "config":
            await self._handle_config_edit(session, stream_id, event)
            self._mark_handled(event)
            return
        if self._is_finish_signal(self._extract_text_from_event(event)):
            await self._finish_input_session(user_id, stream_id, skip_remaining=True)
            self._mark_handled(event)
            return
        if await self._extract_files_from_event(event):
            await self._handle_incoming_files(user_id, stream_id, event)
            self._mark_handled(event)
            return

    @filter.command("wf中断")
    async def handle_rh_cancel(self, event: AstrMessageEvent) -> None:
        """中断任务：还在传文件阶段则直接结束；已提交则回复编号取消运行中的任务。"""
        if self._is_consumed(event):
            return
        stream_id = str(event.unified_msg_origin or "")
        user_id = str(event.get_sender_id() or "")
        group_id = str(event.get_group_id() or "")
        allowed, deny_msg = self._check_access(user_id, group_id)
        if not allowed:
            await self._send_text(stream_id, deny_msg)
            self._mark_handled(event)
            return
        is_admin = self._is_admin(user_id)
        session = self._find_input_session(user_id, stream_id)
        if session is not None:
            key = self._session_key(session.user_id, session.stream_id)
            self._cancel_input_session(key)
            await self._send_text(stream_id, "已中断")
            self._mark_handled(event)
            return
        tasks = [
            (tid, meta)
            for tid, meta in self._task_meta.items()
            if is_admin or (user_id and meta.get("user_id") == user_id)
        ]
        if not tasks:
            await self._send_text(stream_id, "当前没有进行中的任务")
            self._mark_handled(event)
            return
        lines = ["正在运行的任务："]
        for index, (tid, meta) in enumerate(tasks, 1):
            lines.append(f"{index}. {meta.get('name') or tid}")
        lines.append("回复编号取消（如 1；可多个：1 2）")
        await self._send_text(stream_id, "\n".join(lines))
        self._cancel_choices[user_id or stream_id] = [tid for tid, _ in tasks]
        self._mark_handled(event)

    @staticmethod
    def _parse_cancel_indices(text: str, count: int) -> list[int]:
        """解析用户回复的编号（如 1、2、1 2、1,2），返回 0-based 有效编号列表。"""
        tokens = re.split(r"[\s,，、]+", str(text or "").strip())
        indices: list[int] = []
        for token in tokens:
            if not token.isdigit():
                continue
            idx = int(token)
            if 1 <= idx <= count and idx - 1 not in indices:
                indices.append(idx - 1)
        return indices

    async def _cancel_task(self, task_id: str, stream_id: str) -> None:
        """取消 RunningHub 任务并停止本地轮询。

        平台取消失败时仍然停止本地轮询（避免无限占用并发额度），但必须如实告知用户：
        远端任务可能继续运行并计费，需要去 RunningHub 手动处理。
        """
        meta = self._task_meta.get(task_id) or {}
        name = meta.get("name") or task_id
        region = str(meta.get("region") or "overseas").strip()
        client = self._get_client(region)
        if client is None:
            self._rebuild_client()
            client = self._get_client(region)
        remote_cancel_error = ""
        if client is None:
            remote_cancel_error = "插件客户端未初始化"
        else:
            try:
                result = await client.cancel(task_id)
                code = result.get("code")
                if code not in (0, 200, None):
                    raise RunningHubError(str(result.get("msg") or result.get("message") or result))
            except Exception as exc:
                remote_cancel_error = str(exc)
                self.logger.error("取消任务 %s 失败: %s", task_id, exc)

        poll_task = self._pending.pop(task_id, None)
        if poll_task is not None:
            poll_task.cancel()
        self._task_meta.pop(task_id, None)

        if remote_cancel_error:
            await self._send_text(
                stream_id,
                f"已停止本地跟踪，但 RunningHub 平台取消失败：{remote_cancel_error}。"
                f"任务「{name}」可能仍在运行并计费，请到 RunningHub 平台手动取消",
            )
        else:
            await self._send_text(stream_id, f"已取消任务：{name}")

    @filter.event_message_type(filter.EventMessageType.ALL, priority=9998)
    async def handle_notice_collector(self, event: AstrMessageEvent) -> None:
        """处理 OneBot notice 事件中的 QQ 群文件上传。"""
        raw = getattr(event.message_obj, "raw_message", None)
        if not isinstance(raw, dict):
            return
        if str(raw.get("post_type") or "") != "notice":
            return
        if str(raw.get("notice_type") or "") != "group_upload":
            return
        file_info = raw.get("file")
        if not isinstance(file_info, dict):
            return
        filename = str(file_info.get("name") or "").strip()
        file_id = str(file_info.get("id") or "").strip()
        if not filename or not file_id:
            return
        file_type = self._detect_file_type_from_name(filename)
        group_id = str(raw.get("group_id") or "").strip()
        user_id = str(raw.get("user_id") or "").strip()
        stream_id = str(event.unified_msg_origin or "")
        session = self._find_input_session(user_id, stream_id)
        if session is None:
            return
        key = self._session_key(session.user_id, session.stream_id)
        stream_id = stream_id or session.stream_id

        region = str(session.workflow.region or "overseas").strip()
        client = self._get_client(region)
        if client is None:
            self._rebuild_client()
            client = self._get_client(region)
        if client is None:
            self._cancel_input_session(key)
            await self._send_text(stream_id, "插件客户端未初始化，已取消本次任务")
            self._mark_handled(event)
            return

        try:
            file_data = await self._fetch_napcat_file_bytes(file_id, group_id, event=event)
        except Exception as exc:
            self.logger.error("获取 QQ 文件失败: %s", exc)
            await self._send_text(stream_id, f"获取文件失败：{exc}")
            self._mark_handled(event)
            return

        import base64 as _b64

        source = "base64://" + _b64.b64encode(file_data).decode("ascii")
        consumed = await self._consume_files(session, key, [(file_type, source)], stream_id, client)
        if consumed:
            self._mark_handled(event)

    async def _fetch_napcat_file_bytes(
        self, file_id: str, group_id: str, *, event: AstrMessageEvent | None = None
    ) -> bytes:
        """通过 OneBot API 获取 QQ 群文件内容（gzc-download 直链下载到的是错误 ZIP）。"""
        target = DeliveryTarget.from_event(event) if event is not None else DeliveryTarget()
        bot = self.delivery.get_onebot_client(target)
        if bot is None:
            raise RunningHubError("未找到 OneBot 客户端，无法获取 QQ 群文件")
        try:
            result = await bot.call_action("get_group_file_url", file_id=file_id, group_id=group_id)
        except Exception as exc:
            self.logger.warning("OneBot 文件 API get_group_file_url 调用失败: %s", exc)
            result = None
        content = await self._extract_bytes_from_napcat_result(result) if result is not None else None
        if content:
            return content
        raise RunningHubError(f"无法通过 OneBot API 获取文件 file_id={file_id}")

    @staticmethod
    def _decode_base64_bounded(encoded: str, max_bytes: int = _MAX_FILE_BYTES) -> bytes:
        """解码 base64 并强制大小上限，防止 QQ 群文件结果撑爆内存。"""

        encoded = str(encoded or "").strip()
        if not encoded:
            return b""
        if len(encoded) > max_bytes * 4 // 3 + 4:
            raise RunningHubError(f"base64 内容超过 {max_bytes} 字节上限，已拒绝")
        data = base64.b64decode(encoded, validate=False)
        if len(data) > max_bytes:
            raise RunningHubError(f"base64 解码后超过 {max_bytes} 字节上限，已拒绝")
        return data

    async def _extract_bytes_from_napcat_result(self, result: Any) -> bytes | None:
        """从 NapCat get_file / get_group_file_url 返回里解析出文件字节。"""
        if isinstance(result, str):
            result = result.strip()
            if result.startswith("base64://"):
                return self._decode_base64_bounded(result[len("base64://"):])
            if result.startswith(("http://", "https://")):
                client = self._client or self._client_cn
                if client is not None:
                    return await client.download_bytes(result)
            return None

        if not isinstance(result, dict):
            return None

        data = result.get("data")
        if isinstance(data, dict):
            b64 = str(data.get("file") or data.get("base64") or data.get("data") or "").strip()
            b64 = b64.removeprefix("base64://")
            if b64:
                try:
                    return self._decode_base64_bounded(b64)
                except RunningHubError:
                    raise
                except Exception:
                    # 非法 base64 保持原行为：跳过该候选，继续尝试其他字段/API
                    pass
            url = str(data.get("url") or data.get("file_url") or data.get("download_url") or "").strip()
            if url.startswith(("http://", "https://")):
                client = self._client or self._client_cn
                if client is not None:
                    return await client.download_bytes(url)
            path = str(data.get("path") or data.get("file_path") or "").strip()
            if path:
                p = Path(path)
                if p.is_file():
                    if p.stat().st_size > _MAX_FILE_BYTES:
                        raise RunningHubError(f"本地文件超过 {_MAX_FILE_BYTES} 字节上限，已拒绝: {path}")
                    return await asyncio.to_thread(p.read_bytes)

        url = result.get("url")
        if isinstance(url, str) and url.startswith(("http://", "https://")):
            client = self._client or self._client_cn
            if client is not None:
                return await client.download_bytes(url)

        return None

    @filter.command("wf工作流")
    async def handle_list_workflows(self, event: AstrMessageEvent) -> None:
        """列出已配置的工作流。"""
        if self._is_consumed(event):
            return
        stream_id = str(event.unified_msg_origin or "")
        allowed, deny_msg = self._check_access(
            str(event.get_sender_id() or ""), str(event.get_group_id() or "")
        )
        if not allowed:
            await self._send_text(stream_id, deny_msg)
            self._mark_handled(event)
            return
        workflows = self._workflows
        if not workflows:
            await self._send_text(stream_id, "尚未配置任何工作流，请先在插件配置中添加")
            self._mark_handled(event)
            return
        lines = ["已配置的工作流："]
        for workflow in workflows:
            node_count = len([n for n in workflow.input_nodes if str(n.node_id or "").strip()])
            lines.append(f"- {workflow.name}（节点 {node_count} 个，设备 {workflow.instance_type}）")
        await self._send_text(stream_id, "\n".join(lines))
        self._mark_handled(event)

    @filter.command("wf国内工作流")
    async def handle_detect_domestic_workflow(
        self, event: AstrMessageEvent, workflow_id: str = "", workflow_name: str = ""
    ) -> None:
        """识别国内工作流（runninghub.cn）的关键输入节点。"""
        if self._is_consumed(event):
            return
        stream_id = str(event.unified_msg_origin or "")
        allowed, deny_msg = self._check_access(
            str(event.get_sender_id() or ""), str(event.get_group_id() or "")
        )
        if not allowed:
            await self._send_text(stream_id, deny_msg)
            self._mark_handled(event)
            return
        if not str(workflow_id or "").strip():
            await self._send_text(stream_id, "用法：/wf国内工作流 <工作流ID> [工作流名称]")
            self._mark_handled(event)
            return
        name = str(workflow_name or "").strip() or str(workflow_id).strip()
        await self._detect_and_write(
            str(workflow_id).strip(), name, stream_id, detailed=False, region="domestic"
        )
        self._mark_handled(event)

    @filter.command("wf国外工作流")
    async def handle_detect_overseas_workflow(
        self, event: AstrMessageEvent, workflow_id: str = "", workflow_name: str = ""
    ) -> None:
        """识别国外工作流（runninghub.ai）的关键输入节点。"""
        if self._is_consumed(event):
            return
        stream_id = str(event.unified_msg_origin or "")
        allowed, deny_msg = self._check_access(
            str(event.get_sender_id() or ""), str(event.get_group_id() or "")
        )
        if not allowed:
            await self._send_text(stream_id, deny_msg)
            self._mark_handled(event)
            return
        if not str(workflow_id or "").strip():
            await self._send_text(stream_id, "用法：/wf国外工作流 <工作流ID> [工作流名称]")
            self._mark_handled(event)
            return
        name = str(workflow_name or "").strip() or str(workflow_id).strip()
        await self._detect_and_write(
            str(workflow_id).strip(), name, stream_id, detailed=False, region="overseas"
        )
        self._mark_handled(event)

    @filter.command("wf详细国内工作流")
    async def handle_detail_detect_domestic_workflow(
        self, event: AstrMessageEvent, workflow_id: str = "", workflow_name: str = ""
    ) -> None:
        """用 LLM 详细识别国内工作流的全部输入节点与配置节点。"""
        if self._is_consumed(event):
            return
        stream_id = str(event.unified_msg_origin or "")
        allowed, deny_msg = self._check_access(
            str(event.get_sender_id() or ""), str(event.get_group_id() or "")
        )
        if not allowed:
            await self._send_text(stream_id, deny_msg)
            self._mark_handled(event)
            return
        if not str(workflow_id or "").strip():
            await self._send_text(stream_id, "用法：/wf详细国内工作流 <工作流ID> [工作流名称]")
            self._mark_handled(event)
            return
        name = str(workflow_name or "").strip() or str(workflow_id).strip()
        await self._detect_and_write(
            str(workflow_id).strip(), name, stream_id, detailed=True, region="domestic"
        )
        self._mark_handled(event)

    @filter.command("wf详细国外工作流")
    async def handle_detail_detect_overseas_workflow(
        self, event: AstrMessageEvent, workflow_id: str = "", workflow_name: str = ""
    ) -> None:
        """用 LLM 详细识别国外工作流的全部输入节点与配置节点。"""
        if self._is_consumed(event):
            return
        stream_id = str(event.unified_msg_origin or "")
        allowed, deny_msg = self._check_access(
            str(event.get_sender_id() or ""), str(event.get_group_id() or "")
        )
        if not allowed:
            await self._send_text(stream_id, deny_msg)
            self._mark_handled(event)
            return
        if not str(workflow_id or "").strip():
            await self._send_text(stream_id, "用法：/wf详细国外工作流 <工作流ID> [工作流名称]")
            self._mark_handled(event)
            return
        name = str(workflow_name or "").strip() or str(workflow_id).strip()
        await self._detect_and_write(
            str(workflow_id).strip(), name, stream_id, detailed=True, region="overseas"
        )
        self._mark_handled(event)

    async def _detect_and_write(
        self,
        workflow_id: str,
        workflow_name: str,
        stream_id: str,
        *,
        detailed: bool,
        region: str,
    ) -> tuple[bool, str, int]:
        """识别工作流节点并写入配置（detailed=True 走 LLM 全量识别，region 指定区域）。"""
        self.logger.info(
            "[识别] 开始: workflow_id=%s name=%s detailed=%s region=%s",
            workflow_id, workflow_name, detailed, region,
        )

        key_attr = "api_key_cn" if region == "domestic" else "api_key"
        if not getattr(self.config.server, key_attr):
            label = "国内" if region == "domestic" else "国外"
            await self._send_text(stream_id, f"{label} API Key 未填写，请先在插件配置中配置")
            return True, "", 1
        client = self._get_client(region)
        if client is None:
            self._rebuild_client()
            client = self._get_client(region)
        if client is None:
            self.logger.warning("[识别] 未配置任何 api_key")
            await self._send_text(stream_id, "请先填写 RunningHub API Key（国外或国内至少一个）")
            return True, "", 1

        # 名称冲突检查
        for existing in self._workflows:
            if existing.name.strip() == workflow_name:
                await self._send_text(stream_id, f"已存在同名工作流「{workflow_name}」，请换一个名称重试")
                return True, "", 1

        # 用指定区域的 key 拉取工作流
        self.logger.info("[识别] 尝试 %s 拉取: workflow_id=%s", region, workflow_id)
        try:
            workflow_json = await client.get_workflow_json(workflow_id)
        except Exception as exc:
            self.logger.error("[识别] 获取工作流失败（%s）: %s", region, exc)
            await self._send_text(stream_id, f"获取工作流失败，请检查 API Key：{exc}")
            return True, "", 1
        self.logger.info("[识别] 工作流 JSON 已获取（区域=%s），节点总数=%d", region, len(workflow_json))

        if detailed:
            detected, detect_method = await self._detect_full(workflow_json, stream_id=stream_id)
        else:
            detected, detect_method = await self._detect_key_full(workflow_json, stream_id=stream_id)

        if not detected:
            self.logger.warning("[识别] 未识别出输入节点")
            await self._send_text(stream_id, "未识别出输入节点，请手动配置")
            return True, "", 1
        self.logger.info(
            "[识别] %s 识别到 %d 个节点: %s",
            detect_method,
            len(detected),
            ", ".join(f"{n['node_id']}/{n['field_name']}/{n['value_type']}" for n in detected),
        )

        try:
            await self._append_workflow_to_config(
                workflow_name=workflow_name,
                workflow_id=workflow_id,
                nodes=detected,
                region=region,
            )
        except Exception as exc:
            self.logger.error("[识别] 写入插件配置失败: %s", exc, exc_info=True)
            await self._send_text(stream_id, f"写入配置失败：{exc}")
            return True, "", 1

        region_label = "国内" if region == "domestic" else "国外"
        await self._send_text(
            stream_id,
            f"识别成功（{detect_method}·{region_label}），共 {len(detected)} 个节点，具体请查看插件配置",
        )
        return True, "", 1

    async def _detect_full(
        self, workflow_json: dict[str, Any], *, stream_id: str = ""
    ) -> tuple[list[dict[str, str]], str]:
        """详细识别：LLM 优先（全量提示词），失败回退启发式。"""
        if self.config.feature.use_llm:
            llm_nodes = await self._detect_input_nodes_with_llm(workflow_json, stream_id=stream_id)
            if llm_nodes is not None:
                return llm_nodes, "LLM"
        return detect_input_nodes(workflow_json), "启发式"

    async def _detect_key_full(
        self, workflow_json: dict[str, Any], *, stream_id: str = ""
    ) -> tuple[list[dict[str, str]], str]:
        """简化识别：LLM 优先（关键节点专用提示词），失败回退启发式。"""
        if self.config.feature.use_llm:
            llm_nodes = await self._detect_input_nodes_with_llm(
                workflow_json,
                prompt_template=LLM_DETECT_KEY_PROMPT,
                stream_id=stream_id,
            )
            if llm_nodes is not None:
                return llm_nodes, "LLM"
        return detect_key_nodes(workflow_json), "简化"

    async def _persist_workflow_items(
        self, items: list[WorkflowItemSection]
    ) -> dict[str, Any]:
        """把工作流列表写入 AstrBot 配置并热更新当前实例（识别 / 可视化页面共用）。"""
        async with self._config_write_lock:
            temp = self.config.model_copy(deep=True)
            temp.workflows.items = [
                WorkflowItemSection.model_validate(item) for item in items
            ]
            new_raw = dump_config_dict(temp)
            if self._astrbot_config is not None:
                await asyncio.to_thread(self._astrbot_config.save_config, new_raw)
            self._apply_config_dict(new_raw)
            self._refresh_workflows()
            self._refresh_llm_tool_description()
            self._validate_workflows()
            return new_raw

    async def _append_workflow_to_config(
        self,
        *,
        workflow_name: str,
        workflow_id: str,
        nodes: list[dict[str, str]],
        region: str = "overseas",
    ) -> None:
        """将识别出的工作流写入 AstrBot 插件配置并热更新当前实例。"""
        workflow_dict: dict[str, Any] = {
            "name": workflow_name,
            "workflow_id": workflow_id,
            "instance_type": "Standard",
            "region": region,
            "llm_enhance": False,
            "llm_template_path": "",
            "input_nodes": [
                {
                    "node_id": str(node.get("node_id") or ""),
                    "field_name": str(node.get("field_name") or ""),
                    "field_value": str(node.get("field_value") or ""),
                    "value_type": str(node.get("value_type") or ""),
                    "label": str(node.get("label") or node.get("hint") or ""),
                }
                for node in nodes
            ],
        }
        WorkflowItemSection.model_validate(workflow_dict)

        merged = [
            workflow.model_dump(mode="python")
            for workflow in self.config.workflows.items
        ]
        merged.append(workflow_dict)
        merged_models = [WorkflowItemSection.model_validate(item) for item in merged]
        new_raw = await self._persist_workflow_items(merged_models)
        self.logger.info(
            "[识别] 已写入插件配置：workflows=%d, workflow_nodes=%d（本次 %d 个节点）",
            len(new_raw.get("workflows") or []),
            len(new_raw.get("workflow_nodes") or []),
            len(nodes),
        )


    async def _detect_input_nodes_with_llm(
        self,
        workflow_json: dict[str, Any],
        *,
        prompt_template: str | None = None,
        stream_id: str = "",
    ) -> list[dict[str, str]] | None:
        """用内置 LLM 识别节点（失败返回 None，由调用方回退启发式）。

        prompt_template 传入时使用该提示词模板（如关键节点专用模板）。
        """
        workflow_desc = describe_workflow_for_llm(workflow_json)
        template = prompt_template or LLM_DETECT_PROMPT
        prompt = template.format(workflow=workflow_desc)
        try:
            result = await self._llm_generate(
                prompt=prompt,
                stream_id=stream_id,
                provider_id=self.config.feature.model,
                temperature=0.2,
                max_tokens=1500,
            )
        except Exception as exc:
            self.logger.warning("[识别] LLM 识别调用异常，回退启发式: %s", exc, exc_info=True)
            return None
        if not isinstance(result, dict) or not result.get("success"):
            self.logger.warning("[识别] LLM 识别未成功，回退启发式: %s", str(result)[:300])
            return None
        raw_response = str(result.get("response") or result.get("content") or "")
        nodes = parse_llm_nodes(raw_response, workflow_json)
        if not nodes:
            self.logger.warning(
                "[识别] LLM 输出解析/校验失败，回退启发式；原始响应: %s", raw_response[:500]
            )
            return None
        self.logger.info(
            "[识别] LLM 识别出 %d 个节点: %s",
            len(nodes),
            ", ".join(f"{n['node_id']}/{n['field_name']}/{n['value_type']}" for n in nodes),
        )
        return nodes

    @filter.command("wf运行")
    async def handle_pao_tu(self, event: AstrMessageEvent) -> None:
        """运行配置好的工作流，例如：/wf运行 动漫生图 一只猫。"""
        if self._is_consumed(event):
            return
        stream_id = str(event.unified_msg_origin or "")
        # CommandFilter 的默认字符串参数只取第一个词，因此这里直接解析完整消息，
        # 以支持带空格的描述文本。
        rest = re.sub(
            r"^/?wf运行[\s：:，,、]*", "", str(event.message_str or "").strip(), count=1
        ).strip()
        if not rest:
            available = "、".join(w.name for w in self._workflows if w.name) or "（未配置工作流）"
            await self._send_text(
                stream_id, f"用法：/wf运行 <工作流名> <描述文本>\n已配置工作流：{available}"
            )
            self._mark_handled(event)
            return

        parts = rest.split(maxsplit=1)
        workflow_name = parts[0].strip()
        command_text = parts[1].strip() if len(parts) > 1 else ""

        kwargs = self._event_ctx(event)
        kwargs["trigger"] = "command"
        result = await self._start_workflow(workflow_name, command_text, **kwargs)
        await self._send_text(stream_id, result["message"])
        self._mark_handled(event)

    def _refresh_llm_tool_description(self) -> None:
        """把当前可自然语言调用的工作流名称注入 run_workflow 工具描述。"""
        names = self._llm_callable_workflow_names()
        name_list = (
            "、".join(names)
            if names
            else "（当前没有支持自然语言调用的工作流，需是仅有提示词输入的工作流）"
        )
        description = (
            "运行仅支持自然语言调用的 RunningHub 工作流（文生图/文生视频等只有提示词输入的工作流）。"
            f"当前支持的工作流名称：{name_list}。"
            "workflow_name 必须从上述名称中精确选一个；prompt 填用户描述的内容（从用户原话提取，不要脑补）。"
            "只在用户明确要求生成图片/视频时才调用。"
            "调用后立即返回任务已提交，生成结果会异步自动发送到会话，你无需等待或轮询。"
            "若描述列出的工作流里没有用户想要的，可能是工作流刚更新、工具描述未刷新，不要瞎填名称，"
            "告诉用户「工作流列表可能已更新，请重新加载插件后再试」，或改用 /wf运行 命令。"
        )
        try:
            tool = self.context.get_llm_tool_manager().get_func("run_workflow")
            if tool is not None:
                tool.description = description
        except Exception as exc:
            self.logger.debug("刷新 run_workflow 工具描述失败: %s", exc)

    @filter.llm_tool("run_workflow")
    async def handle_run_workflow(
        self, event: AstrMessageEvent, workflow_name: str, prompt: str = ""
    ) -> str:
        """运行配置好的 RunningHub 工作流，提交提示词并生成结果（文生图/文生视频等）。
        仅支持「只有提示词输入、无图片/音频/视频/配置输入」的工作流。

        Args:
            workflow_name(string): 要运行的工作流名称（必须从工具描述中列出的支持名称里精确选一个）
            prompt(string): 要填入输入节点的描述文本（如提示词）；留空则使用配置的默认值
        """
        kwargs = self._event_ctx(event)
        kwargs["trigger"] = "tool"
        workflow_name = str(workflow_name or "").strip()
        prompt = str(prompt or "").strip()
        names = self._llm_callable_workflow_names()
        if not workflow_name:
            return (
                "请从以下支持自然语言调用的工作流名称中精确选一个填入 workflow_name，"
                "并把用户想要生成的内容填入 prompt（从用户原话提取，不要脑补），然后再次调用本工具。"
                "可选工作流：" + ("、".join(names) if names else "（无）")
            )
        if workflow_name not in names:
            all_names = self._workflow_names()
            if workflow_name in all_names:
                reason = (
                    f"工作流「{workflow_name}」包含图片/音频/视频/配置等输入节点，"
                    "不支持自然语言调用，请让用户改用命令 /wf运行 手动运行"
                )
            else:
                reason = f"工作流「{workflow_name}」未配置"
            return (
                reason + "。可选的自然语言调用工作流："
                + ("、".join(names) if names else "（无）")
                + "。请直接结束本轮思考，不要重复调用本工具。"
            )
        workflow = self._find_workflow(workflow_name)
        if not prompt and workflow is not None and self._primary_prompt_node(workflow) is not None:
            return (
                f"工作流「{workflow_name}」的提示词节点没有默认值，"
                "请把用户想要生成的内容填入 prompt 参数后再次调用本工具，不要创建任务。"
            )
        result = await self._start_workflow(workflow_name, prompt, **kwargs)
        if not result["success"]:
            return "错误：" + result["message"]
        return (
            "任务已提交并开始运行，task_id=" + str(result.get("task_id") or "") + "。"
            "生成结果会异步自动发送到会话，你无需等待或轮询，请直接结束本轮思考，不要调用 wait。"
        )

    @staticmethod
    def _web_jsonify(payload: Any):
        """返回 JSON 响应，兼容 Quart（AstrBot 4.x）与 Flask。"""
        try:
            from quart import jsonify
        except ImportError:
            from flask import jsonify
        return jsonify(payload)

    async def _web_request_json(self) -> dict[str, Any]:
        """读取 Web API 的 JSON body，兼容 Quart 与 Flask。"""
        try:
            from quart import request as web_request
            return await web_request.get_json(silent=True) or {}
        except ImportError:
            from flask import request as web_request
            return web_request.get_json(silent=True) or {}

    def _page_config_payload(self) -> dict[str, Any]:
        """生成可视化页面使用的配置快照（只暴露工作流与节点）。"""
        workflows: list[dict[str, Any]] = []
        for workflow in self.config.workflows.items:
            nodes: list[dict[str, Any]] = []
            for node in workflow.input_nodes:
                if not str(node.node_id or "").strip():
                    continue
                nodes.append(
                    {
                        "node_id": str(node.node_id or ""),
                        "field_name": str(node.field_name or "prompt"),
                        "field_value": str(node.field_value or ""),
                        "value_type": str(node.value_type or ""),
                        "effective_type": self._resolve_value_type(node),
                        "label": str(node.label or ""),
                    }
                )
            workflows.append(
                {
                    "name": str(workflow.name or ""),
                    "workflow_id": str(workflow.workflow_id or ""),
                    "instance_type": str(workflow.instance_type or "Standard"),
                    "region": str(workflow.region or "overseas"),
                    "llm_enhance": bool(workflow.llm_enhance),
                    "llm_template_path": str(workflow.llm_template_path or ""),
                    "nodes": nodes,
                }
            )
        return {
            "workflows": workflows,
            "prompt_templates": self._list_prompt_templates(),
            "use_llm": bool(self.config.feature.use_llm),
            "max_nodes": _MAX_NODES,
            "max_workflows": 20,
            "overseas_ready": bool(self.config.server.api_key),
            "domestic_ready": bool(self.config.server.api_key_cn),
        }

    def _workflows_from_page_payload(
        self, workflows_raw: Any
    ) -> tuple[list[WorkflowItemSection], str]:
        """把页面提交的工作流列表校验成强类型模型。"""
        if not isinstance(workflows_raw, list):
            return [], "workflows 必须是数组"
        if len(workflows_raw) > 20:
            return [], "工作流数量不能超过 20 个"
        allowed_types = {"", "default", "text", "image", "audio", "video", "prompt"}
        items: list[WorkflowItemSection] = []
        names: set[str] = set()
        for index, raw in enumerate(workflows_raw, start=1):
            if not isinstance(raw, dict):
                return [], f"第 {index} 个工作流格式不正确"
            name = str(raw.get("name") or "").strip()
            if not name:
                return [], f"第 {index} 个工作流缺少名称"
            if name in names:
                return [], f"工作流名称「{name}」重复"
            names.add(name)
            workflow_id = str(raw.get("workflow_id") or "").strip()
            if not workflow_id:
                return [], f"工作流「{name}」缺少工作流 ID"
            instance_type = str(raw.get("instance_type") or "Standard").strip()
            if instance_type not in ("Standard", "Plus", "Ultra"):
                instance_type = "Standard"
            region = str(raw.get("region") or "overseas").strip()
            if region not in ("overseas", "domestic"):
                region = "overseas"
            llm_enhance = raw.get("llm_enhance", False)
            if not isinstance(llm_enhance, bool):
                llm_enhance = str(llm_enhance).strip().lower() in {"1", "true", "yes", "on"}
            llm_template_path = str(raw.get("llm_template_path") or "").strip()
            nodes_raw = raw.get("nodes") or []
            if not isinstance(nodes_raw, list):
                return [], f"工作流「{name}」的 nodes 必须是数组"
            if len(nodes_raw) > _MAX_NODES:
                return [], f"工作流「{name}」输入节点超过 {_MAX_NODES} 个上限"
            nodes: list[dict[str, Any]] = []
            seen_fields: set[tuple[str, str]] = set()
            prompt_count = 0
            for node_index, node_raw in enumerate(nodes_raw, start=1):
                if not isinstance(node_raw, dict):
                    return [], f"工作流「{name}」第 {node_index} 个节点格式不正确"
                node_id = str(node_raw.get("node_id") or "").strip()
                if not node_id:
                    return [], f"工作流「{name}」第 {node_index} 个节点缺少 node_id"
                field_name = str(node_raw.get("field_name") or "prompt").strip() or "prompt"
                field_value = str(node_raw.get("field_value") or "")
                value_type = str(node_raw.get("value_type") or "").strip().lower()
                if value_type == "auto":
                    value_type = ""
                if value_type not in allowed_types:
                    return [], f"工作流「{name}」节点 {node_id}/{field_name} 类型不合法"
                label = str(node_raw.get("label") or "").strip()
                key = (node_id, field_name)
                if key in seen_fields:
                    return [], f"工作流「{name}」存在重复节点 {node_id}/{field_name}"
                seen_fields.add(key)
                if value_type == "prompt":
                    prompt_count += 1
                    if prompt_count > 1:
                        return [], f"工作流「{name}」最多只能有 1 个主提示词节点"
                nodes.append(
                    {
                        "node_id": node_id,
                        "field_name": field_name,
                        "field_value": field_value,
                        "value_type": value_type,
                        "label": label,
                    }
                )
            items.append(
                WorkflowItemSection.model_validate(
                    {
                        "name": name,
                        "workflow_id": workflow_id,
                        "instance_type": instance_type,
                        "region": region,
                        "llm_enhance": llm_enhance,
                        "llm_template_path": llm_template_path,
                        "input_nodes": nodes,
                    }
                )
            )
        return items, ""

    async def handle_page_get_config(self):
        """可视化页面 API：读取工作流与输入节点。"""
        return self._web_jsonify({"success": True, "data": self._page_config_payload()})

    async def handle_page_save_config(self):
        """可视化页面 API：整体保存工作流与输入节点。"""
        try:
            payload = await self._web_request_json()
        except Exception as exc:  # pragma: no cover
            self.logger.error("读取页面保存请求失败: %s", exc)
            return self._web_jsonify({"success": False, "message": f"读取请求失败: {exc}"})
        if "workflows" not in payload:
            return self._web_jsonify({"success": False, "message": "缺少 workflows 字段"})
        items, error = self._workflows_from_page_payload(payload.get("workflows"))
        if error:
            return self._web_jsonify({"success": False, "message": error})
        try:
            new_raw = await self._persist_workflow_items(items)
        except Exception as exc:  # pragma: no cover
            self.logger.error("[页面] 保存配置失败: %s", exc, exc_info=True)
            return self._web_jsonify({"success": False, "message": f"保存失败: {exc}"})
        self.logger.info(
            "[页面] 已保存工作流配置：workflows=%d, workflow_nodes=%d",
            len(new_raw.get("workflows") or []),
            len(new_raw.get("workflow_nodes") or []),
        )
        return self._web_jsonify(
            {
                "success": True,
                "message": "配置已保存并热更新",
                "workflows": len(new_raw.get("workflows") or []),
                "workflow_nodes": len(new_raw.get("workflow_nodes") or []),
            }
        )

    async def handle_page_analyze_workflow(self):
        """可视化页面 API：拉取并识别工作流节点（不写入配置）。"""
        try:
            payload = await self._web_request_json()
        except Exception as exc:  # pragma: no cover
            self.logger.error("读取页面识别请求失败: %s", exc)
            return self._web_jsonify({"success": False, "message": f"读取请求失败: {exc}"})
        workflow_id = str(payload.get("workflow_id") or "").strip()
        region = str(payload.get("region") or "overseas").strip()
        if region not in ("overseas", "domestic"):
            region = "overseas"
        detailed = bool(payload.get("detailed", False))
        if not workflow_id:
            return self._web_jsonify({"success": False, "message": "workflow_id 不能为空"})
        key_attr = "api_key_cn" if region == "domestic" else "api_key"
        if not getattr(self.config.server, key_attr):
            label = "国内" if region == "domestic" else "国外"
            return self._web_jsonify({"success": False, "message": f"{label} API Key 未填写"})
        client = self._get_client(region)
        if client is None:
            self._rebuild_client()
            client = self._get_client(region)
        if client is None:
            return self._web_jsonify({"success": False, "message": "RunningHub 客户端初始化失败"})
        try:
            workflow_json = await client.get_workflow_json(workflow_id)
        except Exception as exc:  # pragma: no cover
            self.logger.error("[页面] 拉取工作流失败: %s", exc)
            return self._web_jsonify({"success": False, "message": f"拉取工作流失败: {exc}"})
        try:
            if detailed:
                detected, method = await self._detect_full(workflow_json, stream_id="")
            else:
                detected, method = await self._detect_key_full(workflow_json, stream_id="")
        except Exception as exc:  # pragma: no cover
            self.logger.error("[页面] 识别节点失败: %s", exc, exc_info=True)
            return self._web_jsonify({"success": False, "message": f"识别失败: {exc}"})
        if not detected:
            return self._web_jsonify({"success": False, "message": "未识别出输入节点，请手动添加"})
        return self._web_jsonify({"success": True, "method": method, "nodes": detected})

    def _prompt_templates_dir(self) -> Path:
        """扩写提示词模板目录（相对插件目录 prompt/）。"""
        directory = _PLUGIN_DIR / "prompt"
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _safe_prompt_template(self, name: str) -> Path | None:
        """把模板名解析为 prompt/ 目录内的安全路径（仅允许 .txt / .md）。"""
        raw = str(name or "").strip().replace("\\", "/")
        if not raw or raw in {".", ".."} or raw.startswith(".") or "/" in raw:
            return None
        if not raw.lower().endswith((".txt", ".md")):
            return None
        target = (self._prompt_templates_dir() / raw).resolve()
        try:
            target.relative_to(self._prompt_templates_dir().resolve())
        except ValueError:
            return None
        return target

    def _list_prompt_templates(self) -> list[dict[str, Any]]:
        directory = self._prompt_templates_dir()
        templates: list[dict[str, Any]] = []
        for path in directory.iterdir():
            if not path.is_file() or not path.name.lower().endswith((".txt", ".md")):
                continue
            templates.append(
                {
                    "name": path.name,
                    "path": f"prompt/{path.name}",
                    "size": path.stat().st_size,
                    "modified": int(path.stat().st_mtime),
                }
            )
        templates.sort(key=lambda item: str(item["name"]).lower())
        return templates

    async def handle_page_list_prompt_templates(self):
        """可视化页面 API：列出扩写提示词模板。"""
        return self._web_jsonify(
            {"success": True, "data": {"templates": self._list_prompt_templates()}}
        )

    async def handle_page_read_prompt_template(self):
        """可视化页面 API：读取一个扩写提示词模板内容。"""
        try:
            from quart import request as web_request
        except ImportError:
            from flask import request as web_request
        target = self._safe_prompt_template(web_request.args.get("name", ""))
        if target is None or not target.is_file():
            return self._web_jsonify({"success": False, "message": "模板不存在或文件名不合法"})
        if target.stat().st_size > 2 * 1024 * 1024:
            return self._web_jsonify({"success": False, "message": "模板超过 2MB，无法预览"})
        try:
            content = target.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            return self._web_jsonify({"success": False, "message": "模板不是 UTF-8 文本，无法预览"})
        return self._web_jsonify(
            {
                "success": True,
                "data": {
                    "name": target.name,
                    "path": f"prompt/{target.name}",
                    "content": content,
                },
            }
        )

    async def handle_page_upload_prompt_template(self):
        """可视化页面 API：上传扩写提示词模板到 prompt/ 目录。

        同时支持两种前端上传方式：
        - AstrBot 页面桥的 multipart/form-data（bridge.upload）；
        - 普通 JSON（apiPost，filename + content），作为网络异常时的兜底。
        """
        is_quart = True
        try:
            from quart import request as web_request
        except ImportError:
            is_quart = False
            from flask import request as web_request
        try:
            content_type = str(getattr(web_request, "content_type", "") or "").lower()
            if "multipart/form-data" in content_type:
                if is_quart:
                    files = await web_request.files
                else:
                    files = web_request.files
                uploaded = files.get("file")
                if uploaded is None:
                    return self._web_jsonify(
                        {"success": False, "message": "没有收到文件，请使用 multipart/form-data 上传"}
                    )
                raw_name = str(getattr(uploaded, "filename", "") or "").strip()
                filename = Path(raw_name).name
                if not filename or filename in {".", ".."}:
                    return self._web_jsonify({"success": False, "message": "文件名不合法"})
                target = self._safe_prompt_template(filename)
                if target is None:
                    return self._web_jsonify({"success": False, "message": "仅支持 .txt / .md 模板文件"})
                existed = target.exists()
                if is_quart:
                    await uploaded.save(target)
                else:
                    uploaded.save(target)
            else:
                if is_quart:
                    payload = await web_request.get_json(silent=True) or {}
                else:
                    payload = web_request.get_json(silent=True) or {}
                raw_name = str(payload.get("filename") or "").strip()
                content = payload.get("content")
                filename = Path(raw_name).name
                if not filename or filename in {".", ".."}:
                    return self._web_jsonify({"success": False, "message": "文件名不合法"})
                target = self._safe_prompt_template(filename)
                if target is None:
                    return self._web_jsonify({"success": False, "message": "仅支持 .txt / .md 模板文件"})
                if not isinstance(content, str) or not content.strip():
                    return self._web_jsonify({"success": False, "message": "模板内容不能为空"})
                if len(content.encode("utf-8")) > 2 * 1024 * 1024:
                    return self._web_jsonify({"success": False, "message": "文件超过 2MB"})
                existed = target.exists()
                target.write_text(content, encoding="utf-8")
        except Exception as exc:  # pragma: no cover
            self.logger.error("[页面] 上传模板失败: %s", exc, exc_info=True)
            return self._web_jsonify({"success": False, "message": f"上传失败: {exc}"})
        try:
            size = target.stat().st_size
            if size <= 0:
                raise ValueError("文件为空")
            if size > 2 * 1024 * 1024:
                raise ValueError("文件超过 2MB")
            target.read_text(encoding="utf-8")
        except Exception as exc:  # pragma: no cover
            try:
                target.unlink(missing_ok=True)
            except Exception:
                pass
            return self._web_jsonify({"success": False, "message": f"模板内容不可用: {exc}"})
        self.logger.info(
            "[页面] 已上传扩写模板: %s（%d 字节，覆盖=%s）", target.name, size, existed
        )
        return self._web_jsonify(
            {
                "success": True,
                "message": "模板已上传" + ("（已覆盖同名文件）" if existed else ""),
                "data": {
                    "name": target.name,
                    "path": f"prompt/{target.name}",
                    "size": size,
                    "overwritten": existed,
                },
            }
        )


    async def handle_run_workflow_api(self):
        """Web API：运行配置好的 RunningHub 工作流（供其他插件 / WebUI 调用）。"""
        # AstrBot 4.x 的 Dashboard 使用 Quart；兼容未来切到 Flask 的版本
        payload: dict[str, Any] = {}
        try:
            from quart import jsonify
            from quart import request as web_request

            payload = await web_request.get_json(silent=True) or {}
        except ImportError:
            from flask import jsonify
            from flask import request as web_request

            payload = web_request.get_json(silent=True) or {}
        except Exception as exc:  # pragma: no cover
            self.logger.error("读取 Web API 请求失败: %s", exc)
            return {"success": False, "message": f"读取请求失败: {exc}"}, 500
        workflow_name = str(payload.get("workflow_name") or "").strip()
        prompt = str(payload.get("prompt") or "").strip()
        stream_id = str(payload.get("stream_id") or "")
        user_id = str(payload.get("user_id") or "")
        group_id = str(payload.get("group_id") or "")
        platform_id = str(payload.get("platform_id") or "")
        if not workflow_name:
            return jsonify({"success": False, "message": "workflow_name 不能为空"})
        result = await self._start_workflow(
            workflow_name,
            prompt,
            stream_id=stream_id,
            user_id=user_id,
            group_id=group_id,
            platform_id=platform_id,
            trigger="api",
        )
        return jsonify(result)






def type_name_of(file_type: str) -> str:
    """节点文件类型的中文名称。"""
    return {"image": "图片", "audio": "语音", "video": "视频"}.get(file_type, "文件")

