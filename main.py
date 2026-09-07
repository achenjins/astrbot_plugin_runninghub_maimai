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
- /wf 保存提示词 / /wf 提示词重跑 / /wf 提示词
- /wf 上传提示词 / /wf 上传提示词模板
- /wf工作流
- /wf国外工作流 <工作流ID> [名称] / /wf国内工作流 <工作流ID> [名称]
- /wf详细国外工作流 <工作流ID> [名称] / /wf详细国内工作流 <工作流ID> [名称]
- /wf中断
LLM 工具：get_workflow_context / run_workflow
Web API：POST /api/plug/runninghub_workflow_adapter/run_workflow_api
"""

from __future__ import annotations

import asyncio
import base64
import codecs
import hashlib
import importlib
import json
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
from astrbot.core.utils.astrbot_path import get_astrbot_data_path, get_astrbot_plugin_data_path

try:
    from charset_normalizer import from_bytes as _detect_text_encoding
except ImportError:  # pragma: no cover - requests normally installs this dependency
    _detect_text_encoding = None

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

_validation_lib = _load_local_module("validation")

_chat_lib = _load_local_module("chat_workflows")
ChatWorkflowMixin = _chat_lib.ChatWorkflowMixin
ResultDeliveryMixin = _load_local_module("result_delivery").ResultDeliveryMixin


__all__ = ["RunningHubGenericPlugin"]

# 交互式收集的等待超时（秒）
_INPUT_WAIT_TIMEOUT = 600

# 单个工作流的输入/配置节点总数上限（含参考图、配置节点，原 8 个对多参考图工作流不够）
_MAX_NODES = 32

# 上传/下载单个文件的最大字节数（512MB），防止异常或恶意超大内容撑爆内存
_MAX_FILE_BYTES = 512 * 1024 * 1024
# 最近任务记录：只保留 task_id / workflow / coins 三个字段，最多 200 条
_TASK_HISTORY_FILE = "task_history.json"
_TASK_HISTORY_MAX = 200
_TASK_HISTORY_FIELDS = ("task_id", "workflow", "coins")

# 提示词缓存：最近运行每个用户只保留 5 条，手动保存的提示词长期保留。
_PROMPT_LIBRARY_FILE = "prompt_library.json"
_RECENT_PROMPT_MAX = 5
_PROMPT_TEMPLATE_MAX_BYTES = 2 * 1024 * 1024

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
    # 复用缓存时直接使用已经扩写完成的提示词，禁止再次调用 LLM。
    reused_prompt: str = ""
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
    # 已接收文件的 SHA-256，用于过滤 QQ 群文件 notice + 普通消息的重复投递
    received_labels: list[str] = field(default_factory=list)
    received_hashes: set[str] = field(default_factory=set)
    execution_context: dict[str, Any] = field(default_factory=dict)
    consume_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass
class PromptInteraction:
    """保存/选择提示词时的一次短交互。"""

    user_id: str
    stream_id: str
    owner_key: str
    phase: str
    entries: list[dict[str, Any]] = field(default_factory=list)
    selected: dict[str, Any] | None = None
    created_at: float = field(default_factory=time.time)
    expire_task: asyncio.Task | None = None


class RunningHubGenericPlugin(ChatWorkflowMixin, ResultDeliveryMixin, Star):
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
        self._result_send_locks: dict[tuple[str, str], asyncio.Lock] = {}
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
        self._user_submitting: dict[str, int] = {}
        self._task_meta: dict[str, dict[str, str]] = {}
        self._cancel_choices: dict[str, list[str]] = {}
        self._task_history: list[dict[str, str]] = []
        self._task_history_recorded: set[str] = set()
        self._task_history_lock: asyncio.Lock = asyncio.Lock()
        self._prompt_library: dict[str, dict[str, list[dict[str, Any]]]] = {}
        self._prompt_library_lock: asyncio.Lock = asyncio.Lock()
        self._prompt_interactions: dict[str, PromptInteraction] = {}
        self._media_store = None

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
        # AstrBot's flag means "forbid the default LLM request" despite its name.
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
        """自然语言可选择所有显式开启的有效工作流，包括图片和参数输入。"""
        nodes = self._ordered_nodes(workflow)
        return bool(workflow.llm_enabled and workflow.workflow_id.strip() and nodes
                    and sum(self._resolve_value_type(n) == "prompt" for n in nodes) <= 1)

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
        self._load_task_history()
        self._load_prompt_library()
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
        self.context.register_web_api(
            "/runninghub_workflow_adapter/page/account",
            self.handle_page_get_account,
            ["GET"],
            "可视化页面：查询 RunningHub 账户余额",
        )
        self.context.register_web_api(
            "/runninghub_workflow_adapter/page/tasks",
            self.handle_page_get_tasks,
            ["GET"],
            "可视化页面：读取最近任务记录",
        )
        self.context.register_web_api(
            "/runninghub_workflow_adapter/page/tasks/clear",
            self.handle_page_clear_tasks,
            ["POST"],
            "可视化页面：清空最近任务记录",
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
        expire_tasks.extend(
            interaction.expire_task
            for interaction in self._prompt_interactions.values()
            if interaction.expire_task is not None
        )
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
        self._prompt_interactions.clear()
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
        interval = 60
        max_age = 24 * 3600
        next_file_cleanup = 0.0
        while True:
            try:
                await asyncio.sleep(5)
                if time.monotonic() >= next_file_cleanup:
                    self._cleanup_cache_once(max_age_seconds=max_age)
                    next_file_cleanup = time.monotonic() + 6 * 3600
                if self._media_store is not None:
                    self._media_store.prune()
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
        """旧配置保留供管理员修复；运行前仍会拒绝不合法的节点。"""
        for workflow in self._workflows:
            for error in _validation_lib.workflow_errors(workflow):
                self.logger.warning("配置需修正：%s", error["message"])
            if len(workflow.input_nodes) > _MAX_NODES:
                self.logger.warning("工作流 %s 超过 %d 个节点上限", workflow.name, _MAX_NODES)

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

    def _check_access(self, user_id: str, group_id: str, *, check_quota: bool = True) -> tuple[bool, str]:
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

        if check_quota and cfg.max_per_user_per_hour > 0:
            if not uid:
                return False, "无法识别用户身份，已阻止本次请求（已开启频率限制）"
            now = time.time()
            bucket = self._user_requests.setdefault(uid, [])
            bucket[:] = [t for t in bucket if now - t < 3600]
            if len(bucket) + self._user_submitting.get(uid, 0) >= cfg.max_per_user_per_hour:
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
            # 配置值仍是相对路径：优先读用户持久化目录（更新不丢失），
            # 找不到时回退到插件自带的 prompt/ 目录。
            relative = str(resolved).replace("\\", "/")
            if relative.startswith("prompt/"):
                relative = relative[len("prompt/"):]
            user_resolved = self._prompt_templates_dir() / relative
            resolved = user_resolved if user_resolved.is_file() else (_PLUGIN_DIR / resolved)
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
        return _validation_lib.resolve_value_type(node)

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
                    "required": node.required,
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
                    "required": node.required,
                    "param_type": node.param_type,
                    "minimum": node.minimum,
                    "maximum": node.maximum,
                    "choices": node.choices,
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
        reused_prompt = str(kwargs.pop("reused_prompt", "") or "").strip()
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

        config_errors = _validation_lib.workflow_errors(workflow)
        if config_errors:
            return {"success": False, "message": config_errors[0]["message"] + "，请管理员修改配置"}

        # Natural-language overrides are validated by ChatWorkflowMixin. Use a
        # per-run copy so one user's parameters never change shared config.
        natural = bool(kwargs.get("natural"))
        overrides = kwargs.pop("node_overrides", {})
        if natural:
            workflow = workflow.model_copy(deep=True)
            for node in workflow.input_nodes:
                key = _chat_lib.node_key(node)
                if key in overrides:
                    node.field_value = overrides[key]

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
        if reused_prompt and text_target is None:
            return {
                "success": False,
                "message": f"工作流「{workflow.name}」已没有主提示词节点，无法复用该提示词",
            }
        editable_nodes = self._editable_config_nodes(workflow)
        if natural:
            # Defaults and explicit parameters are already known; only missing
            # required values need an interactive follow-up.
            required_keys = {_chat_lib.node_key(n) for n in self._ordered_nodes(workflow)
                             if n.required and not n.field_value.strip()}
            editable_nodes = [n for n in editable_nodes
                              if f"{n['node_id']}/{n['field_name']}" in required_keys]
        # 存在无默认值的 prompt 节点且用户没给描述：不能直接提交，先交互收集描述
        missing_prompt_text = bool(text_node is not None and not command_text)
        # 会话中需要回填文字的目标节点：给了文本或需要补文本时才记录
        session_text_node = text_target if (command_text or missing_prompt_text) else None

        # 先用原始文本构建节点参数（文字节点暂填原文，扩写见下）
        node_info_list, waiting = self._build_node_info_list(
            workflow,
            command_text,
            enhanced_text=reused_prompt or None,
        )
        if natural:
            # Unspecified optional parameters retain the cloud workflow's own
            # defaults. Explicit empty strings still override text parameters.
            omitted = {_chat_lib.node_key(n) for n in self._ordered_nodes(workflow)
                       if self._resolve_value_type(n) == "text" and not n.required
                       and not n.field_value and _chat_lib.node_key(n) not in overrides}
            node_info_list = [n for n in node_info_list
                              if f"{n['nodeId']}/{n['fieldName']}" not in omitted]

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
                reused_prompt=reused_prompt,
                text_node_id=session_text_node.node_id.strip() if session_text_node else "",
                text_field_name=session_text_node.field_name.strip() if session_text_node else "",
                editable_nodes=editable_nodes,
                chat_info=chat_info,
                phase="files" if waiting else "text",
                execution_context=kwargs,
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
                    "message": self._input_prompt(session),
                }
            return {
                "success": True,
                "waiting": True,
                "required_files": [],
                "message": f"工作流「{workflow.name}」需要描述文本，请直接发送要生成的内容。{self._input_controls(session)}",
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
                reused_prompt=reused_prompt,
                text_node_id=session_text_node.node_id.strip() if session_text_node else "",
                text_field_name=session_text_node.field_name.strip() if session_text_node else "",
                editable_nodes=editable_nodes,
                chat_info=chat_info,
                execution_context=kwargs,
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
                    "message": self._input_prompt(session),
                }
            # 无文件但需确认可编辑配置：直接进入配置确认
            session.phase = "config"
            return {"success": True, "waiting": True, "required_files": [], "message": self._config_prompt(session)}

        # 无文件、无可编辑配置：立即扩写并回填文字节点（用户输入优先，目标为第一个 prompt 节点）
        final_prompt = reused_prompt or command_text
        if command_text and text_target and not reused_prompt and workflow.llm_enhance:
            final_prompt = await self._enhance_text(workflow, command_text, stream_id=stream_id)
        if command_text and text_target:
            self._patch_text_value(
                node_info_list,
                text_target.node_id.strip(),
                text_target.field_name.strip(),
                final_prompt,
            )

        result = await self._submit_and_poll(client, workflow, node_info_list, stream_id, kwargs)
        if result.get("success") and command_text and text_target:
            await self._record_recent_prompt(
                workflow,
                command_text,
                final_prompt,
                user_id=user_id,
                platform_id=str(kwargs.get("platform_id") or ""),
            )
        return result

    async def _submit_and_poll(
        self,
        client: RunningHubClient,
        workflow: WorkflowItemSection,
        node_info_list: list[dict[str, str]],
        stream_id: str,
        kwargs: dict,
    ) -> dict[str, Any]:
        """提交任务并启动后台轮询。"""
        acquired = False
        reserved_uid = ""
        try:
            await self._semaphore.acquire()
            acquired = True
            allowed, deny_msg = self._check_access(str(kwargs.get("user_id") or ""), str(kwargs.get("group_id") or ""))
            if not allowed:
                self._semaphore.release()
                acquired = False
                return {"success": False, "message": deny_msg}
            if self.config.access.max_per_user_per_hour > 0:
                reserved_uid = str(kwargs.get("user_id") or "")
                self._user_submitting[reserved_uid] = self._user_submitting.get(reserved_uid, 0) + 1
            task_id = await client.submit(
                node_info_list,
                instance_type=workflow.instance_type,
                workflow_id=workflow.workflow_id.strip(),
            )
        except RunningHubError as exc:
            if acquired:
                self._semaphore.release()
            self.logger.error("提交任务失败: %s", exc)
            return {"success": False, "submission_attempted": True, "message": f"提交任务失败：{exc}"}
        except Exception as exc:
            if acquired:
                self._semaphore.release()
            self.logger.error("提交任务异常: %s", exc, exc_info=True)
            return {"success": False, "submission_attempted": True, "message": f"提交任务异常：{exc}"}
        except asyncio.CancelledError:
            if acquired:
                self._semaphore.release()
            raise
        finally:
            if reserved_uid:
                remaining = self._user_submitting.get(reserved_uid, 1) - 1
                if remaining:
                    self._user_submitting[reserved_uid] = remaining
                else:
                    self._user_submitting.pop(reserved_uid, None)

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
        try:
            self._remember_workflow_run(task_id, workflow, node_info_list, stream_id, kwargs)
        except Exception as exc:
            self.logger.warning("任务已提交，但复用记录保存失败: %s", exc)
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
        names = {"image": "图片", "audio": "音频", "video": "视频"}
        lines = []
        for index, item in enumerate(waiting, 1):
            required = item.get("required", getattr(item.get("node"), "required", False))
            role = item.get("label") or item.get("node_id") or "素材"
            lines.append(f"{index}. {role}（{names.get(item['value_type'], '文件')}，{'必填' if required else '可跳过'}）")
        return "\n".join(lines)

    @staticmethod
    def _input_controls(session: InputSession) -> str:
        remaining = max(0, int((_INPUT_WAIT_TIMEOUT - (time.time() - session.created_at) + 59) // 60))
        return f"请在 {remaining} 分钟内完成；取消：/wf中断"

    def _input_prompt(self, session: InputSession) -> str:
        lines = [f"工作流「{session.workflow.name}」尚未提交。"]
        if session.received_labels:
            lines.append("已准备：" + "、".join(session.received_labels))
        lines.extend(["还需按顺序发送：", self._format_waiting_summary(session.waiting_nodes)])
        if any(not n.get("required") for n in session.waiting_nodes):
            lines.append("可选素材不需要时回复「跳过剩余」；必填素材不能跳过。")
        if not session.command_text and session.text_node_id:
            lines.append("素材收齐后还需补充描述。")
        lines.append(self._input_controls(session))
        return "\n".join(lines)

    def _config_prompt(self, session: InputSession, notice: str = "") -> str:
        missing = any(n.get("required") and not n["field_value"].strip() for n in session.editable_nodes)
        lines = [notice] if notice else []
        lines += ["请填写以下参数：" if missing else "请确认以下参数：", self._build_config_edit_tips(session.editable_nodes)]
        lines.append("按顺序回复，用空格分隔；必填空项必须填写。" if missing else "按顺序回复，用空格分隔；单项用「-」保留默认，回复「不变」全部保留。")
        lines.append(self._input_controls(session))
        return "\n".join(lines)

    def _create_input_session(
        self,
        *,
        user_id: str,
        stream_id: str,
        workflow: WorkflowItemSection,
        waiting_nodes: list[dict[str, Any]],
        collected: list[dict[str, str]],
        command_text: str = "",
        reused_prompt: str = "",
        text_node_id: str = "",
        text_field_name: str = "",
        editable_nodes: list[dict[str, str]] | None = None,
        chat_info: dict[str, str] | None = None,
        phase: str = "files",
        execution_context: dict[str, Any] | None = None,
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
                    "required": item.get("required", getattr(item.get("node"), "required", False)),
                }
                for item in waiting_nodes
            ],
            collected=collected,
            command_text=command_text,
            reused_prompt=reused_prompt,
            text_node_id=text_node_id,
            text_field_name=text_field_name,
            editable_nodes=editable_nodes or [],
            chat_info=chat_info or {},
            phase=phase,
            execution_context=dict(execution_context or {}),
        )
        collected_keys = {f"{n['nodeId']}/{n['fieldName']}" for n in collected}
        session.received_labels = [n.label or _chat_lib.node_key(n) for n in self._ordered_nodes(workflow)
                                   if self._resolve_value_type(n) in {"image", "audio", "video"}
                                   and _chat_lib.node_key(n) in collected_keys]
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
                self._input_prompt(session),
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
        """把 files 列表按类型分配到等待节点并上传，返回是否已消费。

        QQ 群文件会同时以 notice(group_upload) 和普通 file 消息到达，
        这里按内容 SHA-256 去重，并且只在本次调用确实消费了新文件时才
        发送「已收到，还剩余…」汇总，避免连发两条。
        """
        async with session.consume_lock:
            if self._input_sessions.get(key) is not session:
                return True
            consumed_any = False
            for file_type, source in files:
                index = next(
                    (i for i, n in enumerate(session.waiting_nodes) if n["value_type"] == file_type),
                    None,
                )
                if index is None:
                    await self._send_text(stream_id, f"当前已不需要{type_name_of(file_type)}文件，已忽略")
                    continue
                try:
                    file_data = await self._fetch_file_bytes(source)
                except Exception as exc:
                    self.logger.error("读取待上传文件失败: %s", exc)
                    await self._send_text(stream_id, f"「{session.waiting_nodes[index]['label']}」读取失败，请重新发送该文件；其他已收文件保留。")
                    continue
                if self._input_sessions.get(key) is not session:
                    return True
                digest = hashlib.sha256(file_data).hexdigest()
                if digest in session.received_hashes:
                    # notice 和普通消息投递的同一个文件，只处理一次
                    continue
                session.received_hashes.add(digest)

                node = session.waiting_nodes.pop(index)
                try:
                    filename = self._guess_filename(source, file_type, file_data)
                    file_name = await client.upload_file(file_data, filename)
                except Exception as exc:
                    session.received_hashes.discard(digest)
                    session.waiting_nodes.insert(index, node)
                    self.logger.error("上传文件到 RunningHub 失败: %s", exc)
                    await self._send_text(stream_id, f"「{node['label']}」上传失败，请重新发送该文件；其他已收文件保留。")
                    continue
                if self._input_sessions.get(key) is not session:
                    return True
                consumed_any = True
                session.received_labels.append(node["label"])
                session.collected.append(
                    {
                        "nodeId": node["node_id"],
                        "fieldName": node["field_name"],
                        "fieldValue": file_name,
                    }
                )
                if file_type == "image":
                    try:
                        ctx = {**session.chat_info, "stream_id": session.stream_id, "user_id": session.user_id}
                        store = self._get_media_store()
                        ref = store.remember(store.owner(ctx), source, data=file_data,
                                             position=session.uploaded_images + 1)
                        session.execution_context.setdefault("image_bindings", {})[
                            f"{node['node_id']}/{node['field_name']}"
                        ] = ref["ref"]
                    except Exception as exc:
                        self.logger.warning("上传成功，但参考图片记录失败: %s", exc)
                if file_type == "image":
                    session.uploaded_images += 1
                elif file_type == "audio":
                    session.uploaded_audios += 1
                elif file_type == "video":
                    session.uploaded_videos += 1
                self.logger.info("已接收输入 %s: %s", node["label"], file_name)

            if not consumed_any:
                return True

            if session.waiting_nodes:
                await self._send_text(
                    stream_id,
                    self._input_prompt(session),
                )
                return True

            await self._after_files_collected(session, key, stream_id, client, "输入已收齐")
            return True


    async def _ask_config_edit(self, session: InputSession, stream_id: str, notice: str = "") -> None:
        """进入可编辑配置确认阶段并向用户发确认提示。"""
        session.phase = "config"
        await self._send_text(stream_id, self._config_prompt(session, notice))

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
                f"{notice}。请发送描述文本。{self._input_controls(session)}",
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
            value = node["field_value"] or ("必填，尚未填写" if node.get("required") else "未设置")
            constraints = []
            if node.get("minimum") is not None:
                constraints.append(f"至少 {node['minimum']:g}")
            if node.get("maximum") is not None:
                constraints.append(f"至多 {node['maximum']:g}")
            if node.get("choices"):
                constraints.append("可选 " + " / ".join(node["choices"]))
            suffix = f"（{'；'.join(constraints)}）" if constraints else ""
            lines.append(f"{index}. {node['label']}：{value}{suffix}")
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
                    "请先发送描述文本，文件稍后再传",
                )
            else:
                await self._send_text(stream_id, "请发送描述文本，例如：一只窗边的猫")
            return
        if self._is_finish_signal(text):
            await self._send_text(
                stream_id,
                "需要描述文本，不能跳过；请直接发送内容",
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
                self._config_prompt(session),
            )
            return
        values = self._parse_config_edit(text, len(session.editable_nodes))
        # Validate before changing any collected values or submitting a task.
        try:
            schema = {_chat_lib.node_key(n): n for n in self._ordered_nodes(session.workflow)}
            for index, item in enumerate(session.editable_nodes):
                candidate = values[index] if index < len(values) and values[index] is not None else item["field_value"]
                normalized = _chat_lib.parameter_value(schema[f"{item['node_id']}/{item['field_name']}"], candidate)
                if index < len(values) and values[index] is not None:
                    values[index] = normalized
        except ValueError as exc:
            await self._send_text(stream_id, str(exc) + "，请重新填写")
            return
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
        """领取并提交已收集的输入，同一会话只允许提交一次。"""
        if self._input_sessions.get(key) is not session:
            return
        self._remove_input_session(key)
        if session.expire_task is not None:
            session.expire_task.cancel()

        # 文字扩写延后到此刻：用实际上传的文件数量重新扩写并回填文字节点；
        # 交互补充的描述此时可能还没有对应条目，_patch_text_value 会自动追加。
        final_prompt = ""
        if session.command_text and session.text_node_id:
            final_prompt = session.reused_prompt or session.command_text
            if session.workflow.llm_enhance and not session.reused_prompt:
                actual_desc = self._format_file_counts(
                    session.uploaded_images, session.uploaded_audios, session.uploaded_videos
                )
                final_prompt = await self._enhance_text(
                    session.workflow,
                    session.command_text,
                    actual_file_desc=actual_desc,
                    stream_id=stream_id,
                )
            session.collected = self._patch_text_value(
                session.collected,
                session.text_node_id,
                session.text_field_name,
                final_prompt,
            )

        # 用触发时的 chat_info 构造扁平 kwargs，_extract_chat_info 能识别，恢复 NapCat 直发与自动撤回
        kwargs = {
            **session.execution_context,
            "group_id": str(session.chat_info.get("group_id") or ""),
            "user_id": str(session.chat_info.get("user_id") or ""),
            "platform_id": str(session.chat_info.get("platform_id") or ""),
        }
        result = await self._submit_and_poll(
            client, session.workflow, session.collected, stream_id, kwargs
        )
        if result.get("success") and final_prompt:
            await self._record_recent_prompt(
                session.workflow,
                session.command_text,
                final_prompt,
                user_id=str(session.chat_info.get("user_id") or session.user_id),
                platform_id=str(session.chat_info.get("platform_id") or ""),
            )
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
        if session.consume_lock.locked():
            await self._send_text(stream_id, "文件正在上传，请等待处理完成；如需取消，请发送 /wf中断")
            return True
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
        required = {_chat_lib.node_key(n) for n in self._ordered_nodes(session.workflow) if n.required}
        missing = [n["label"] for n in session.waiting_nodes
                   if f"{n['node_id']}/{n['field_name']}" in required]
        if missing:
            await self._send_text(stream_id, "还缺少必填文件：" + "、".join(missing) + "，请发送文件或 /wf中断")
            return True
        if skipped:
            notice = f"已跳过剩余 {skipped} 个文件"
            session.waiting_nodes.clear()
        else:
            notice = "输入已收齐"
        await self._after_files_collected(session, key, stream_id, client, notice)
        return True

    def _cancel_input_session(self, key: str) -> None:
        session = self._remove_input_session(key)
        if session is not None and session.expire_task is not None:
            session.expire_task.cancel()

    @staticmethod
    def _prompt_summary(text: str, limit: int = 120) -> str:
        """把多行提示词压成适合编号列表展示的一行。"""
        summary = re.sub(r"\s+", " ", str(text or "")).strip()
        return summary if len(summary) <= limit else summary[: limit - 1] + "…"

    def _remove_prompt_interaction(self, key: str) -> PromptInteraction | None:
        interaction = self._prompt_interactions.pop(key, None)
        if (
            interaction is not None
            and interaction.expire_task is not None
            and interaction.expire_task is not asyncio.current_task()
        ):
            interaction.expire_task.cancel()
        return interaction

    def _register_prompt_interaction(self, interaction: PromptInteraction) -> str:
        """注册提示词选择交互，并取消同用户在当前会话中的旧输入收集。"""
        key = self._session_key(interaction.user_id, interaction.stream_id)
        self._remove_prompt_interaction(key)
        self._cancel_input_session(key)
        self._prompt_interactions[key] = interaction

        async def _expire() -> None:
            await asyncio.sleep(_INPUT_WAIT_TIMEOUT)
            if self._prompt_interactions.get(key) is interaction:
                self._prompt_interactions.pop(key, None)
                if interaction.stream_id:
                    try:
                        await self._send_text(
                            interaction.stream_id, "提示词选择已超时，请重新输入命令"
                        )
                    except (OSError, RuntimeError):
                        pass

        interaction.expire_task = asyncio.create_task(_expire())
        return key

    def _find_workflow_for_prompt(self, entry: dict[str, Any]) -> WorkflowItemSection | None:
        """优先按工作流 ID 找当前配置，兼容工作流在保存后改名。"""
        workflow_id = str(entry.get("workflow_id") or "").strip()
        region = str(entry.get("region") or "overseas").strip()
        if workflow_id:
            for workflow in self._workflows:
                if (
                    str(workflow.workflow_id or "").strip() == workflow_id
                    and str(workflow.region or "overseas").strip() == region
                ):
                    return workflow
            return None
        return self._find_workflow(str(entry.get("workflow_name") or ""))

    async def _start_cached_prompt(
        self, event: AstrMessageEvent, entry: dict[str, Any]
    ) -> dict[str, Any]:
        """使用已扩写提示词启动原工作流，重新收集文件与可编辑参数。"""
        workflow = self._find_workflow_for_prompt(entry)
        if workflow is None:
            return {
                "success": False,
                "message": "原工作流已不存在或区域已改变，无法复用该提示词",
            }
        kwargs = self._event_ctx(event)
        kwargs["trigger"] = "cached_prompt"
        kwargs["reused_prompt"] = str(entry.get("enhanced_prompt") or "").strip()
        return await self._start_workflow(
            workflow.name,
            str(entry.get("original_prompt") or entry.get("enhanced_prompt") or "").strip(),
            **kwargs,
        )

    @staticmethod
    def _parse_prompt_choice(text: str, count: int) -> int | None:
        text = str(text or "").strip()
        if not text.isdigit():
            return None
        choice = int(text)
        return choice - 1 if 1 <= choice <= count else None

    @staticmethod
    def _parse_prompt_delete_choice(text: str, count: int) -> int | None:
        """解析保存列表中的删除指令，例如「删除1」或「删除 1」。"""
        match = re.fullmatch(r"(?:删除|刪除|删|刪)\s*(\d+)", str(text or "").strip())
        if match is None:
            return None
        choice = int(match.group(1))
        return choice - 1 if 1 <= choice <= count else None

    @staticmethod
    def _saved_prompt_menu(entries: list[dict[str, Any]]) -> str:
        """构建已保存提示词的短列表。"""
        lines = ["已保存提示词："]
        for index, entry in enumerate(entries, 1):
            description = RunningHubGenericPlugin._prompt_summary(
                str(entry.get("description") or ""), 80
            )
            lines.append(f"{index}. {description} [{entry.get('workflow_name') or '未知工作流'}]")
        lines.append("回复数字运行，删除+数字删除，或取消")
        return "\n".join(lines)

    async def _delete_saved_prompt_entry(
        self, owner_key: str, entry: dict[str, Any]
    ) -> bool:
        """从持久保存列表删除一条提示词。"""
        async with self._prompt_library_lock:
            owner = self._prompt_library.get(owner_key)
            saved = owner.get("saved", []) if owner else []
            for index, item in enumerate(saved):
                same_content = (
                    item.get("workflow_id") == entry.get("workflow_id")
                    and item.get("region") == entry.get("region")
                    and item.get("enhanced_prompt") == entry.get("enhanced_prompt")
                )
                if not same_content:
                    continue
                saved.pop(index)
                try:
                    await asyncio.to_thread(self._write_prompt_library_file)
                except OSError as exc:  # pragma: no cover
                    self.logger.warning("[提示词] 写入删除结果失败: %s", exc)
                return True
        return False

    async def _finish_prompt_file_upload(
        self,
        interaction: PromptInteraction,
        filename: str,
        file_data: bytes,
    ) -> bool:
        """校验并保存聊天中上传的提示词模板文件。"""
        key = self._session_key(interaction.user_id, interaction.stream_id)
        try:
            content, source_encoding = self._decode_prompt_file_content(file_data)
            target = self._safe_prompt_template(Path(filename).name)
            if target is None:
                raise ValueError("仅支持 .md 或 .txt 文件")
            existed = target.exists()
            await asyncio.to_thread(target.write_text, content, encoding="utf-8")
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            await self._send_text(interaction.stream_id, f"上传失败：{exc}")
            return True
        self._remove_prompt_interaction(key)
        suffix = "（已覆盖）" if existed else ""
        if source_encoding != "UTF-8":
            suffix += "（已转 UTF-8）"
        await self._send_text(interaction.stream_id, f"提示词模板已上传：{target.name}{suffix}")
        return True

    async def _finish_uploaded_prompt(
        self,
        interaction: PromptInteraction,
        filename: str,
        file_data: bytes,
    ) -> bool:
        """把上传文件作为一条可在「/wf 提示词」中运行的记录保存。"""
        key = self._session_key(interaction.user_id, interaction.stream_id)
        try:
            content, source_encoding = self._decode_prompt_file_content(file_data)
            workflow = interaction.selected or {}
            workflow_name = str(workflow.get("workflow_name") or "").strip()
            workflow_id = str(workflow.get("workflow_id") or "").strip()
            if not workflow_name:
                raise ValueError("未选择工作流")
            description = Path(filename).stem.strip(" ._-") or "上传提示词"
            entry = {
                "workflow_name": workflow_name,
                "workflow_id": workflow_id,
                "region": str(workflow.get("region") or "overseas").strip() or "overseas",
                "original_prompt": content,
                "enhanced_prompt": content,
                "description": description,
                "created_at": time.time(),
                "saved_at": 0.0,
            }
            await self._save_prompt_entry(interaction.owner_key, entry, description)
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            await self._send_text(interaction.stream_id, f"上传失败：{exc}")
            return True
        self._remove_prompt_interaction(key)
        suffix = "（已转 UTF-8）" if source_encoding != "UTF-8" else ""
        await self._send_text(
            interaction.stream_id,
            f"已加入提示词：{description} [{workflow_name}]{suffix}",
        )
        return True

    @staticmethod
    def _decode_prompt_file_content(file_data: bytes) -> tuple[str, str]:
        """限制、解码并规范化上传的提示词文本。"""
        if len(file_data) > _PROMPT_TEMPLATE_MAX_BYTES:
            raise ValueError("文件不能超过 2MB")
        content, source_encoding = RunningHubGenericPlugin._decode_prompt_template_bytes(file_data)
        content = content.replace("\r\n", "\n").replace("\r", "\n")
        if not content.strip():
            raise ValueError("文件内容不能为空")
        return content, source_encoding

    @staticmethod
    def _decode_prompt_template_bytes(file_data: bytes) -> tuple[str, str]:
        """识别常见文本编码，并返回统一写入 UTF-8 的文本。"""
        if file_data.startswith(codecs.BOM_UTF8):
            return file_data.decode("utf-8-sig"), "UTF-8"
        if file_data.startswith(codecs.BOM_UTF32_LE) or file_data.startswith(codecs.BOM_UTF32_BE):
            return file_data.decode("utf-32"), "UTF-32"
        if file_data.startswith(codecs.BOM_UTF16_LE) or file_data.startswith(codecs.BOM_UTF16_BE):
            return file_data.decode("utf-16"), "UTF-16"

        try:
            return file_data.decode("utf-8"), "UTF-8"
        except UnicodeDecodeError:
            pass

        if _detect_text_encoding is not None:
            try:
                detected = _detect_text_encoding(file_data).best()
                encoding = str(getattr(detected, "encoding", "") or "").strip()
                if encoding:
                    return file_data.decode(encoding), encoding.upper().replace("_", "-")
            except (LookupError, UnicodeDecodeError):
                pass

        # 无 BOM 的 UTF-16 文本通常含有大量 NUL 字节，先尝试按其端序解码。
        if b"\x00" in file_data[:256]:
            for encoding in ("utf-16-le", "utf-16-be"):
                try:
                    return file_data.decode(encoding), "UTF-16"
                except UnicodeDecodeError:
                    continue

        # GB18030 是 GBK 的超集，可覆盖 Windows 中文编辑器常见的 ANSI 文件。
        for encoding in ("gb18030", "big5"):
            try:
                return file_data.decode(encoding), encoding.upper()
            except UnicodeDecodeError:
                continue
        raise UnicodeDecodeError(
            "prompt-template",
            file_data,
            0,
            min(len(file_data), 1),
            "无法识别文件编码，请另存为文本文件后重试",
        )

    async def _extract_prompt_file_from_event(
        self, event: AstrMessageEvent
    ) -> tuple[str, bytes] | None:
        """从聊天消息中读取第一个 .md/.txt 文件。"""
        for comp in event.get_messages():
            if not isinstance(comp, FileComponent):
                continue
            filename = str(getattr(comp, "name", "") or "").strip()
            if filename and Path(filename).suffix.lower() not in {".md", ".txt"}:
                continue
            try:
                # 先取 URL/本地路径但不下载，避免把 NapCat 的 gzc 直链下载成错误 ZIP。
                source = str(await comp.get_file(allow_return_url=True) or "").strip()
                if any(
                    marker in source.lower()
                    for marker in ("gzc-download.ftn.qq.com", "ftn.qq.com")
                ):
                    continue
                if not source:
                    source = str(await comp.get_file() or "").strip()
                elif source.startswith(("http://", "https://")):
                    source = str(await comp.get_file() or "").strip()
                source = source.removeprefix("file:///")
                if not filename:
                    filename = Path(source.split("?", 1)[0]).name
                if Path(filename).suffix.lower() not in {".md", ".txt"}:
                    continue
                return filename, await self._fetch_file_bytes(source)
            except Exception as exc:
                self.logger.warning("读取提示词文件失败: %s", exc)
        return None

    async def _handle_prompt_interaction(
        self, interaction: PromptInteraction, event: AstrMessageEvent
    ) -> bool:
        """消费保存描述、提示词选择或模板文件消息。"""
        text = self._extract_text_from_event(event).strip()
        key = self._session_key(interaction.user_id, interaction.stream_id)
        stream_id = str(event.unified_msg_origin or interaction.stream_id)
        command_text = text.lstrip("/").strip()
        if text.startswith("/") or command_text == "wf" or command_text.startswith("wf "):
            self._remove_prompt_interaction(key)
            return False
        if text.lower() in {"取消", "退出", "cancel", "quit"}:
            self._remove_prompt_interaction(key)
            await self._send_text(stream_id, "已取消提示词操作")
            return True

        if interaction.phase in {"upload_template_file", "upload_prompt_file"}:
            uploaded = await self._extract_prompt_file_from_event(event)
            if uploaded is None:
                await self._send_text(stream_id, "请发送 .md 或 .txt 文件，或回复取消")
                return True
            if interaction.phase == "upload_template_file":
                await self._finish_prompt_file_upload(interaction, *uploaded)
            else:
                await self._finish_uploaded_prompt(interaction, *uploaded)
            return True

        if interaction.phase == "upload_prompt_workflow":
            choice = self._parse_prompt_choice(text, len(interaction.entries))
            if choice is None:
                await self._send_text(stream_id, f"请回复 1-{len(interaction.entries)}，或取消")
                return True
            interaction.selected = dict(interaction.entries[choice])
            interaction.phase = "upload_prompt_file"
            await self._send_text(stream_id, "请发送 .md 或 .txt 提示词文件，或取消")
            return True

        if interaction.phase == "save_description":
            if not text:
                await self._send_text(stream_id, "请发送保存描述，或取消")
                return True
            if len(text) > 100:
                await self._send_text(stream_id, "保存描述最多 100 字")
                return True
            selected = interaction.selected
            if selected is None:
                self._remove_prompt_interaction(key)
                await self._send_text(stream_id, "记录已失效，请重新输入 /wf 保存提示词")
                return True
            await self._save_prompt_entry(interaction.owner_key, selected, text)
            self._remove_prompt_interaction(key)
            await self._send_text(stream_id, f"已保存提示词：{text}")
            return True

        delete_choice = self._parse_prompt_delete_choice(text, len(interaction.entries))
        if interaction.phase == "run_select" and delete_choice is not None:
            selected = interaction.entries[delete_choice]
            deleted = await self._delete_saved_prompt_entry(interaction.owner_key, selected)
            if not deleted:
                await self._send_text(stream_id, "提示词已不存在，请重新输入 /wf 提示词")
                self._remove_prompt_interaction(key)
                return True
            remaining = self._prompt_entries(interaction.owner_key, "saved")
            if remaining:
                interaction.entries = remaining
                await self._send_text(
                    stream_id,
                    f"已删除：{selected.get('description') or '未命名'}\n{self._saved_prompt_menu(remaining)}",
                )
            else:
                self._remove_prompt_interaction(key)
                await self._send_text(stream_id, "已删除，暂无保存的提示词")
            return True

        choice = self._parse_prompt_choice(text, len(interaction.entries))
        if choice is None:
            await self._send_text(
                stream_id,
                f"请回复 1-{len(interaction.entries)}，或取消",
            )
            return True
        selected = interaction.entries[choice]
        if interaction.phase == "save_select":
            interaction.selected = selected
            interaction.phase = "save_description"
            await self._send_text(
                stream_id,
                "请发送这条提示词的保存描述（例：雨夜霓虹猫），或取消",
            )
            return True

        self._remove_prompt_interaction(key)
        result = await self._start_cached_prompt(event, selected)
        await self._send_text(stream_id, result["message"])
        return True

    def _build_waiting_tips_from_dicts(self, waiting: list[dict[str, str]]) -> str:
        return self._format_waiting_summary(waiting)

    def _event_has_qq_group_file_component(self, event: AstrMessageEvent) -> bool:
        """判断消息里是否有 QQ 群文件直链组件（其真实内容由 notice 提供）。"""
        for comp in event.get_messages():
            if isinstance(comp, FileComponent):
                source = str(getattr(comp, "url", "") or getattr(comp, "file", "") or "")
                if any(
                    marker in source.lower()
                    for marker in ("gzc-download.ftn.qq.com", "ftn.qq.com")
                ):
                    return True
        return False

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
                    # QQ 群文件的普通消息里是 gzc-download.ftn.qq.com 直链，
                    # 直链下载到的是错误内容；真实文件由 notice group_upload 获取。
                    if any(
                        marker in source.lower()
                        for marker in ("gzc-download.ftn.qq.com", "ftn.qq.com")
                    ):
                        continue
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
    def _is_napcat_group_upload_notice(event: AstrMessageEvent) -> bool:
        """判断事件是否为 NapCat 群文件通知。"""
        raw = getattr(getattr(event, "message_obj", None), "raw_message", None)
        return bool(
            isinstance(raw, dict)
            and str(raw.get("post_type") or "") == "notice"
            and str(raw.get("notice_type") or "") == "group_upload"
        )

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
        task_meta = self._task_meta.get(task_id) or {}
        workflow_name = str(task_meta.get("name") or "")
        try:
            try:
                result = await client.wait_for_result(task_id)
            except (RunningHubError, TimeoutError) as exc:
                self.logger.error("任务 %s 未成功完成: %s", task_id, exc)
                if stream_id:
                    await self._send_text(stream_id, f"任务 {task_id} 等待超时，尚未确认生成结果。请到 RunningHub 核对任务状态，避免重复生成。" if isinstance(exc, TimeoutError) else f"任务 {task_id} 未成功取得生成结果，请到 RunningHub 查看任务状态和失败原因。")
                return

            # 任务成功后立即记录消耗（只存 task_id / 工作流 / RH 币）
            try:
                await self._record_task_history(task_id, workflow_name, self._consume_coins_from_result(result))
            except Exception as exc:
                self.logger.warning("记录任务消耗失败: %s", exc)


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
                    await self._send_text(stream_id, f"任务 {task_id} 已结束，但未返回可发送的结果，请检查工作流输出节点。")
                return

            record = {
                "owner": _chat_lib.MediaStore.owner({**(kwargs or {}), "stream_id": stream_id}),
                "task_id": task_id, "workflow": workflow_name, "region": task_meta.get("region", "overseas"),
                "outputs": [{"url": url, "type": kind, "sent": False, "image_ref": ""} for url, kind in result_items],
                "saved": False,
            }
            try:
                if record["owner"]:
                    record["saved"] = True
                    self._get_media_store().remember_delivery(**record)
            except Exception as exc:
                record["saved"] = False
                self.logger.warning("保存补发记录失败: %s", exc)
            if stream_id:
                target = DeliveryTarget.from_dict({**chat_info, "stream_id": stream_id})
                await self._deliver_saved_results(record, target, client)
        except asyncio.CancelledError:
            self.logger.info("任务 %s 已被取消", task_id)
            raise
        except Exception as exc:
            self.logger.error("任务 %s 处理异常: %s", task_id, exc, exc_info=True)
            if stream_id:
                await self._send_text(stream_id, f"任务 {task_id} 处理结果时发生异常；若已有结果记录，可使用 /wf补发 {task_id}。详细原因已记录日志。")
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

    @filter.event_message_type(filter.EventMessageType.ALL, priority=10000)
    async def remember_chat_images(self, event: AstrMessageEvent) -> None:
        """记录本会话用户图片；不消费消息、不主动调用模型。"""
        await self._observe_chat_images(event)

    @filter.event_message_type(filter.EventMessageType.ALL, priority=9999)
    async def handle_input_collector(self, event: AstrMessageEvent) -> None:
        """拦截交互式输入会话中的文件 / 控制词消息，阻止其继续进入 LLM。"""
        # QQ 群文件会先产生 notice，再产生一条空的 file 消息；放行 notice，
        # 由后面的 handle_notice_collector 通过 OneBot API 下载真实文件。
        if self._is_napcat_group_upload_notice(event):
            return
        user_id = str(event.get_sender_id() or "")
        stream_id = str(event.unified_msg_origin or "")
        text = self._extract_text_from_event(event).strip()
        if re.match(r"^/?wf中断(?:\s|$)", text):
            return
        if text.startswith("/") and not self._is_finish_signal(text):
            return
        interaction = self._prompt_interactions.get(self._session_key(user_id, stream_id))
        if interaction is not None and await self._handle_prompt_interaction(
            interaction, event
        ):
            self._mark_handled(event)
            return
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
        if self._event_has_qq_group_file_component(event):
            # 真实文件由 handle_notice_collector 处理，这里只拦截普通消息，避免进入默认 LLM
            self._mark_handled(event)
            return

    @filter.command_group("wf")
    def prompt_command_group(self) -> None:
        """提示词缓存与复用命令组。"""
        pass

    @prompt_command_group.command("保存提示词")
    async def handle_prompt_save_command(self, event: AstrMessageEvent) -> None:
        """从最近提示词中选择一条持久保存。"""
        await self._handle_prompt_command(event, "保存提示词")

    @prompt_command_group.command("提示词重跑")
    async def handle_prompt_rerun_command(self, event: AstrMessageEvent) -> None:
        """复用最近一次扩写提示词重新运行。"""
        await self._handle_prompt_command(event, "提示词重跑")

    @prompt_command_group.command("提示词")
    async def handle_prompt_list_command(self, event: AstrMessageEvent) -> None:
        """选择已保存提示词并运行。"""
        await self._handle_prompt_command(event, "提示词")

    @prompt_command_group.command("上传提示词")
    async def handle_prompt_upload_command(self, event: AstrMessageEvent) -> None:
        """上传一条可在「/wf 提示词」中选择运行的提示词。"""
        await self._handle_prompt_command(event, "上传提示词")

    @prompt_command_group.command("上传提示词模板")
    async def handle_prompt_upload_template_command(self, event: AstrMessageEvent) -> None:
        """上传并保存一个 .md/.txt 扩写提示词模板。"""
        await self._handle_prompt_command(event, "上传提示词模板")

    async def _handle_prompt_command(
        self, event: AstrMessageEvent, action: str
    ) -> None:
        """执行提示词命令组的子命令。"""
        if self._is_consumed(event):
            return
        ctx = self._event_ctx(event)
        stream_id = ctx["stream_id"]
        allowed, deny_msg = self._check_access(ctx["user_id"], ctx["group_id"])
        if not allowed:
            await self._send_text(stream_id, deny_msg)
            self._mark_handled(event)
            return
        owner_key = self._prompt_owner_key(ctx["user_id"], ctx["platform_id"])
        session_key = self._session_key(ctx["user_id"], stream_id)

        if action == "保存提示词":
            entries = self._prompt_entries(owner_key, "recent")[:_RECENT_PROMPT_MAX]
            if not entries:
                await self._send_text(stream_id, "暂无最近提示词，请先运行一次工作流")
            else:
                lines = ["最近提示词（原描述）："]
                for index, entry in enumerate(entries, 1):
                    summary = self._prompt_summary(str(entry.get("original_prompt") or ""))
                    lines.append(f"{index}. [{entry['workflow_name']}] {summary}")
                lines.append("回复数字保存，或取消")
                self._register_prompt_interaction(
                    PromptInteraction(
                        user_id=ctx["user_id"],
                        stream_id=stream_id,
                        owner_key=owner_key,
                        phase="save_select",
                        entries=entries,
                    )
                )
                await self._send_text(stream_id, "\n".join(lines))
        elif action == "提示词重跑":
            entries = self._prompt_entries(owner_key, "recent")
            if not entries:
                await self._send_text(stream_id, "暂无最近提示词，无法重跑")
            else:
                self._remove_prompt_interaction(session_key)
                self._cancel_input_session(session_key)
                result = await self._start_cached_prompt(event, entries[0])
                await self._send_text(stream_id, result["message"])
        elif action == "提示词":
            entries = self._prompt_entries(owner_key, "saved")
            if not entries:
                await self._send_text(stream_id, "暂无保存提示词，请先用 /wf 保存提示词")
            else:
                self._register_prompt_interaction(
                    PromptInteraction(
                        user_id=ctx["user_id"],
                        stream_id=stream_id,
                        owner_key=owner_key,
                        phase="run_select",
                        entries=entries,
                    )
                )
                await self._send_text(stream_id, self._saved_prompt_menu(entries))
        elif action == "上传提示词":
            workflows = [
                {
                    "workflow_name": workflow.name,
                    "workflow_id": workflow.workflow_id,
                    "region": workflow.region,
                }
                for workflow in self._workflows
                if str(workflow.name or "").strip()
            ]
            if not workflows:
                await self._send_text(stream_id, "暂无可用工作流，请先配置工作流")
            elif len(workflows) == 1:
                self._register_prompt_interaction(
                    PromptInteraction(
                        user_id=ctx["user_id"],
                        stream_id=stream_id,
                        owner_key=owner_key,
                        phase="upload_prompt_file",
                        selected=workflows[0],
                    )
                )
                await self._send_text(stream_id, "请发送 .md 或 .txt 提示词文件，或取消")
            else:
                self._register_prompt_interaction(
                    PromptInteraction(
                        user_id=ctx["user_id"],
                        stream_id=stream_id,
                        owner_key=owner_key,
                        phase="upload_prompt_workflow",
                        entries=workflows,
                    )
                )
                lines = ["请选择提示词对应的工作流："]
                for index, workflow in enumerate(workflows, 1):
                    lines.append(
                        f"{index}. {workflow['workflow_name']} [{workflow.get('region') or 'overseas'}]"
                    )
                lines.append("回复数字，或取消")
                await self._send_text(stream_id, "\n".join(lines))
        elif action == "上传提示词模板":
            self._register_prompt_interaction(
                PromptInteraction(
                    user_id=ctx["user_id"],
                    stream_id=stream_id,
                    owner_key=owner_key,
                    phase="upload_template_file",
                )
            )
            await self._send_text(stream_id, "请发送 .md 或 .txt 模板文件，或取消")
        else:
            await self._send_text(
                stream_id,
                "提示词命令：\n"
                "/wf 保存提示词：保存最近记录\n"
                "/wf 提示词重跑：重跑最近记录\n"
                "/wf 提示词：运行或删除已保存记录\n"
                "/wf 上传提示词：上传可运行提示词\n"
                "/wf 上传提示词模板：上传 .md/.txt 模板",
            )
        self._mark_handled(event)

    @filter.command("wf补发")
    async def handle_resend_result(self, event: AstrMessageEvent) -> None:
        """补发本会话用户的已生成结果，不触发新任务。"""
        if self._is_consumed(event):
            return
        self._mark_handled(event)
        await self._resend_result_command(event)

    @filter.command("wf中断")
    async def handle_rh_cancel(self, event: AstrMessageEvent) -> None:
        """中断任务：还在传文件阶段则直接结束；已提交则回复编号取消运行中的任务。"""
        if self._is_consumed(event):
            return
        stream_id = str(event.unified_msg_origin or "")
        user_id = str(event.get_sender_id() or "")
        group_id = str(event.get_group_id() or "")
        allowed, deny_msg = self._check_access(user_id, group_id, check_quota=False)
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
        interaction = self._prompt_interactions.get(self._session_key(user_id, stream_id))
        if interaction is not None and interaction.phase in {
            "upload_template_file",
            "upload_prompt_file",
        }:
            if Path(filename).suffix.lower() not in {".md", ".txt"}:
                await self._send_text(stream_id, "请发送 .md 或 .txt 提示词文件")
                self._mark_handled(event)
                return
            try:
                file_data = await self._fetch_napcat_file_bytes(file_id, group_id, event=event)
            except Exception as exc:
                self.logger.error("获取提示词文件失败: %s", exc)
                await self._send_text(stream_id, f"获取文件失败：{exc}")
                self._mark_handled(event)
                return
            if interaction.phase == "upload_template_file":
                await self._finish_prompt_file_upload(interaction, filename, file_data)
            else:
                await self._finish_uploaded_prompt(interaction, filename, file_data)
            self._mark_handled(event)
            return
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
            # NapCat 不同版本会把真实文件放在 file/base64/data、URL 或本地路径中。
            # 只有明确的 base64 值才进入解码，不能把 URL/路径当作 base64 处理。
            for field_name in ("file", "base64", "data"):
                candidate = data.get(field_name)
                if not isinstance(candidate, str):
                    continue
                candidate = candidate.strip()
                if not candidate:
                    continue
                if candidate.startswith("base64://"):
                    try:
                        return self._decode_base64_bounded(candidate[len("base64://"):])
                    except RunningHubError:
                        raise
                    except Exception:
                        continue
                if candidate.startswith(("http://", "https://")):
                    client = self._client or self._client_cn
                    if client is not None:
                        return await client.download_bytes(candidate)
                    continue
                local_path = candidate.removeprefix("file:///")
                path = Path(local_path)
                if path.is_file():
                    if path.stat().st_size > _MAX_FILE_BYTES:
                        raise RunningHubError(
                            f"本地文件超过 {_MAX_FILE_BYTES} 字节上限: {local_path}"
                        )
                    return await asyncio.to_thread(path.read_bytes)
                # 仅允许 base64/base64 字段携带无前缀编码，避免误解码文件名。
                if field_name in {"base64", "data"}:
                    try:
                        return self._decode_base64_bounded(candidate)
                    except RunningHubError:
                        raise
                    except Exception:
                        continue
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
        errors = [error["message"] for item in items for error in _validation_lib.workflow_errors(item)]
        if errors:
            raise ValueError("\n".join(errors))
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
        """Only global workflow information belongs in the shared tool description."""
        names = "、".join(self._llm_callable_workflow_names()) or "（无）"
        description = (
            "根据用户明确的生成或修改要求运行 RunningHub 工作流，支持聊天图片、引用图片和参数。"
            f"当前可用名称：{names}。"
            "涉及图片、参数、继续修改或不确定工作流用途时，先调用 get_workflow_context。"
            "用它返回的图片编号和输入 key，不得编造 URL、图片内容或参数。"
            "修改生成结果时选择对应 generated 图片；沿用原参考图和参数时使用 reuse_task_id。"
            "缺少素材时按工具返回提示补充；尚未提交不得声称已开始。每轮只提交一次，不要轮询。"
        )
        try:
            tool = self.context.get_llm_tool_manager().get_func("run_workflow")
            if tool is not None:
                tool.description = description
        except Exception as exc:
            self.logger.debug("刷新 run_workflow 工具描述失败: %s", exc)

    @filter.llm_tool("get_workflow_context")
    async def handle_workflow_context(self, event: AstrMessageEvent) -> str:
        """查看可用工作流用途、输入角色、参数约束、当前/引用/近期图片编号和可复用任务。
        当用户要求生成、编辑图片或说“刚才那张”“生成的第二张”时先调用本工具。
        图片列表不包含视觉描述，不得凭编号猜测画面。多图用途不明确时询问用户。
        返回数据仅限当前会话用户；用户主动引用的图片也可使用。

        Args:
        """
        return await self._natural_context(event)

    @filter.llm_tool("run_workflow")
    async def handle_run_workflow(
        self, event: AstrMessageEvent, workflow_name: str = "", prompt: str = "",
        image_refs: list[str] | None = None,
        image_bindings: dict[str, str] | None = None,
        parameters: dict[str, Any] | None = None,
        reuse_task_id: str = "",
    ) -> str:
        """按用户明确的生成/编辑要求运行工作流。涉及图片或参数时先调用 get_workflow_context。
        每轮只运行一次；根据返回值区分等待补充和已提交，不要轮询。

        Args:
            workflow_name(string): 工作流名称，从 get_workflow_context 返回列表选择；复用任务时可留空
            prompt(string): 用户完整的生成/修改要求；复用任务时只填本次修改要求，留空沿用原提示词
            image_refs(array[string]): 单图工作流的图片编号；省略时使用当前消息或引用图片，空数组表示不用聊天图片
            image_bindings(object): 多图角色绑定，key 为输入的 节点ID/字段名，value 为图片编号；与 image_refs 二选一
            parameters(object): 用户明确要求修改的可编辑参数，key 为 节点ID/字段名，value 满足参数约束；固定节点不可修改
            reuse_task_id(string): 从 get_workflow_context 选择原任务 ID，继承其提示词、参数和原参考图；修改生成结果需另外指定 generated 图片
        """
        return await self._run_natural_workflow(event, workflow_name, prompt, image_refs,
                                                image_bindings, parameters, reuse_task_id)

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

    # ── 提示词缓存 ────────────────────────────────────────────────

    @staticmethod
    def _prompt_owner_key(user_id: str, platform_id: str = "") -> str:
        """生成提示词库所有者键，避免不同平台的相同用户 ID 串库。"""
        user_id = str(user_id or "").strip()
        platform_id = str(platform_id or "").strip()
        if not user_id:
            return ""
        return f"{platform_id}:{user_id}" if platform_id else user_id

    def _prompt_library_path(self) -> Path:
        """提示词库文件路径（保存在 AstrBot 插件持久化目录）。"""
        directory = Path(get_astrbot_plugin_data_path()).resolve() / _PLUGIN_DIR.name
        directory.mkdir(parents=True, exist_ok=True)
        return directory / _PROMPT_LIBRARY_FILE

    @staticmethod
    def _normalise_prompt_entry(raw: Any) -> dict[str, Any] | None:
        """校验持久化提示词条目，只接收运行所需的稳定字段。"""
        if not isinstance(raw, dict):
            return None
        workflow_name = str(raw.get("workflow_name") or "").strip()
        workflow_id = str(raw.get("workflow_id") or "").strip()
        original_prompt = str(raw.get("original_prompt") or "").strip()
        enhanced_prompt = str(raw.get("enhanced_prompt") or "").strip()
        if not workflow_name or not enhanced_prompt:
            return None
        try:
            created_at = float(raw.get("created_at") or 0)
        except (TypeError, ValueError):
            created_at = 0.0
        try:
            saved_at = float(raw.get("saved_at") or 0)
        except (TypeError, ValueError):
            saved_at = 0.0
        return {
            "workflow_name": workflow_name,
            "workflow_id": workflow_id,
            "region": str(raw.get("region") or "overseas").strip() or "overseas",
            "original_prompt": original_prompt or enhanced_prompt,
            "enhanced_prompt": enhanced_prompt,
            "description": str(raw.get("description") or "").strip(),
            "created_at": created_at,
            "saved_at": saved_at,
        }

    def _load_prompt_library(self) -> None:
        """从磁盘读取按用户隔离的最近提示词和持久保存提示词。"""
        path = self._prompt_library_path()
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return
        except (OSError, json.JSONDecodeError) as exc:
            self.logger.warning("[提示词缓存] 读取文件失败: %s", exc)
            return
        users = raw.get("users") if isinstance(raw, dict) else None
        if not isinstance(users, dict):
            return
        library: dict[str, dict[str, list[dict[str, Any]]]] = {}
        for owner_key, owner_data in users.items():
            if not isinstance(owner_key, str) or not isinstance(owner_data, dict):
                continue
            recent = [
                entry
                for item in owner_data.get("recent") or []
                if (entry := self._normalise_prompt_entry(item)) is not None
            ][:_RECENT_PROMPT_MAX]
            saved = [
                entry
                for item in owner_data.get("saved") or []
                if (entry := self._normalise_prompt_entry(item)) is not None
                and entry["description"]
            ]
            if recent or saved:
                library[owner_key] = {"recent": recent, "saved": saved}
        self._prompt_library = library

    def _write_prompt_library_file(self) -> None:
        """原子写入提示词库文件（调用方需持有 _prompt_library_lock）。"""
        path = self._prompt_library_path()
        temp = path.with_suffix(path.suffix + ".tmp")
        temp.write_text(
            json.dumps({"version": 1, "users": self._prompt_library}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp.replace(path)

    def _prompt_entries(self, owner_key: str, kind: str) -> list[dict[str, Any]]:
        """返回某用户的提示词条目副本，防止交互过程中被新任务改写。"""
        owner = self._prompt_library.get(owner_key) or {}
        entries = owner.get(kind) or []
        return [dict(item) for item in entries]

    async def _record_recent_prompt(
        self,
        workflow: WorkflowItemSection,
        original_prompt: str,
        enhanced_prompt: str,
        *,
        user_id: str,
        platform_id: str = "",
    ) -> None:
        """记录一次成功提交使用的最终提示词，按工作流和内容去重。"""
        owner_key = self._prompt_owner_key(user_id, platform_id)
        enhanced_prompt = str(enhanced_prompt or "").strip()
        if not owner_key or not enhanced_prompt:
            return
        entry = {
            "workflow_name": str(workflow.name or "").strip(),
            "workflow_id": str(workflow.workflow_id or "").strip(),
            "region": str(workflow.region or "overseas").strip(),
            "original_prompt": str(original_prompt or enhanced_prompt).strip(),
            "enhanced_prompt": enhanced_prompt,
            "description": "",
            "created_at": time.time(),
            "saved_at": 0.0,
        }
        async with self._prompt_library_lock:
            owner = self._prompt_library.setdefault(owner_key, {"recent": [], "saved": []})
            recent = owner.setdefault("recent", [])
            recent[:] = [
                item
                for item in recent
                if not (
                    item.get("workflow_id") == entry["workflow_id"]
                    and item.get("enhanced_prompt") == enhanced_prompt
                )
            ]
            recent.insert(0, entry)
            del recent[_RECENT_PROMPT_MAX:]
            try:
                await asyncio.to_thread(self._write_prompt_library_file)
            except OSError as exc:  # pragma: no cover
                self.logger.warning("[提示词缓存] 写入最近提示词失败: %s", exc)

    async def _save_prompt_entry(
        self, owner_key: str, entry: dict[str, Any], description: str
    ) -> None:
        """把一条最近提示词加入用户的持久保存列表。"""
        saved_entry = dict(entry)
        saved_entry["description"] = str(description or "").strip()
        saved_entry["saved_at"] = time.time()
        async with self._prompt_library_lock:
            owner = self._prompt_library.setdefault(owner_key, {"recent": [], "saved": []})
            saved = owner.setdefault("saved", [])
            saved[:] = [
                item
                for item in saved
                if not (
                    item.get("workflow_id") == saved_entry.get("workflow_id")
                    and item.get("enhanced_prompt") == saved_entry.get("enhanced_prompt")
                )
            ]
            saved.insert(0, saved_entry)
            try:
                await asyncio.to_thread(self._write_prompt_library_file)
            except OSError as exc:  # pragma: no cover
                self.logger.warning("[提示词缓存] 写入保存提示词失败: %s", exc)

    # ── 余额 / 任务记录（Pages）───────────────────────────────

    def _task_history_path(self) -> Path:
        """最近任务记录文件路径（保存在 AstrBot 插件持久化目录）。"""
        directory = Path(get_astrbot_plugin_data_path()).resolve() / _PLUGIN_DIR.name
        directory.mkdir(parents=True, exist_ok=True)
        return directory / _TASK_HISTORY_FILE

    def _load_task_history(self) -> None:
        """从插件持久化目录读取最近任务记录（只保留三个业务字段）。"""
        path = self._task_history_path()
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return
        except Exception as exc:
            self.logger.warning("[任务记录] 读取历史文件失败: %s", exc)
            return
        if not isinstance(raw, list):
            raw = []
        records: list[dict[str, str]] = []
        seen: set[str] = set()
        for item in raw:
            if not isinstance(item, dict):
                continue
            task_id = str(item.get("task_id") or "").strip()
            if not task_id or task_id in seen:
                continue
            workflow = str(item.get("workflow") or "").strip()
            coins = str(item.get("coins") or "0").strip()
            seen.add(task_id)
            records.append({"task_id": task_id, "workflow": workflow, "coins": coins})
            if len(records) >= _TASK_HISTORY_MAX:
                break
        self._task_history = records
        self._task_history_recorded = seen

    def _write_task_history_file(self) -> None:
        """原子写入任务记录文件（调用方需持有 _task_history_lock）。"""
        path = self._task_history_path()
        temp = path.with_suffix(path.suffix + ".tmp")
        temp.write_text(
            json.dumps(self._task_history, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp.replace(path)

    async def _record_task_history(self, task_id: str, workflow: str, coins: Any) -> None:
        """追加一条任务记录（最新在前，按 task_id 去重，最多 200 条）。"""
        task_id = str(task_id or "").strip()
        if not task_id:
            return
        workflow = str(workflow or "").strip()
        coins = str(coins if coins is not None else "0").strip()
        async with self._task_history_lock:
            if task_id in self._task_history_recorded:
                return
            self._task_history_recorded.add(task_id)
            self._task_history.insert(0, {"task_id": task_id, "workflow": workflow, "coins": coins})
            del self._task_history[_TASK_HISTORY_MAX:]
            if len(self._task_history_recorded) > _TASK_HISTORY_MAX * 2:
                self._task_history_recorded = {
                    str(item.get("task_id") or "") for item in self._task_history
                }
            try:
                await asyncio.to_thread(self._write_task_history_file)
            except Exception as exc:  # pragma: no cover
                self.logger.warning("[任务记录] 写入历史文件失败: %s", exc)

    @staticmethod
    def _consume_coins_from_result(result: Any) -> str:
        """从 RunningHub 任务查询响应里提取消耗的 RH 币。"""
        if isinstance(result, dict):
            usage = result.get("usage")
            if isinstance(usage, dict):
                coins = usage.get("consumeCoins") or usage.get("consume_coins")
                if coins is not None:
                    return str(coins).strip()
            coins = result.get("consumeCoins")
            if coins is not None:
                return str(coins).strip()
        return "0"

    async def handle_page_get_account(self):
        """可视化页面 API：查询两个区域的 RunningHub 账户余额。"""
        regions: list[dict[str, Any]] = []
        for region, key_attr, label in (
            ("overseas", "api_key", "国外 runninghub.ai"),
            ("domestic", "api_key_cn", "国内 runninghub.cn"),
        ):
            entry: dict[str, Any] = {"region": region, "label": label, "status": "ok", "message": ""}
            api_key = str(getattr(self.config.server, key_attr) or "")
            if not api_key:
                entry.update({"status": "missing", "message": "未配置 API Key", "account": None})
                regions.append(entry)
                continue
            client = self._get_client(region)
            if client is None:
                self._rebuild_client()
                client = self._get_client(region)
            if client is None:
                entry.update({"status": "error", "message": "客户端初始化失败", "account": None})
                regions.append(entry)
                continue
            try:
                account = await asyncio.wait_for(client.account_status(), timeout=20)
            except Exception as exc:
                entry.update({"status": "error", "message": str(exc), "account": None})
            else:
                entry["account"] = {
                    "remain_coins": str(account.get("remainCoins") or "0"),
                    "current_task_counts": str(account.get("currentTaskCounts") or "0"),
                    "remain_money": str(account.get("remainMoney") or ""),
                    "currency": str(account.get("currency") or ""),
                    "api_type": str(account.get("apiType") or ""),
                }
            regions.append(entry)
        return self._web_jsonify({"success": True, "data": {"regions": regions}})

    async def handle_page_get_tasks(self):
        """可视化页面 API：读取最近任务记录（最新在前）。"""
        records = [dict(item) for item in self._task_history]
        return self._web_jsonify({"success": True, "data": {"records": records}})

    async def handle_page_clear_tasks(self):
        """可视化页面 API：清空最近任务记录。"""
        async with self._task_history_lock:
            self._task_history.clear()
            self._task_history_recorded.clear()
            try:
                await asyncio.to_thread(self._write_task_history_file)
            except Exception as exc:  # pragma: no cover
                self.logger.warning("[任务记录] 清空历史文件失败: %s", exc)
                return self._web_jsonify({"success": False, "message": f"清空失败: {exc}"})
        return self._web_jsonify({"success": True, "message": "任务记录已清空"})


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
                        "required": node.required,
                        "param_type": node.param_type,
                        "minimum": node.minimum,
                        "maximum": node.maximum,
                        "choices": node.choices,
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
                    "description": workflow.description,
                    "llm_enabled": workflow.llm_enabled,
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
                        "required": bool(node_raw.get("required", False)),
                        "param_type": str(node_raw.get("param_type") or "string"),
                        "minimum": node_raw.get("minimum") if node_raw.get("minimum") != "" else None,
                        "maximum": node_raw.get("maximum") if node_raw.get("maximum") != "" else None,
                        "choices": node_raw.get("choices") or [],
                    }
                )
                try:
                    node_model = InputNodeSection.model_validate(nodes[-1])
                    node_error = _validation_lib.validate_node(node_model)
                    if node_error:
                        return [], f"工作流「{name}」第 {node_index} 项「{label or node_id}」({node_id}/{field_name})：{node_error}"
                except ValueError as exc:
                    return [], f"工作流「{name}」节点 {node_id}/{field_name} 参数约束无效：{exc}"
            if len(str(raw.get("description") or "")) > 1000:
                return [], f"工作流「{name}」用途说明不能超过 1000 字符"
            items.append(
                WorkflowItemSection.model_validate(
                    {
                        "name": name,
                        "workflow_id": workflow_id,
                        "instance_type": instance_type,
                        "region": region,
                        "llm_enhance": llm_enhance,
                        "llm_template_path": llm_template_path,
                        "description": str(raw.get("description") or ""),
                        "llm_enabled": bool(raw.get("llm_enabled", True)),
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
        """用户扩写模板目录（AstrBot 持久化目录，插件更新后不会丢失）。

        插件目录里的 prompt/ 只作为内置种子模板；首次使用时会复制到
        ``data/plugin_data/<插件目录>/prompt/``，之后页面上下传的模板都
        保存在这里，更新插件不会被覆盖。
        """
        directory = (
            Path(get_astrbot_plugin_data_path()).resolve()
            / _PLUGIN_DIR.name
            / "prompt"
        )
        directory.mkdir(parents=True, exist_ok=True)
        bundled = _PLUGIN_DIR / "prompt"
        if bundled.is_dir():
            for seed in bundled.iterdir():
                if seed.is_file() and seed.name.lower().endswith((".txt", ".md")):
                    target = directory / seed.name
                    if not target.exists():
                        try:
                            target.write_bytes(seed.read_bytes())
                        except OSError:
                            pass
        return directory

    def _safe_prompt_template(self, name: str) -> Path | None:
        """把模板名解析为 prompt/ 目录内的安全路径（仅允许 .txt / .md）。

        兼容两种写法：
        - ``anima3_prompt_template.txt``（纯文件名）
        - ``prompt/anima3_prompt_template.txt``（页面下拉里使用的相对路径）
        """
        raw = str(name or "").strip().replace("\\", "/")
        if raw.startswith("prompt/"):
            raw = raw[len("prompt/"):]
        if (
            not raw
            or raw in {".", ".."}
            or raw.startswith((".", "/"))
            or "/" in raw
            or "\\" in raw
        ):
            return None
        if not raw.lower().endswith((".txt", ".md")):
            return None
        target = (self._prompt_templates_dir() / raw).resolve()
        try:
            target.relative_to(self._prompt_templates_dir().resolve())
        except ValueError:
            return None
        return target


    def _resolve_prompt_template(self, name: str) -> Path | None:
        """读取模板时按「用户持久化目录 → 插件内置目录」顺序解析。"""
        raw = str(name or "").strip().replace("\\", "/")
        if raw.startswith("prompt/"):
            raw = raw[len("prompt/"):]
        if (
            not raw
            or raw in {".", ".."}
            or raw.startswith((".", "/"))
            or "/" in raw
            or "\\" in raw
        ) or not raw.lower().endswith((".txt", ".md")):
            return None
        user_target = (self._prompt_templates_dir() / raw).resolve()
        if user_target.is_file():
            return user_target
        bundled_target = (_PLUGIN_DIR / "prompt" / raw).resolve()
        if bundled_target.is_file():
            return bundled_target
        return user_target


    def _list_prompt_templates(self) -> list[dict[str, Any]]:
        """列出所有可用模板：持久化目录优先，插件内置目录兜底合并。"""
        templates: dict[str, dict[str, Any]] = {}
        directory = self._prompt_templates_dir()
        bundled = _PLUGIN_DIR / "prompt"
        roots = [directory]
        if bundled.is_dir() and bundled.resolve() != directory.resolve():
            roots.append(bundled)
        for root in roots:
            try:
                entries = list(root.iterdir())
            except OSError:
                continue
            for path in entries:
                if not path.is_file() or not path.name.lower().endswith((".txt", ".md")):
                    continue
                if path.name in templates:
                    continue
                try:
                    stat = path.stat()
                except OSError:
                    continue
                templates[path.name] = {
                    "name": path.name,
                    "path": f"prompt/{path.name}",
                    "size": stat.st_size,
                    "modified": int(stat.st_mtime),
                }
        return sorted(templates.values(), key=lambda item: str(item["name"]).lower())

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
        target = self._resolve_prompt_template(web_request.args.get("name", ""))
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

