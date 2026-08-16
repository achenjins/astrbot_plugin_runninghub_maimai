"""AstrBot 插件配置模型与归一化。

从 maibot 插件的 TOML 配置模型迁移而来：
- pydantic 模型保持强类型校验；
- ``build_config_model`` 把 AstrBot 的 ``AstrBotConfig`` 字典归一化为强类型模型；
- ``dump_config_dict`` 把强类型模型转回 AstrBot 配置 JSON 结构。
"""

from __future__ import annotations

import json
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

NODE_FORM_SLOTS = 8

class PluginMetaSection(BaseModel):
    """插件配置版本信息（用于未来配置迁移，请勿删除）。"""

    __ui_label__ = "配置版本"

    config_version: str = Field(
        default="2.0.0",
        description="插件配置版本号（一般无需修改）",
        json_schema_extra={"label": "配置版本", "hidden": True},
    )


class ServerSection(BaseModel):
    """RunningHub 服务配置。"""

    __ui_label__ = "RunningHub 服务"

    base_url: str = Field(
        default="https://www.runninghub.ai",
        description="国外平台基地址（runninghub.ai）",
        json_schema_extra={"label": "国外基地址", "disabled": True},
    )
    api_key: str = Field(
        default="",
        description="国外 RunningHub API Key（在平台个人中心获取，务必保密）",
        json_schema_extra={"label": "国外 API Key", "placeholder": "粘贴你的 API Key", "x-widget": "password"},
    )
    base_url_cn: str = Field(
        default="https://www.runninghub.cn",
        description="国内平台基地址（runninghub.cn）",
        json_schema_extra={"label": "国内基地址", "disabled": True},
    )
    api_key_cn: str = Field(
        default="",
        description="国内 RunningHub API Key（可与国外只填一个；只填一个时拉取默认用该 key）",
        json_schema_extra={"label": "国内 API Key", "placeholder": "粘贴你的国内 API Key", "x-widget": "password"},
    )


class GenerationSection(BaseModel):
    """生成与轮询配置。"""

    __ui_label__ = "生成参数"

    poll_interval: int = Field(
        default=15, ge=3, description="任务轮询间隔（秒）", json_schema_extra={"label": "轮询间隔（秒）"}
    )
    max_wait: int = Field(
        default=1800, ge=60, description="任务最大等待时间（秒）", json_schema_extra={"label": "最大等待（秒）"}
    )
    max_concurrent: int = Field(
        default=2, ge=1, le=10, description="同时进行中的任务数上限", json_schema_extra={"label": "并发上限"}
    )
    download_timeout: int = Field(
        default=120, ge=30, description="下载图片超时（秒）", json_schema_extra={"label": "下载超时（秒）"}
    )


class FeatureSection(BaseModel):
    """可选功能设置（自动撤回 / 节点识别 / 提示词扩写）。"""

    __ui_label__ = "功能设置"

    enable: bool = Field(
        default=False,
        description="启用发送后自动撤回（仅在使用 NapCat 适配器时生效，其他平台无效）",
        json_schema_extra={"label": "启用自动撤回", "hint": "仅 NapCat 适配器生效"},
    )
    recall_seconds: int = Field(
        default=90,
        ge=0,
        description="图片发送后自动撤回的秒数（0 表示不撤回）",
        json_schema_extra={"label": "撤回延迟（秒）", "hint": "0 表示不撤回"},
    )
    result_notice: bool = Field(
        default=True,
        description="生成结果发出后，是否由插件追加一条「生成完成」确认消息（AstrBot 没有 Maisaka 主动回复能力，此处为迁移后的简化实现）",
        json_schema_extra={"label": "完成后发确认消息"},
    )
    use_llm: bool = Field(
        default=True,
        description="用内置 LLM 识别输入节点与配置节点（覆盖任意节点类型，比白名单更准）；失败时自动回退启发式规则",
        json_schema_extra={"label": "LLM 识别"},
    )
    model: str = Field(
        default="",
        description="节点识别使用的 AstrBot 模型提供商 ID；留空使用默认/当前会话模型",
        json_schema_extra={"label": "识别模型 ID"},
    )
    enhance_model: str = Field(
        default="",
        description="提示词扩写使用的 AstrBot 模型提供商 ID；留空使用默认/当前会话模型",
        json_schema_extra={"label": "扩写模型 ID"},
    )


class AccessSection(BaseModel):
    """访问控制与费用保护（默认全部放行，与旧版行为一致）。"""

    __ui_label__ = "访问控制"

    allow_users: list[str] = Field(
        default_factory=list,
        description="允许使用本插件的用户 ID 白名单；留空表示不限制任何用户",
        json_schema_extra={"label": "用户白名单", "placeholder": "用户ID，每行一个"},
    )
    allow_groups: list[str] = Field(
        default_factory=list,
        description="允许使用本插件的群组 ID 白名单；留空表示不限制群组（私聊不受群组白名单约束）",
        json_schema_extra={"label": "群组白名单", "placeholder": "群号，每行一个"},
    )
    max_per_user_per_hour: int = Field(
        default=0,
        ge=0,
        description="每个用户每小时最多触发的任务数（0 表示不限制）",
        json_schema_extra={"label": "每用户每小时上限", "hint": "0 表示不限制"},
    )
    admin_users: list[str] = Field(
        default_factory=list,
        description="管理员用户 ID 列表；管理员可用 /wf中断 中断所有人的任务",
        json_schema_extra={"label": "管理员 ID", "placeholder": "用户ID，每行一个"},
    )


class InputNodeSection(BaseModel):
    """单个工作流输入节点配置（可自由增加数量，最多 32 个）。

    类型下拉框选择该节点的用途：
    - prompt：主提示词，接收命令/LLM 扩写文本（整个工作流仅一个，多了报错）
    - text：可编辑配置（带默认值），上传文件后询问用户是否修改
    - default：固定默认值，直接使用不询问
    - image / audio：上传文件（留空则等待上传）
    - 自动推断：按字段名推断（含 image→图片、audio/voice→语音、其余→文字）
    """

    __ui_label__ = "输入节点"

    node_id: str = Field(
        default="",
        description="RunningHub 工作流中的节点 ID（如 353）",
        json_schema_extra={"label": "节点 ID", "placeholder": "353"},
    )
    field_name: str = Field(
        default="prompt",
        description="节点字段名，可自定义；自动识别时会自动填写（如 prompt / text / image / audio）",
        json_schema_extra={"label": "字段名", "placeholder": "prompt"},
    )
    field_value: str = Field(
        default="",
        description="输入内容。填写后作为固定默认值直接使用（不接受修改）；留空则按类型由用户提供",
        json_schema_extra={"label": "输入内容（默认值）", "hint": "留空=等待用户输入；填写=固定默认值"},
    )
    value_type: Literal["", "default", "text", "image", "audio", "video", "prompt"] = Field(
        default="",
        description="节点用途：prompt=主提示词（接收命令/扩写文本，仅一个）；text=可编辑配置（上传后询问修改）；default=固定默认值；image/audio/video=上传文件",
        json_schema_extra={
            "label": "节点类型",
            "x-widget": "select",
            "options": [
                {"value": "prompt", "label": "主提示词（接收命令/扩写文本）"},
                {"value": "text", "label": "可编辑配置（上传后询问修改）"},
                {"value": "default", "label": "默认值（固定使用输入内容）"},
                {"value": "image", "label": "图片（等待上传）"},
                {"value": "audio", "label": "语音（等待上传）"},
                {"value": "video", "label": "视频（等待上传）"},
            ],
        },
    )
    label: str = Field(
        default="",
        description="该输入的中文说明（等待上传时提示用户），留空使用节点 ID",
        json_schema_extra={"label": "输入说明", "placeholder": "角色参考图"},
    )

    @field_validator("value_type", mode="before")
    @classmethod
    def _normalize_value_type(cls, value: Any) -> Any:
        """WebUI 下拉的 SelectItem 不允许空字符串选项，用 "auto" 表示自动推断。"""
        if value == "auto":
            return ""
        return value


class WorkflowItemSection(BaseModel):
    """单个工作流配置（可自由增加数量）。"""

    __ui_label__ = "工作流"

    name: str = Field(
        default="",
        description="工作流显示名称，用于命令调用，如 /wf运行 动漫生图",
        json_schema_extra={"label": "工作流名称", "placeholder": "动漫生图"},
    )
    workflow_id: str = Field(
        default="",
        description="RunningHub 工作流 ID",
        json_schema_extra={"label": "工作流 ID", "placeholder": "2087492768787685378"},
    )
    instance_type: Literal["Standard", "Plus", "Ultra"] = Field(
        default="Standard",
        description="设备类型：Standard / Plus / Ultra",
        json_schema_extra={"label": "设备类型"},
    )
    region: Literal["overseas", "domestic"] = Field(
        default="overseas",
        description="区域：overseas=国外（runninghub.ai），domestic=国内（runninghub.cn）；决定用哪个 API 拉取与提交",
        json_schema_extra={"label": "区域", "hint": "overseas=国外 / domestic=国内"},
    )
    llm_enhance: bool = Field(
        default=False,
        description="开启后，命令文本先按模板扩写再传入文字节点",
        json_schema_extra={"label": "启用 LLM 扩写"},
    )
    llm_template_path: str = Field(
        default="",
        description="LLM 扩写提示词模板文件路径，使用相对路径（相对插件目录，如 templates/my_template.txt）",
        json_schema_extra={
            "label": "扩写模板路径（相对插件目录）",
            "placeholder": "templates/my_template.txt",
            "hint": "相对路径相对插件目录解析",
        },
    )
    input_nodes: list[InputNodeSection] = Field(
        default_factory=list,
        description="输入节点列表，按此顺序接收用户输入（最多 8 个）",
        json_schema_extra={"label": "输入节点"},
    )


class WorkflowsSection(BaseModel):
    """工作流列表配置（WebUI 中可自由增删工作流与输入节点）。"""

    __ui_label__ = "工作流列表"

    items: list[WorkflowItemSection] = Field(
        default_factory=list,
        description="工作流列表，可自由增删；每个工作流包含名称、工作流 ID、设备类型、LLM 扩写开关与输入节点",
        json_schema_extra={"label": "工作流列表", "min_items": 0, "max_items": 20},
    )


class GenericConfig(BaseModel):
    """插件完整配置。"""

    plugin: PluginMetaSection = Field(default_factory=PluginMetaSection)
    server: ServerSection = Field(default_factory=ServerSection)
    generation: GenerationSection = Field(default_factory=GenerationSection)
    feature: FeatureSection = Field(default_factory=FeatureSection)
    access: AccessSection = Field(default_factory=AccessSection)
    workflows: WorkflowsSection = Field(default_factory=WorkflowsSection)

    @model_validator(mode="before")
    @classmethod
    def _merge_legacy_feature_sections(cls, data: Any) -> Any:
        """兼容旧配置：把 cleanup / detect / llm 三节合并为 feature 一节。"""
        if not isinstance(data, dict):
            return data
        feature = dict(data.get("feature") or {})
        for old_key in ("cleanup", "detect", "llm"):
            old = data.get(old_key)
            if isinstance(old, dict):
                for key, value in old.items():
                    feature.setdefault(key, value)
        if feature:
            data = {key: value for key, value in data.items() if key not in ("cleanup", "detect", "llm")}
            data["feature"] = feature
        return data

    @field_validator("workflows", mode="before")
    @classmethod
    def _coerce_legacy_workflows(cls, value: Any) -> Any:
        """兼容最老版本配置：顶层 ``workflows = [ {...}, ... ]`` 数组形态。

        该形态会被归一化为 ``{"items": [...]}``，加载后由 on_load 的迁移逻辑
        落盘为新结构，避免旧配置直接导致激活失败。
        """
        if isinstance(value, list):
            return {
                "items": [
                    item.model_dump(mode="python") if isinstance(item, WorkflowItemSection) else item
                    for item in value
                ]
            }
        return value


def _node_from_raw(raw: dict[str, Any]) -> dict[str, str] | None:
    if not isinstance(raw, dict):
        return None
    node_id = str(raw.get("node_id") or "").strip()
    if not node_id:
        return None
    return {
        "node_id": node_id,
        "field_name": str(raw.get("field_name") or "prompt").strip() or "prompt",
        "field_value": str(raw.get("field_value") or ""),
        "value_type": str(raw.get("value_type") or "").strip().lower(),
        "label": str(raw.get("label") or "").strip(),
    }


def _parse_json_nodes(nodes_raw: Any) -> list[dict[str, Any]]:
    """兼容旧版 input_nodes 的 JSON 字符串 / list 两种形态。"""
    nodes: list[dict[str, Any]] = []
    if isinstance(nodes_raw, str):
        stripped = nodes_raw.strip()
        if stripped:
            try:
                parsed = json.loads(stripped)
            except Exception:
                parsed = []
            if isinstance(parsed, list):
                nodes = [n for n in parsed if isinstance(n, dict)]
    elif isinstance(nodes_raw, list):
        nodes = [n for n in nodes_raw if isinstance(n, dict)]
    return [n for n in (_node_from_raw(n) for n in nodes) if n is not None]


def _legacy_nodes_from_entry(entry: dict[str, Any]) -> list[dict[str, Any]]:
    """解析旧版配置中嵌入在工作流项里的输入节点。"""
    if "input_nodes" in entry:
        nodes = _parse_json_nodes(entry.get("input_nodes"))
    else:
        nodes = []
        for index in range(1, NODE_FORM_SLOTS + 1):
            slot = entry.get(f"input_node_{index}")
            node = _node_from_raw(slot) if isinstance(slot, dict) else None
            if node is not None:
                nodes.append(node)
        nodes.extend(_parse_json_nodes(entry.get("input_nodes_extra")))
    return nodes


def _workflow_dict_from_raw(entry: dict[str, Any]) -> dict[str, Any] | None:
    instance_type = str(entry.get("instance_type") or "Standard").strip()
    if instance_type not in ("Standard", "Plus", "Ultra"):
        instance_type = "Standard"
    region = str(entry.get("region") or "overseas").strip()
    if region not in ("overseas", "domestic"):
        region = "overseas"
    return {
        "name": str(entry.get("name") or "").strip(),
        "workflow_id": str(entry.get("workflow_id") or "").strip(),
        "instance_type": instance_type,
        "region": region,
        "llm_enhance": bool(entry.get("llm_enhance", False)),
        "llm_template_path": str(entry.get("llm_template_path") or "").strip(),
        "input_nodes": [],
    }


def build_workflow_items(
    raw_workflows: Any, raw_nodes: Any = None
) -> list[dict[str, Any]]:
    """把 AstrBot 配置归一化为 pydantic 可校验的工作流 dict 列表。

    主形态：
    - ``workflows``：template_list，每个工作流一项；
    - ``workflow_nodes``：template_list，每个输入节点一项，通过 workflow_name 关联。

    同时兼容旧版把输入节点嵌在工作流项里的配置。
    """
    if isinstance(raw_workflows, dict):
        raw_workflows = raw_workflows.get("items") or []
    if not isinstance(raw_workflows, list):
        return []

    items: list[dict[str, Any]] = []
    embedded_nodes: dict[str, list[dict[str, Any]]] = {}
    for entry in raw_workflows:
        if not isinstance(entry, dict):
            continue
        workflow = _workflow_dict_from_raw(entry)
        name = workflow["name"]
        items.append(workflow)
        embedded_nodes[name] = _legacy_nodes_from_entry(entry)

    if isinstance(raw_nodes, dict):
        raw_nodes = raw_nodes.get("items") or []
    has_embedded_nodes = any(
        "input_nodes" in entry
        or "input_nodes_extra" in entry
        or any(f"input_node_{index}" in entry for index in range(1, NODE_FORM_SLOTS + 1))
        for entry in (raw_workflows if isinstance(raw_workflows, list) else [])
        if isinstance(entry, dict)
    )
    if raw_nodes is None or (not raw_nodes and has_embedded_nodes):
        # 旧版配置：输入节点嵌在工作流项里（AstrBot 会自动补默认 workflow_nodes=[]）
        for workflow in items:
            workflow["input_nodes"] = embedded_nodes.get(workflow["name"], [])
        return items

    # 新版：按 workflow_name 关联，保持 template_list 中的添加顺序
    nodes_by_workflow: dict[str, list[dict[str, Any]]] = {wf["name"]: [] for wf in items}
    if isinstance(raw_nodes, dict):
        raw_nodes = raw_nodes.get("items") or []
    if isinstance(raw_nodes, list):
        for raw in raw_nodes:
            if not isinstance(raw, dict):
                continue
            workflow_name = str(raw.get("workflow_name") or "").strip()
            node = _node_from_raw(raw)
            if not workflow_name or node is None:
                continue
            nodes_by_workflow.setdefault(workflow_name, []).append(node)

    for workflow in items:
        workflow["input_nodes"] = nodes_by_workflow.get(workflow["name"], [])
    return items


def dump_workflow_items(items: list[WorkflowItemSection]) -> list[dict[str, Any]]:
    """把强类型工作流列表转成 AstrBot workflows template_list 条目。"""
    return [
        {
            "__template_key": "workflow",
            "name": str(wf.name or ""),
            "workflow_id": str(wf.workflow_id or ""),
            "instance_type": str(wf.instance_type or "Standard"),
            "region": str(wf.region or "overseas"),
            "llm_enhance": bool(wf.llm_enhance),
            "llm_template_path": str(wf.llm_template_path or ""),
        }
        for wf in items
    ]


def dump_workflow_nodes(items: list[WorkflowItemSection]) -> list[dict[str, Any]]:
    """把强类型工作流的输入节点展开为 workflow_nodes template_list 条目。"""
    result: list[dict[str, Any]] = []
    for wf in items:
        for node in wf.input_nodes:
            result.append(
                {
                    "__template_key": "input_node",
                    "workflow_name": str(wf.name or ""),
                    "node_id": str(node.node_id or ""),
                    "field_name": str(node.field_name or "prompt"),
                    "field_value": str(node.field_value or ""),
                    "value_type": str(node.value_type or ""),
                    "label": str(node.label or ""),
                }
            )
    return result


def build_config_model(data: dict[str, Any]) -> GenericConfig:
    """把 AstrBotConfig 字典归一化为强类型 GenericConfig。"""
    data = data or {}
    server = data.get("server") if isinstance(data.get("server"), dict) else {}
    generation = data.get("generation") if isinstance(data.get("generation"), dict) else {}
    feature = data.get("feature") if isinstance(data.get("feature"), dict) else {}
    access = data.get("access") if isinstance(data.get("access"), dict) else {}
    plugin = data.get("plugin") if isinstance(data.get("plugin"), dict) else {}

    def _s(mapping: dict, key: str, default: Any) -> Any:
        val = mapping.get(key, default)
        return default if val is None else val

    normalized: dict[str, Any] = {
        "plugin": {
            "config_version": str(
                _s(plugin, "config_version", "")
                or _s(data, "config_version", "")
                or "2.0.0"
            )
        },
        "server": {
            "base_url": str(_s(server, "base_url", "https://www.runninghub.ai")),
            "api_key": str(_s(server, "api_key", "")),
            "base_url_cn": str(_s(server, "base_url_cn", "https://www.runninghub.cn")),
            "api_key_cn": str(_s(server, "api_key_cn", "")),
        },
        "generation": {
            "poll_interval": int(_s(generation, "poll_interval", 15)),
            "max_wait": int(_s(generation, "max_wait", 1800)),
            "max_concurrent": int(_s(generation, "max_concurrent", 2)),
            "download_timeout": int(_s(generation, "download_timeout", 120)),
        },
        "feature": {
            "enable": bool(_s(feature, "enable", False)),
            "recall_seconds": int(_s(feature, "recall_seconds", 90)),
            "result_notice": bool(_s(feature, "result_notice", True)),
            "use_llm": bool(_s(feature, "use_llm", True)),
            "model": str(_s(feature, "model", "") or _s(feature, "detect_provider_id", "") or ""),
            "enhance_model": str(
                _s(feature, "enhance_model", "")
                or _s(feature, "enhance_provider_id", "")
                or ""
            ),
        },
        "access": {
            "allow_users": [str(u) for u in (_s(access, "allow_users", []) or [])],
            "allow_groups": [str(g) for g in (_s(access, "allow_groups", []) or [])],
            "max_per_user_per_hour": int(_s(access, "max_per_user_per_hour", 0)),
            "admin_users": [str(u) for u in (_s(access, "admin_users", []) or [])],
        },
        "workflows": {"items": build_workflow_items(data.get("workflows"), data.get("workflow_nodes"))},
    }
    return GenericConfig.model_validate(normalized)


def dump_config_dict(config: GenericConfig) -> dict[str, Any]:
    """生成与 _conf_schema.json 对齐的完整配置字典。"""
    return {
        "config_version": str(config.plugin.config_version or "2.0.0"),
        "server": {
            "base_url": str(config.server.base_url or "https://www.runninghub.ai"),
            "api_key": str(config.server.api_key or ""),
            "base_url_cn": str(config.server.base_url_cn or "https://www.runninghub.cn"),
            "api_key_cn": str(config.server.api_key_cn or ""),
        },
        "generation": {
            "poll_interval": int(config.generation.poll_interval),
            "max_wait": int(config.generation.max_wait),
            "max_concurrent": int(config.generation.max_concurrent),
            "download_timeout": int(config.generation.download_timeout),
        },
        "feature": {
            "enable": bool(config.feature.enable),
            "recall_seconds": int(config.feature.recall_seconds),
            "result_notice": bool(config.feature.result_notice),
            "use_llm": bool(config.feature.use_llm),
            "model": str(config.feature.model or ""),
            "enhance_model": str(config.feature.enhance_model or ""),
        },
        "access": {
            "allow_users": [str(u) for u in config.access.allow_users],
            "allow_groups": [str(g) for g in config.access.allow_groups],
            "max_per_user_per_hour": int(config.access.max_per_user_per_hour),
            "admin_users": [str(u) for u in config.access.admin_users],
        },
        "workflows": dump_workflow_items(config.workflows.items),
            "workflow_nodes": dump_workflow_nodes(config.workflows.items),
    }
