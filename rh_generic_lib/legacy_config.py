"""maibot config.toml → AstrBot 插件配置迁移。

原插件使用 TOML 配置，AstrBot 使用 ``_conf_schema.json`` 生成的 JSON 配置。
本模块把旧 TOML 归一化为 AstrBot 配置字典，并在插件首次部署时自动导入
（见 ``RunningHubGenericPlugin._import_legacy_config_if_needed``）。

支持的旧结构：
1. ``[[workflows.items]]`` + ``[[workflows.items.input_nodes]]``（最新版）
2. 顶层 ``[[workflows]]`` 数组（最老版）
3. 顶层或 [workflows] 下的 workflows_toml 字符串（更早版本）

模型槽位映射：maibot 的 ``utils`` / ``replyer`` / ``planner`` 是宿主模型槽位，
AstrBot 没有对应概念，统一映射为空字符串（使用默认/当前会话模型）。
"""

from __future__ import annotations

import json
import tomllib
from pathlib import Path
from typing import Any



_NODE_KEYS = ("node_id", "field_name", "field_value", "value_type", "label")
_MAIBOT_MODEL_SLOTS = {"utils", "replyer", "planner"}


def _string(value: Any, default: str = "") -> str:
    return default if value is None else str(value)


def _bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    if isinstance(value, int):
        return bool(value)
    return default


def _int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalise_nodes(raw_nodes: Any) -> list[dict[str, str]]:
    nodes: list[dict[str, str]] = []
    if not isinstance(raw_nodes, list):
        return nodes
    for raw in raw_nodes:
        if not isinstance(raw, dict):
            continue
        nodes.append(
            {
                "node_id": _string(raw.get("node_id")),
                "field_name": _string(raw.get("field_name"), "prompt"),
                "field_value": _string(raw.get("field_value")),
                "value_type": _string(raw.get("value_type")),
                "label": _string(raw.get("label") or raw.get("hint")),
            }
        )
    return nodes


def _normalise_workflows(raw_workflows: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """把三类旧工作流结构统一成 AstrBot 配置的 workflows / workflow_nodes。"""
    workflows: list[dict[str, Any]] = []
    workflow_nodes: list[dict[str, Any]] = []
    if isinstance(raw_workflows, dict):
        items = raw_workflows.get("items")
        if not isinstance(items, list):
            items = []
        source: list[Any] = [dict(w) if isinstance(w, dict) else w for w in items]
    elif isinstance(raw_workflows, list):
        source = raw_workflows
    else:
        return workflows, workflow_nodes

    for raw in source:
        if not isinstance(raw, dict):
            continue
        instance_type = _string(raw.get("instance_type"), "Standard")
        if instance_type not in ("Standard", "Plus", "Ultra"):
            instance_type = "Standard"
        region = _string(raw.get("region"), "overseas")
        if region not in ("overseas", "domestic"):
            region = "overseas"
        name = _string(raw.get("name"))
        workflows.append(
            {
                "__template_key": "workflow",
                "name": name,
                "workflow_id": _string(raw.get("workflow_id")),
                "instance_type": instance_type,
                "region": region,
                "llm_enhance": _bool(raw.get("llm_enhance"), False),
                "llm_template_path": _string(raw.get("llm_template_path")),
            }
        )
        for node in _normalise_nodes(raw.get("input_nodes")):
            workflow_nodes.append(
                {
                    "__template_key": "input_node",
                    "workflow_name": name,
                    **node,
                }
            )
    return workflows, workflow_nodes


def _parse_nested_workflows_toml(text: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """解析 workflows_toml 字符串里的旧表结构。"""
    data = tomllib.loads(text)
    raw = data.get("workflows")
    if isinstance(raw, dict):
        raw = raw.get("items") or raw.get("workflows_toml")
    return _normalise_workflows(raw)


def parse_legacy_toml(path: str | Path) -> dict[str, Any]:
    """读取旧 config.toml，返回 AstrBot 配置结构。"""
    data = tomllib.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(data, dict):
        raise ValueError("config.toml 顶层结构异常")

    server = data.get("server") or {}
    generation = data.get("generation") or {}
    feature = dict(data.get("feature") or {})
    # 兼容最老版本：cleanup / detect / llm 三节合并为 feature
    for legacy_section in ("cleanup", "detect", "llm"):
        legacy = data.get(legacy_section)
        if isinstance(legacy, dict):
            for key, value in legacy.items():
                feature.setdefault(key, value)
    access = data.get("access") or {}

    # 模型槽位映射为空（使用 AstrBot 默认/当前会话模型）
    detect_model = _string(feature.get("model"))
    enhance_model = _string(feature.get("enhance_model"))
    if detect_model in _MAIBOT_MODEL_SLOTS:
        detect_model = ""
    if enhance_model in _MAIBOT_MODEL_SLOTS:
        enhance_model = ""

    workflows_raw = data.get("workflows")
    workflows: list[dict[str, Any]] = []
    workflow_nodes: list[dict[str, Any]] = []
    if isinstance(workflows_raw, dict):
        embedded = workflows_raw.get("workflows_toml")
        if isinstance(embedded, str) and embedded.strip():
            workflows, workflow_nodes = _parse_nested_workflows_toml(embedded)
        else:
            workflows, workflow_nodes = _normalise_workflows(workflows_raw)
    else:
        workflows, workflow_nodes = _normalise_workflows(workflows_raw)

    legacy_text = _string(data.get("workflows_toml"))
    if not workflows and legacy_text.strip():
        workflows, workflow_nodes = _parse_nested_workflows_toml(legacy_text)

    return {
        "config_version": "2.0.0",
        "server": {
            "base_url": _string(server.get("base_url"), "https://www.runninghub.ai"),
            "api_key": _string(server.get("api_key")),
            "base_url_cn": _string(server.get("base_url_cn"), "https://www.runninghub.cn"),
            "api_key_cn": _string(server.get("api_key_cn")),
        },
        "generation": {
            "poll_interval": _int(generation.get("poll_interval"), 15),
            "max_wait": _int(generation.get("max_wait"), 1800),
            "max_concurrent": _int(generation.get("max_concurrent"), 2),
            "download_timeout": _int(generation.get("download_timeout"), 120),
        },
        "feature": {
            "enable": _bool(feature.get("enable"), False),
            "recall_seconds": _int(feature.get("recall_seconds"), 90),
            "result_notice": True,
            "use_llm": _bool(feature.get("use_llm"), True),
            "model": detect_model,
            "enhance_model": enhance_model,
        },
        "access": {
            "allow_users": [str(u) for u in (access.get("allow_users") or [])],
            "allow_groups": [str(g) for g in (access.get("allow_groups") or [])],
            "max_per_user_per_hour": _int(access.get("max_per_user_per_hour"), 0),
            "admin_users": [str(u) for u in (access.get("admin_users") or [])],
        },
        "workflows": workflows,
        "workflow_nodes": workflow_nodes,
    }


def convert_toml_file(src: str | Path, dst: str | Path | None = None) -> dict[str, Any]:
    """把旧 config.toml 转换为 AstrBot 配置。

    Args:
        src: 旧 config.toml 路径。
        dst: 可选，目标 JSON 文件路径；提供时写出。

    Returns:
        转换后的 AstrBot 配置字典。
    """
    result = parse_legacy_toml(src)
    if dst is not None:
        Path(dst).write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    return result
