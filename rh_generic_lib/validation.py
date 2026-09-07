"""Shared, model-free validation for saved workflows and runtime parameters."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from types import SimpleNamespace
from typing import Any


def node_key(node: Any) -> str:
    return f"{node.node_id}/{node.field_name}"


def resolve_value_type(node: Any) -> str:
    explicit = str(node.value_type or "").strip().lower()
    if explicit in ("default", "text", "image", "audio", "video", "prompt"):
        return explicit
    name = str(node.field_name or "").lower()
    for kind, terms in (("image", ("image", "pic", "photo", "img")),
                        ("audio", ("audio", "voice", "sound", "music", "speech")),
                        ("video", ("video", "mp4", "mov", "webm", "clip"))):
        if any(term in name for term in terms):
            return kind
    return "text"


def parameter_value(node: Any, value: Any) -> str:
    """Validate a model-provided value against the administrator's schema."""
    label = node.label or node_key(node)
    if isinstance(value, (dict, list)) or value is None:
        raise ValueError(f"参数「{label}」需要单个值")
    text = str(value).strip()
    if len(text) > 8000:
        raise ValueError(f"参数「{label}」过长")
    if node.param_type in {"integer", "number"}:
        try:
            number = Decimal(text)
        except (InvalidOperation, ValueError):
            raise ValueError(f"参数「{label}」需要数字") from None
        if isinstance(value, bool) or not number.is_finite():
            raise ValueError(f"参数「{label}」需要有限数字")
        if abs(number.as_tuple().exponent) > 100 or (number and abs(number.adjusted()) > 100):
            raise ValueError(f"参数「{label}」数值过大或过小")
        if node.param_type == "integer":
            if number != number.to_integral_value():
                raise ValueError(f"参数「{label}」需要整数")
            text = str(int(number))
        else:
            text = format(number, "f")
        if node.minimum is not None and number < Decimal(str(node.minimum)):
            raise ValueError(f"参数「{label}」不能小于 {node.minimum:g}")
        if node.maximum is not None and number > Decimal(str(node.maximum)):
            raise ValueError(f"参数「{label}」不能大于 {node.maximum:g}")
    elif node.param_type == "boolean":
        if text.lower() not in {"true", "false", "1", "0"}:
            raise ValueError(f"参数「{label}」需要 true 或 false")
        text = "true" if text.lower() in {"true", "1"} else "false"
    if node.choices and text not in node.choices:
        raise ValueError(f"参数「{label}」可选：{'、'.join(node.choices)}")
    if node.required and not text:
        raise ValueError(f"参数「{label}」不能为空")
    return text



def validate_node(node: Any) -> str:
    """Return the first actionable configuration error, allowing missing inputs."""
    kind = resolve_value_type(node)
    if kind == "default" and node.required and not node.field_value.strip():
        return "固定默认值节点标为必填时必须填写默认值；需要用户提供时请改成对应输入类型"
    has_bounds = node.minimum is not None or node.maximum is not None
    if kind != "text":
        if has_bounds or node.choices or node.param_type != "string":
            return "参数类型、数值范围和允许值仅用于可编辑配置，请清除这些约束或修改节点类型"
        return ""
    if has_bounds and node.param_type not in {"integer", "number"}:
        return "只有整数或数值参数可以设置数值范围"
    if node.minimum is not None and node.maximum is not None and node.minimum > node.maximum:
        return "数值下限不能大于上限"
    try:
        if node.choices:
            bare = SimpleNamespace(**{k: getattr(node, k) for k in
                                      ("node_id", "field_name", "label", "param_type", "minimum", "maximum", "required")}, choices=[])
            for choice in node.choices:
                normalized = parameter_value(bare, choice)
                if normalized != choice:
                    return f"允许值「{choice}」请写成「{normalized}」"
        if node.field_value.strip():
            parameter_value(node, node.field_value)
    except ValueError as exc:
        return str(exc)
    return ""


def workflow_errors(workflow: Any) -> list[dict[str, Any]]:
    errors = []
    seen = set()
    prompt_count = 0
    for index, node in enumerate(workflow.input_nodes):
        key = node_key(node)
        message = ""
        if not node.node_id.strip():
            message = "节点 ID 不能为空"
        elif key in seen:
            message = "节点 ID 和字段名重复，请删除或修改重复项"
        elif resolve_value_type(node) == "prompt":
            prompt_count += 1
            if prompt_count > 1:
                message = "最多只能有一个主提示词节点"
        seen.add(key)
        message = message or validate_node(node)
        if message:
            errors.append({"node_index": index, "key": key,
                           "message": f"工作流「{workflow.name}」第 {index + 1} 项「{node.label or key}」({key})：{message}"})
    return errors
