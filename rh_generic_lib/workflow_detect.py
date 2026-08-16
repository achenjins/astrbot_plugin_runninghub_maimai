"""RunningHub 工作流节点识别（无框架依赖的纯函数）。"""

from __future__ import annotations

import json
import re
from typing import Any

LLM_DETECT_PROMPT = """你是 ComfyUI/RunningHub 工作流配置分析器。下面是一个工作流的节点清单（"节点 ID（class_type）标题" + 各字段：字段名: 值/连线，<连线> 表示该字段来自其他节点输出，不可编辑）。

请判断哪些字段是【用户输入节点】、哪些是【推荐预设的配置节点】，只输出一个 JSON 对象，不要输出任何解释、代码块围栏或多余文本。

输出格式（严格遵守）：
{{"nodes":[{{"node_id":"6","field_name":"text","value_type":"prompt","field_value":"","label":"提示词"}},{{"node_id":"5","field_name":"width","value_type":"text","field_value":"512","label":"宽度"}}]}}

判定规则：
1. node_id 与 field_name 必须真实存在于上面清单中，禁止编造；<连线> 字段不可编辑，一律不得输出。
2. 输入节点（终端用户需要提供）：
   - 文字类（提示词/描述文本，主提示词）→ value_type="prompt"（整个工作流最多 1 个）
   - 图片类（参考图/LoadImage 等）→ value_type="image"
   - 音频类（参考音频/配音）→ value_type="audio"
   - 视频类（参考视频/LoadVideo 等）→ value_type="video"
   输入节点的 field_value 一律留空 ""。
3. 配置节点（值得预设的常见参数：分辨率/宽高、画面比例、步数、采样器、CFG、种子、批次、lora 强度等）→ value_type="text"，field_value 填当前值（字符串形式），label 用简短中文。
   重要：即使节点带有连线输入，它的【标量参数】也必须作为配置节点列出，例如：
   - KSampler：steps / cfg / sampler_name / seed / denoise
   - EmptyLatentImage：width / height / batch_size
   - 分辨率、画面比例（aspect ratio）、lora 强度、controlnet 强度等任何对出图效果有意义的标量参数
4. 不要输出：CheckpointLoader、VAE、SaveImage、Upscale 等纯内部/保存类节点，也不要输出任何 <连线> 字段。
5. 输入节点最多 8 个，配置节点最多 8 个，二者独立计数、互不影响；没有的类别可以少列或不列。
6. label 一律用简短中文。

工作流节点清单：
{workflow}
"""

LLM_DETECT_KEY_PROMPT = """你是 ComfyUI/RunningHub 工作流配置分析器。下面是一个工作流的节点清单（"节点 ID（class_type）标题" + 各字段：字段名: 值/连线，<连线> 表示该字段来自其他节点输出，不可编辑）。

请只识别下面这几类【关键节点】，其余节点（步数、采样器、CFG、种子、lora 强度等）一律不要输出。只输出一个 JSON 对象，不要输出任何解释、代码块围栏或多余文本。

输出格式（严格遵守）：
{{"nodes":[{{"node_id":"6","field_name":"text","value_type":"prompt","field_value":"","label":"提示词"}},{{"node_id":"5","field_name":"width","value_type":"default","field_value":"512","label":"宽度"}}]}}

判定规则：
1. node_id 与 field_name 必须真实存在于上面清单中，禁止编造；<连线> 字段不可编辑，一律不得输出。
2. 输入节点（终端用户需要提供）：
   - 文字类（提示词/描述文本，主提示词）→ value_type="prompt"（整个工作流最多 1 个）
   - 图片类（参考图/LoadImage 等）→ value_type="image"
   - 音频类（参考音频/配音）→ value_type="audio"
   - 视频类（参考视频/LoadVideo 等）→ value_type="video"
   输入节点的 field_value 一律留空 ""。
3. 预设配置节点（仅这两类）：
   - 分辨率（width / height / resolution）→ value_type="default"，field_value 填当前值
   - 长宽比例（aspect ratio / ratio / 比例 / 画幅 / 宽高比）→ value_type="default"，field_value 填当前值
4. 除上述 6 类（prompt / image / audio / video / 分辨率 / 长宽比例）外，其余节点一律不要输出。
5. label 一律用简短中文。

工作流节点清单：
{workflow}
"""



def safe_int(value: Any) -> int:
    """容错 int 转换。"""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def detect_input_nodes(workflow_json: dict[str, Any]) -> list[dict[str, str]]:
    """从工作流 JSON 自动识别可能的输入节点（启发式，作为 LLM 识别的兜底）。

    规则：class_type 命中白名单。普通节点要求所有 inputs 均为标量值
    （非节点连线）；CLIPTextEncode 例外——它通常带有 clip 等连线输入，
    但只要 text 字段是标量，就是典型文字输入节点。
    返回节点 dict：node_id / field_name / value_type / field_value / label。
    """
    detected: list[dict[str, str]] = []
    for node_id, node in sorted(workflow_json.items(), key=lambda item: safe_int(item[0])):
        if not isinstance(node, dict):
            continue
        class_type = str(node.get("class_type") or "")
        inputs = node.get("inputs")
        if not isinstance(inputs, dict) or not inputs:
            continue
        cls_lower = class_type.lower()
        cls_compact = cls_lower.replace(" ", "")
        has_links = any(isinstance(v, (list, tuple)) and v for v in inputs.values())

        if "cliptextencode" in cls_compact or "text encode" in cls_lower:
            # CLIPTextEncode：text 为标量即视为文字输入（连线输入不影响判定）
            if isinstance(inputs.get("text"), str):
                detected.append(
                    {
                        "node_id": node_id,
                        "field_name": "text",
                        "value_type": "prompt",
                        "field_value": "",
                        "label": f"文本输入（{class_type}）",
                    }
                )
            continue
        # 其余节点：存在节点连线（["id", 0] 形式）则排除
        if has_links:
            continue
        if "prompt text" in cls_lower or "primitivestring" in cls_compact or "stringmultiline" in cls_compact:
            field_name = (
                "prompt"
                if "prompt text" in cls_lower
                else ("text" if "text" in inputs else ("value" if "value" in inputs else "prompt"))
            )
            detected.append(
                {
                    "node_id": node_id,
                    "field_name": field_name,
                    "value_type": "prompt",
                    "field_value": "",
                    "label": f"文本输入（{class_type}）",
                }
            )
        elif "loadimage" in cls_compact or "load image" in cls_lower:
            detected.append(
                {
                    "node_id": node_id,
                    "field_name": "image",
                    "value_type": "image",
                    "field_value": "",
                    "label": f"图片输入（{class_type}）",
                }
            )
        elif "loadaudio" in cls_compact or "audio upload" in cls_lower or "audioupload" in cls_compact:
            detected.append(
                {
                    "node_id": node_id,
                    "field_name": "audio",
                    "value_type": "audio",
                    "field_value": "",
                    "label": f"语音输入（{class_type}）",
                }
            )
        elif "loadvideo" in cls_compact or "video upload" in cls_lower or "videoupload" in cls_compact or "loadclip" in cls_compact:
            detected.append(
                {
                    "node_id": node_id,
                    "field_name": "video",
                    "value_type": "video",
                    "field_value": "",
                    "label": f"视频输入（{class_type}）",
                }
            )
    return detected

def detect_key_nodes(workflow_json: dict[str, Any]) -> list[dict[str, str]]:
    """简化识别：仅提取文字/图片/音频/视频/分辨率/长宽比例等关键节点。

    文字/图片/音频/视频复用启发式识别；分辨率（width/height/resolution）与
    长宽比例（aspect/ratio/比例/画幅/宽高比）作为 default 类型；其余一律忽略。
    """
    detected = detect_input_nodes(workflow_json)
    _RES_KEYWORDS = ("width", "height", "resolution", "分辨率")
    _ASPECT_KEYWORDS = ("aspect", "ratio", "比例", "画幅", "宽高比")
    for node_id, node in sorted(workflow_json.items(), key=lambda item: safe_int(item[0])):
        if not isinstance(node, dict):
            continue
        inputs = node.get("inputs")
        if not isinstance(inputs, dict) or not inputs:
            continue
        # 跳过存在节点连线的内部节点
        if any(isinstance(v, (list, tuple)) and v for v in inputs.values()):
            continue
        for field_name, value in inputs.items():
            if isinstance(value, (list, tuple, dict)):
                continue
            fn = field_name.lower()
            if any(k in fn for k in _RES_KEYWORDS):
                label = {"width": "宽度", "height": "高度"}.get(fn, "分辨率")
                detected.append(
                    {
                        "node_id": node_id,
                        "field_name": field_name,
                        "value_type": "default",
                        "field_value": str(value),
                        "label": label,
                    }
                )
            elif any(k in fn for k in _ASPECT_KEYWORDS):
                detected.append(
                    {
                        "node_id": node_id,
                        "field_name": field_name,
                        "value_type": "default",
                        "field_value": str(value),
                        "label": "长宽比例",
                    }
                )
    return detected

def describe_workflow_for_llm(workflow_json: dict[str, Any]) -> str:
    """将工作流节点压缩为供 LLM 判断的清单（含字段名、是否连线、当前值）。"""
    lines: list[str] = []
    for node_id, node in sorted(workflow_json.items(), key=lambda item: safe_int(item[0])):
        if not isinstance(node, dict):
            continue
        class_type = str(node.get("class_type") or "")
        meta = node.get("_meta")
        title = str(meta.get("title") or "") if isinstance(meta, dict) else ""
        header = f"节点 {node_id}（{class_type}）"
        if title and title != class_type:
            header += f" 标题={title}"
        lines.append(header)
        inputs = node.get("inputs")
        if isinstance(inputs, dict):
            for field_name, value in inputs.items():
                if isinstance(value, (list, tuple)):
                    lines.append(f"    {field_name}: <连线>")
                elif isinstance(value, dict):
                    lines.append(f"    {field_name}: <对象: {','.join(str(k) for k in value)}>")
                else:
                    sample = str(value)
                    if len(sample) > 80:
                        sample = sample[:80] + "…"
                    lines.append(f"    {field_name}: {sample!r}")
        lines.append("")
    return "\n".join(lines)

def parse_llm_nodes(response_text: str, workflow_json: dict[str, Any]) -> list[dict[str, str]] | None:
    """解析并校验 LLM 输出为节点列表（node_id/field_name 必须真实存在）。

    Returns:
        list | None: 校验通过的节点列表；解析失败或无有效节点返回 None。
    """
    text = str(response_text or "").strip()
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.MULTILINE)
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end <= start:
        return None
    try:
        data = json.loads(text[start : end + 1])
    except Exception:
        return None
    raw_nodes = data.get("nodes")
    if not isinstance(raw_nodes, list):
        return None

    result: list[dict[str, str]] = []
    for item in raw_nodes:
        if not isinstance(item, dict):
            continue
        node_id = str(item.get("node_id") or "").strip()
        field_name = str(item.get("field_name") or "").strip()
        value_type = str(item.get("value_type") or "").strip().lower()
        label = str(item.get("label") or "").strip()
        if value_type not in ("prompt", "text", "image", "audio", "video", "default"):
            continue
        node = workflow_json.get(node_id)
        if not isinstance(node, dict):
            continue
        inputs = node.get("inputs")
        if not isinstance(inputs, dict) or field_name not in inputs:
            continue
        # 连线字段（来自其他节点输出）不可编辑，一律排除
        if isinstance(inputs.get(field_name), (list, tuple)):
            continue
        field_value = ""
        if value_type in ("text", "default"):
            field_value = str(item.get("field_value") or "").strip()
            current = inputs.get(field_name)
            if not field_value and not isinstance(current, (list, tuple, dict)):
                field_value = str(current)
        result.append(
            {
                "node_id": node_id,
                "field_name": field_name,
                "value_type": value_type,
                "field_value": field_value,
                "label": label or field_name,
            }
        )
    return result or None

async def _detect_input_nodes_with_llm(
    self,
    workflow_json: dict[str, Any],
    *,
    prompt_template: str | None = None,
) -> list[dict[str, str]] | None:
    """用内置 LLM 识别节点（失败返回 None，由调用方回退启发式）。

    prompt_template 传入时使用该提示词模板（如关键节点专用模板）。
    """
    workflow_desc = self._describe_workflow_for_llm(workflow_json)
    template = prompt_template or LLM_DETECT_PROMPT
    prompt = template.format(workflow=workflow_desc)
    try:
        result = await self.ctx.llm.generate(
            prompt=prompt,
            model=self.config.feature.model,
            temperature=0.2,
            max_tokens=1500,
        )
    except Exception as exc:
        self.ctx.logger.warning("[识别] LLM 识别调用异常，回退启发式: %s", exc, exc_info=True)
        return None
    if not isinstance(result, dict) or not result.get("success"):
        self.ctx.logger.warning("[识别] LLM 识别未成功，回退启发式: %s", str(result)[:300])
        return None
    raw_response = str(result.get("response") or result.get("content") or "")
    nodes = self._parse_llm_nodes(raw_response, workflow_json)
    if not nodes:
        self.ctx.logger.warning(
            "[识别] LLM 输出解析/校验失败，回退启发式；原始响应: %s", raw_response[:500]
        )
        return None
    self.ctx.logger.info(
        "[识别] LLM 识别出 %d 个节点: %s",
        len(nodes),
        ", ".join(f"{n['node_id']}/{n['field_name']}/{n['value_type']}" for n in nodes),
    )
    return nodes

