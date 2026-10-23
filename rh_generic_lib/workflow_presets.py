"""Map the shared workflow presets to AstrBot's configuration field names."""
from __future__ import annotations

from typing import Any

from .upstream.workflow_presets import match_workflow_preset as _match_shared_preset


def match_workflow_preset(workflow_json: dict[str, Any]) -> dict[str, Any] | None:
    """Return a detached, structurally matched preset ready for AstrBot settings."""
    preset = _match_shared_preset(workflow_json)
    if preset is None:
        return None
    for node in preset["input_nodes"]:
        node.setdefault("field_value", "")
        node.setdefault("required", False)
        node["param_type"] = node.pop("parameter_type", "string")
    # Both upstream presets are image workflows; this keeps result recognition
    # correct when the platform returns a result URL without a file extension.
    preset["settings"]["output_type"] = "image"
    return preset
