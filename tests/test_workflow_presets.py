"""Only the known, editable preset structures are applied to AstrBot config."""
from __future__ import annotations

import copy
from pathlib import Path

import pytest

from rh_generic_lib.config import WorkflowItemSection
from rh_generic_lib.validation import workflow_errors
from rh_generic_lib.workflow_detect import validate_detected_prompt_nodes
from rh_generic_lib.workflow_presets import match_workflow_preset


def qwen_graph():
    graph = {
        "416": {"class_type": "KSampler", "inputs": {"model": ["1", 0]}},
        "418": {"class_type": "TextEncodeQwenImage21", "inputs": {"clip": ["2", 0]}},
        "424": {"class_type": "ResolutionSelector", "inputs": {"aspect_ratio": "1:1 (Square)"}},
        "494": {"class_type": "CR Prompt Text", "inputs": {"prompt": "old prompt"}},
    }
    for node_id in ("420", "432", "433", "436", "435", "434"):
        graph[node_id] = {"class_type": "LoadImage", "inputs": {"image": "old.png"}}
    return graph


def anima_graph():
    return {
        "348": {"class_type": "UNETLoader", "inputs": {"unet_name": "model.safetensors"}},
        "353": {"class_type": "CR Prompt Text", "inputs": {"prompt": "old prompt"}},
        "357": {"class_type": "CLIPTextEncode", "inputs": {"text": ["353", 0]}},
        "358": {"class_type": "CLIPTextEncode", "inputs": {"text": "negative"}},
        "362": {"class_type": "KSampler", "inputs": {"model": ["348", 0]}},
        "385": {"class_type": "CR Prompt Text", "inputs": {"prompt": "style"}},
        "387": {"class_type": "JoinStringMulti", "inputs": {"string_1": ["353", 0]}},
    }


@pytest.mark.parametrize("make_graph", [qwen_graph, anima_graph])
def test_known_presets_produce_valid_astrbot_workflows_without_changing_graph(make_graph):
    graph = make_graph()
    original = copy.deepcopy(graph)
    preset = match_workflow_preset(graph)
    assert preset is not None
    workflow = WorkflowItemSection(name="用户另存的工作流", workflow_id="different-id", region="domestic",
                                   input_nodes=preset["input_nodes"], **preset["settings"])
    assert workflow.workflow_id == "different-id" and workflow.region == "domestic"
    assert workflow.output_type == "image" and workflow.llm_enhance
    assert not workflow_errors(workflow)
    assert (Path(__file__).resolve().parents[1] / workflow.llm_template_path).is_file()
    assert graph == original
    validate_detected_prompt_nodes(preset["input_nodes"])


def test_qwen_preset_retains_six_optional_reference_slots_and_editable_ratio():
    preset = match_workflow_preset(qwen_graph())
    images = [n for n in preset["input_nodes"] if n["value_type"] == "image"]
    assert [n["node_id"] for n in images] == ["420", "432", "433", "436", "435", "434"]
    assert all(n["field_value"] == "" and n["required"] is False for n in images)
    ratio = next(n for n in preset["input_nodes"] if n["field_name"] == "aspect_ratio")
    assert ratio["value_type"] == "text" and ratio["param_type"] == "string"
    assert len(ratio["choices"]) == 5


@pytest.mark.parametrize("mutation", ["missing", "wrong_class", "linked", "object"])
def test_presets_do_not_match_modified_or_uneditable_structures(mutation):
    graph = qwen_graph()
    if mutation == "missing":
        graph.pop("432")
    elif mutation == "wrong_class":
        graph["432"]["class_type"] = "OtherLoadImage"
    elif mutation == "linked":
        graph["494"]["inputs"]["prompt"] = ["1", 0]
    else:
        graph["424"]["inputs"]["aspect_ratio"] = {"unexpected": "value"}
    assert match_workflow_preset(graph) is None


def test_preset_results_are_detached_and_arbitrary_node_ids_are_not_guessed():
    first = match_workflow_preset(qwen_graph())
    first["input_nodes"][0]["label"] = "changed"
    first["settings"]["llm_enhance"] = False
    second = match_workflow_preset(qwen_graph())
    assert second["input_nodes"][0]["label"] == "参考图1"
    assert second["settings"]["llm_enhance"] is True
    graph = qwen_graph()
    graph["renamed"] = graph.pop("494")
    assert match_workflow_preset(graph) is None


def test_multiple_primary_prompts_have_an_explicit_actionable_error():
    nodes = [{"node_id": "1", "value_type": "prompt"}, {"node_id": "2", "value_type": "prompt"}]
    with pytest.raises(ValueError, match="识别出多个主提示词节点.*重新识别"):
        validate_detected_prompt_nodes(nodes)
    validate_detected_prompt_nodes([{"value_type": "prompt"}, {"value_type": "default"}])
