"""Host-specific settings and preset registration survive the shared-core migration."""
import asyncio

from rh_generic_lib.config import build_config_model, dump_config_dict
from rh_generic_lib.upstream.workflow_presets import WORKFLOW_PRESETS
from rh_generic_lib.workflow_presets import match_workflow_preset
from rh_generic_lib.validation import workflow_errors
from .test_chat_workflows import event, plugin, finish_jobs  # noqa: F401


def test_new_settings_roundtrip_keeps_native_provider_and_explicit_disabled_values():
    raw = {"generation": {"auto_resume_tracking": False, "delivery_retries": 0, "max_file_mb": 32},
           "feature": {"recent_images": 0, "image_memory_hours": 48, "vision_model": "native-provider-id"},
           "access": {"allow_plugin_api": True, "natural_language_ignore_user_allowlist": True},
           "workflows": [{"name": "edit", "workflow_id": "1", "capability": "multi_reference",
                          "cost_hint": "由平台计费", "prompt_profile": "edit", "input_nodes": [
                              {"node_id": "2", "field_name": "image", "value_type": "image",
                               "input_key": "subject", "role": "人物主体"}]}]}
    config = build_config_model(raw)
    reloaded = build_config_model(dump_config_dict(config))
    assert reloaded.model_dump() == config.model_dump()
    assert not reloaded.generation.auto_resume_tracking and reloaded.generation.delivery_retries == 0
    assert reloaded.feature.vision_model == "native-provider-id"
    assert reloaded.workflows.items[0].input_nodes[0].role == "人物主体"


def test_api_generation_is_opt_in_without_disabling_existing_page_management(plugin, monkeypatch):
    async def run():
        request = event()
        context = plugin._event_ctx(request)
        denied = await plugin._start_workflow("改图", "水彩", **context, trigger="api")
        assert not denied["success"] and "API" in denied["message"]
        assert not plugin._client.submissions
        monkeypatch.setattr(plugin, "_web_jsonify", lambda data: data)
        assert (await plugin.handle_page_get_tasks())["success"]
        plugin.config.access.allow_plugin_api = True
        workflow = plugin._workflows[0]
        workflow.input_nodes = workflow.input_nodes[:1]
        accepted = await plugin._start_workflow("改图", "水彩", **context, trigger="api")
        assert accepted["success"]
        await finish_jobs(plugin)
        assert len(plugin._client.submissions) == 1
    asyncio.run(run())


def test_registering_recognized_preset_keeps_parameters_template_and_requested_identity(plugin):
    async def run():
        source = WORKFLOW_PRESETS[0]
        graph = {key: {"class_type": value, "inputs": {}} for key, value in source["signature"].items()}
        for node in source["input_nodes"]:
            graph[node["node_id"]]["inputs"][node["field_name"]] = ""
        preset = match_workflow_preset(graph)
        detected, method = await plugin._detect_key_full(graph)
        assert "预设" in method
        await plugin._append_workflow_to_config(workflow_name="我的另存副本", workflow_id="new-id",
                                                region="domestic", nodes=detected, settings=preset["settings"])
        saved = plugin._find_workflow("我的另存副本")
        assert saved.workflow_id == "new-id" and saved.region == "domestic"
        assert saved.llm_enhance and saved.output_type == "image"
        assert saved.llm_template_path == "prompt/qwen_image_prompt_template.md"
        assert len(next(n for n in saved.input_nodes if n.field_name == "aspect_ratio").choices) == 5
        assert not workflow_errors(saved)
    asyncio.run(run())


def test_duplicate_custom_alias_cannot_silently_override_another_input(plugin):
    first, second = plugin._workflows[0].input_nodes[:2]
    first.input_key = second.input_key = "same-role"
    assert any("别名" in item["message"] for item in workflow_errors(plugin._workflows[0]))
