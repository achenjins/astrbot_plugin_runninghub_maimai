"""AstrBot 迁移插件基础测试（不需要真实 RunningHub / AstrBot Core）。"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

import pytest

PLUGIN_DIR = Path(__file__).resolve().parents[1]
if str(PLUGIN_DIR) not in sys.path:
    sys.path.insert(0, str(PLUGIN_DIR))

from astrbot.core.config.astrbot_config import AstrBotConfig  # noqa: E402

import main as plugin_main  # noqa: E402
from rh_generic_lib.delivery import Delivery, DeliveryTarget  # noqa: E402
from rh_generic_lib.legacy_config import parse_legacy_toml  # noqa: E402
from rh_generic_lib.workflow_detect import detect_key_nodes  # noqa: E402


class FakeContext:
    def __init__(self) -> None:
        self.sent: list[tuple[str, Any]] = []
        self.web_apis: list[tuple] = []
        self.platform_inst: Any | None = None

    async def send_message(self, session: str, message_chain: Any) -> bool:
        self.sent.append((session, message_chain))
        return True

    def register_web_api(self, route, handler, methods, desc) -> None:
        self.web_apis.append((route, handler, methods, desc))

    def get_platform_inst(self, platform_id: str) -> Any | None:
        return self.platform_inst

    def get_using_provider(self) -> Any | None:
        return None

    async def get_current_chat_provider_id(self, umo: str) -> str:
        return ""

    def get_llm_tool_manager(self) -> Any | None:
        return None


class FakeEvent:
    def __init__(self, text: str = "", user_id: str = "10001", group_id: str = "20001") -> None:
        self.message_str = text
        self._user_id = user_id
        self._group_id = group_id
        self._platform_id = "fake"
        self._extras: dict[str, Any] = {}
        self.call_llm = False

    @property
    def unified_msg_origin(self) -> str:
        return f"fake:group_message:{self._group_id}"

    def get_sender_id(self) -> str:
        return self._user_id

    def get_group_id(self) -> str:
        return self._group_id

    def get_platform_id(self) -> str:
        return self._platform_id

    def get_messages(self) -> list:
        return []

    def get_extra(self, key: str, default: Any = None) -> Any:
        return self._extras.get(key, default)

    def set_extra(self, key: str, value: Any) -> None:
        self._extras[key] = value

    def should_call_llm(self, call_llm: bool) -> None:
        self.call_llm = call_llm
    def stop_event(self) -> None:
        self._result = object()



@pytest.fixture()
def ctx() -> FakeContext:
    return FakeContext()


@pytest.fixture()
def star(ctx: FakeContext, tmp_path: Path) -> plugin_main.RunningHubGenericPlugin:
    schema = json.loads((PLUGIN_DIR / "_conf_schema.json").read_text(encoding="utf-8"))
    cfg = AstrBotConfig(config_path=str(tmp_path / "plugin_config.json"), schema=schema)
    return plugin_main.RunningHubGenericPlugin(ctx, cfg)


def test_config_defaults(star: plugin_main.RunningHubGenericPlugin) -> None:
    assert star.config.server.base_url == "https://www.runninghub.ai"
    assert star.config.generation.poll_interval == 15
    assert star.config.feature.result_notice is True
    assert star.config.workflows.items == []


def test_append_workflow_persists_to_astrbot_config(
    star: plugin_main.RunningHubGenericPlugin, tmp_path: Path
) -> None:
    async def _run() -> None:
        await star._append_workflow_to_config(
            workflow_name="测试",
            workflow_id="42",
            nodes=[{"node_id": "353", "field_name": "prompt", "value_type": "prompt"}],
            region="overseas",
        )

    asyncio.run(_run())
    assert star._workflow_names() == ["测试"]
    saved = star._astrbot_config["workflows"][0]
    assert saved["name"] == "测试"
    saved_nodes = star._astrbot_config["workflow_nodes"]
    assert saved_nodes[0]["workflow_name"] == "测试"
    assert saved_nodes[0]["node_id"] == "353"


def test_config_workflow_json_nodes_roundtrip(star: plugin_main.RunningHubGenericPlugin) -> None:
    raw = {
        "config_version": "2.0.0",
        "server": {},
        "generation": {},
        "feature": {},
        "access": {},
        "workflows": [
            {
                "__template_key": "workflow",
                "name": "动漫生图",
                "workflow_id": "123",
                "instance_type": "Standard",
                "region": "overseas",
                "llm_enhance": False,
                "llm_template_path": "","input_node_1": {
                      "node_id": "353",
                      "field_name": "prompt",
                      "field_value": "",
                      "value_type": "prompt",
                      "label": "提示词",
                  },
            }
        ],
    }
    star._apply_config_dict(raw)
    star._refresh_workflows()
    assert star._workflow_names() == ["动漫生图"]
    dumped = star._dump_config_dict()
    assert dumped["workflows"][0]["__template_key"] == "workflow"
    assert dumped["workflow_nodes"][0]["workflow_name"] == "动漫生图"
    assert dumped["workflow_nodes"][0]["node_id"] == "353"


def test_detect_key_nodes(star: plugin_main.RunningHubGenericPlugin) -> None:
    workflow_json = {
        "6": {"class_type": "CLIPTextEncode", "inputs": {"text": "hello", "clip": ["4", 0]}},
        "5": {"class_type": "EmptyLatentImage", "inputs": {"width": 512, "height": 512}},
    }
    detected = detect_key_nodes(workflow_json)
    assert {"node_id": "6", "value_type": "prompt"} in [
        {k: n[k] for k in ("node_id", "value_type")} for n in detected
    ]
    assert any(n["field_name"] == "width" and n["value_type"] == "default" for n in detected)


def test_empty_run_command_replies_usage(star: plugin_main.RunningHubGenericPlugin, ctx: FakeContext) -> None:
    async def _run() -> None:
        event = FakeEvent("/rh运行")
        await star.handle_pao_tu(event)

    asyncio.run(_run())
    assert ctx.sent
    assert "用法" in ctx.sent[0][1].get_plain_text()


def test_list_workflows_command(star: plugin_main.RunningHubGenericPlugin, ctx: FakeContext) -> None:
    star._apply_config_dict(
        {
            "workflows": [
                {
                    "__template_key": "workflow",
                    "name": "动漫生图",
                    "workflow_id": "123",
                    "input_nodes": "[]",
                }
            ]
        }
    )
    star._refresh_workflows()

    async def _run() -> None:
        event = FakeEvent("/工作流")
        await star.handle_list_workflows(event)

    asyncio.run(_run())
    assert any("动漫生图" in sent[1].get_plain_text() for sent in ctx.sent)


def test_delivery_prefers_generic_for_plain_send(ctx: FakeContext) -> None:
    class FakeBot:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def call_action(self, action: str, **kwargs: Any) -> dict:
            self.calls.append(action)
            return {"status": "ok", "retcode": 0, "data": {"message_id": 1}}

    class FakePlatform:
        def __init__(self, bot: FakeBot) -> None:
            self._bot = bot

        def get_client(self) -> FakeBot:
            return self._bot

    bot = FakeBot()
    ctx.platform_inst = FakePlatform(bot)
    delivery = Delivery(ctx, __import__("logging").getLogger("test"))
    target = DeliveryTarget(
        stream_id="fake:group_message:20001",
        group_id="20001",
        user_id="10001",
        platform_id="fake",
    )

    async def _run() -> None:
        await delivery.send_text(target, "hello")

    asyncio.run(_run())
    assert bot.calls == []
    assert len(ctx.sent) == 1


def test_onebot_recall_treats_none_data_as_success(ctx: FakeContext) -> None:
    class FakeBot:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def call_action(self, action: str, **kwargs: Any) -> None:
            self.calls.append(action)
            return None

    class FakePlatform:
        def __init__(self, bot: FakeBot) -> None:
            self._bot = bot

        def get_client(self) -> FakeBot:
            return self._bot

    bot = FakeBot()
    ctx.platform_inst = FakePlatform(bot)
    delivery = Delivery(ctx, __import__("logging").getLogger("test"))
    target = DeliveryTarget(platform_id="fake")

    async def _run() -> None:
        ok = await delivery.recall(target, "123")
        assert ok is True

    asyncio.run(_run())
    assert bot.calls == ["delete_msg"]



def test_legacy_toml_conversion(tmp_path: Path) -> None:
    toml_text = """[server]
base_url = "https://www.runninghub.ai"
api_key = "old-key"
base_url_cn = "https://www.runninghub.cn"
api_key_cn = ""

[generation]
poll_interval = 7
max_wait = 600
max_concurrent = 3
download_timeout = 60

[feature]
enable = true
recall_seconds = 30
use_llm = false
model = "utils"
enhance_model = "replyer"

[access]
allow_users = ["10001"]
allow_groups = ["20001"]
max_per_user_per_hour = 5
admin_users = ["10001"]

[[workflows.items]]
name = "动漫生图"
workflow_id = "123456"
instance_type = "Plus"
region = "domestic"
llm_enhance = true
llm_template_path = "templates/test.txt"

[[workflows.items.input_nodes]]
node_id = "353"
field_name = "prompt"
field_value = ""
value_type = "prompt"
label = "提示词"
"""
    src = tmp_path / "config.toml"
    src.write_text(toml_text, encoding="utf-8")
    converted = parse_legacy_toml(src)

    assert converted["server"]["api_key"] == "old-key"
    assert converted["generation"]["poll_interval"] == 7
    assert converted["feature"]["enable"] is True
    assert converted["feature"]["model"] == ""
    assert converted["feature"]["enhance_model"] == ""
    assert converted["access"]["allow_users"] == ["10001"]
    assert len(converted["workflows"]) == 1
    workflow = converted["workflows"][0]
    assert workflow["__template_key"] == "workflow"
    assert workflow["instance_type"] == "Plus"
    assert workflow["region"] == "domestic"
    node = converted["workflow_nodes"][0]
    assert node["__template_key"] == "input_node"
    assert node["workflow_name"] == "动漫生图"
    assert node["node_id"] == "353"


def test_legacy_import_on_first_deploy(
    ctx: FakeContext,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy = tmp_path / "config.toml"
    legacy.write_text(
        """[server]
api_key = "legacy-key"

[[workflows.items]]
name = "旧工作流"
workflow_id = "999"
input_nodes = []
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(plugin_main, "_PLUGIN_DIR", tmp_path)

    schema = json.loads((PLUGIN_DIR / "_conf_schema.json").read_text(encoding="utf-8"))
    cfg = AstrBotConfig(config_path=str(tmp_path / "new_config.json"), schema=schema)
    star = plugin_main.RunningHubGenericPlugin(ctx, cfg)
    star._import_legacy_config_if_needed()
    star._apply_config_dict(dict(cfg))
    star._refresh_workflows()

    assert star.config.server.api_key == "legacy-key"
    assert star._workflow_names() == ["旧工作流"]


def test_legacy_json_input_nodes_still_parses(star: plugin_main.RunningHubGenericPlugin) -> None:
    raw = {
        "workflows": [
            {
                "__template_key": "workflow",
                "name": "旧版",
                "workflow_id": "9",
                "input_nodes": json.dumps(
                    [{"node_id": "353", "field_name": "prompt", "value_type": "prompt"}],
                    ensure_ascii=False,
                ),
            }
        ]
    }
    star._apply_config_dict(raw)
    star._refresh_workflows()
    assert star._workflow_names() == ["旧版"]
    assert star.config.workflows.items[0].input_nodes[0].node_id == "353"


def test_embedded_node_slots_survive_astrbot_default_workflow_nodes(
    star: plugin_main.RunningHubGenericPlugin,
) -> None:
    # AstrBotConfig 会给新 schema 自动补 workflow_nodes=[]，
    # 但旧配置仍把节点嵌在 workflow 项里，必须优先使用旧嵌入节点。
    raw = {
        "workflows": [
            {
                "__template_key": "workflow",
                "name": "旧配置",
                "workflow_id": "7",
                "input_node_1": {
                    "node_id": "353",
                    "field_name": "prompt",
                    "field_value": "",
                    "value_type": "prompt",
                    "label": "提示词",
                },
            }
        ],
        "workflow_nodes": [],
    }
    star._apply_config_dict(raw)
    star._refresh_workflows()
    assert star._workflow_names() == ["旧配置"]
    assert star.config.workflows.items[0].input_nodes[0].node_id == "353"
