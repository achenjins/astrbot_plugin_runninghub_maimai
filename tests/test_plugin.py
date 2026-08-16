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
        event = FakeEvent("/wf运行")
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
        event = FakeEvent("/wf工作流")
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




def test_namespaced_import_ignores_stale_top_level_lib(tmp_path: Path) -> None:
    """回归测试：AstrBot 只重载 ``data.plugins.<插件目录>`` 命名空间。

    顶层 ``rh_generic_lib`` 一旦被旧版本加载就会留在 sys.modules 里。插件在
    AstrBot 内必须从自身包命名空间加载本地库，否则升级后 ``dump_config_dict``
    是旧函数（没有 workflow_nodes），识别结果会写丢。
    """
    import shutil
    import subprocess

    root = tmp_path / "simroot"
    package_dir = root / "simdata" / "plugins" / "astrobt_test_plugin"
    lib_dir = package_dir / "rh_generic_lib"
    shutil.copytree(PLUGIN_DIR / "rh_generic_lib", lib_dir)
    shutil.copy2(PLUGIN_DIR / "main.py", package_dir / "main.py")

    stale_root = tmp_path / "stale"
    stale_lib = stale_root / "rh_generic_lib"
    shutil.copytree(PLUGIN_DIR / "rh_generic_lib", stale_lib)
    stale_config = stale_lib / "config.py"
    stale_config.write_text(
        stale_config.read_text(encoding="utf-8")
        + "\n\n"
        + "def dump_config_dict(config):\n"
        + '    return {"config_version": "stale", "server": {}, "generation": {}, '
        + '"feature": {}, "access": {}, "workflows": []}\n',
        encoding="utf-8",
    )

    script = (
        "import sys\n"
        f'sys.path.insert(0, r"{stale_root}")\n'
        f'sys.path.insert(0, r"{root}")\n'
        "import importlib\n"
        'm = importlib.import_module("simdata.plugins.astrobt_test_plugin.main")\n'
        "out = m.dump_config_dict(m.GenericConfig())\n"
        "print(m._config_lib.__file__)\n"
        'print("workflow_nodes" in out)\n'
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=str(PLUGIN_DIR),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    assert lines[-2].startswith(str(lib_dir))
    assert lines[-1] == "True"



def test_page_entry_exists() -> None:
    page = PLUGIN_DIR / "pages" / "workflow-editor" / "index.html"
    assert page.is_file()
    text = page.read_text(encoding="utf-8")
    assert "AstrBotPluginPage" in text
    assert "page/config" in text
    assert "style.css" in text

    style = PLUGIN_DIR / "pages" / "workflow-editor" / "style.css"
    assert style.is_file()
    assert "mask-image" in style.read_text(encoding="utf-8")

    icon_dir = PLUGIN_DIR / "pages" / "workflow-editor" / "assets" / "icons"
    assert len(list(icon_dir.glob("*.svg"))) >= 21


def test_page_config_payload_exposes_workflow_nodes(
    star: plugin_main.RunningHubGenericPlugin,
) -> None:
    raw = {
        "workflows": [
            {
                "__template_key": "workflow",
                "name": "可视化",
                "workflow_id": "42",
                "instance_type": "Standard",
                "region": "overseas",
                "llm_enhance": False,
                "llm_template_path": "",
            }
        ],
        "workflow_nodes": [
            {
                "__template_key": "input_node",
                "workflow_name": "可视化",
                "node_id": "353",
                "field_name": "prompt",
                "field_value": "",
                "value_type": "prompt",
                "label": "提示词",
            }
        ],
    }
    star._apply_config_dict(raw)
    star._refresh_workflows()
    payload = star._page_config_payload()
    assert payload["workflows"][0]["name"] == "可视化"
    assert payload["workflows"][0]["nodes"][0]["node_id"] == "353"
    assert payload["workflows"][0]["nodes"][0]["effective_type"] == "prompt"
    assert any(
        item["path"] == "prompt/anima3_prompt_template.txt"
        for item in payload["prompt_templates"]
    )


def test_page_workflow_payload_validation(
    star: plugin_main.RunningHubGenericPlugin,
) -> None:
    items, error = star._workflows_from_page_payload(
        [
            {
                "name": "可视化",
                "workflow_id": "42",
                "instance_type": "Standard",
                "region": "overseas",
                "llm_enhance": False,
                "llm_template_path": "",
                "nodes": [
                    {
                        "node_id": "353",
                        "field_name": "prompt",
                        "field_value": "",
                        "value_type": "prompt",
                        "label": "提示词",
                    }
                ],
            }
        ]
    )
    assert error == ""
    assert len(items) == 1
    assert items[0].input_nodes[0].node_id == "353"

    _, duplicate_name = star._workflows_from_page_payload(
        [
            {"name": "同名", "workflow_id": "1", "nodes": []},
            {"name": "同名", "workflow_id": "2", "nodes": []},
        ]
    )
    assert "重复" in duplicate_name

    _, duplicate_prompt = star._workflows_from_page_payload(
        [
            {
                "name": "双提示词",
                "workflow_id": "1",
                "nodes": [
                    {"node_id": "1", "field_name": "a", "value_type": "prompt"},
                    {"node_id": "2", "field_name": "b", "value_type": "prompt"},
                ],
            }
        ]
    )
    assert "主提示词" in duplicate_prompt


def test_page_save_persists_workflow_nodes(
    star: plugin_main.RunningHubGenericPlugin,
) -> None:
    items, error = star._workflows_from_page_payload(
        [
            {
                "name": "页面保存",
                "workflow_id": "77",
                "nodes": [
                    {"node_id": "10", "field_name": "text", "value_type": "prompt"},
                    {"node_id": "11", "field_name": "image", "value_type": "image"},
                ],
            }
        ]
    )
    assert error == ""

    async def _run() -> None:
        await star._persist_workflow_items(items)

    asyncio.run(_run())
    saved = star._astrbot_config["workflow_nodes"]
    assert [n["node_id"] for n in saved] == ["10", "11"]
    assert star._workflow_names() == ["页面保存"]

def test_preset_prompt_template_ships_with_plugin() -> None:
    template = PLUGIN_DIR / "prompt" / "anima3_prompt_template.txt"
    assert template.is_file()
    content = template.read_text(encoding="utf-8")
    assert content.startswith("# ANIMA3 提示词生成模板")


def test_prompt_template_path_validation(
    star: plugin_main.RunningHubGenericPlugin,
) -> None:
    target = star._safe_prompt_template("anima3_prompt_template.txt")
    assert target is not None
    assert target.name == "anima3_prompt_template.txt"
    assert star._safe_prompt_template("../metadata.yaml") is None
    assert star._safe_prompt_template("bad.exe") is None
    assert star._safe_prompt_template("") is None


def test_prompt_template_list_contains_preset(
    star: plugin_main.RunningHubGenericPlugin,
) -> None:
    templates = star._list_prompt_templates()
    paths = [item["path"] for item in templates]
    assert "prompt/anima3_prompt_template.txt" in paths