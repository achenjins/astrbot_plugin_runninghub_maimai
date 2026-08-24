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
from astrbot.api.message_components import File as FileComponent  # noqa: E402

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
    def __init__(
        self,
        text: str = "",
        user_id: str = "10001",
        group_id: str = "20001",
        messages: list[Any] | None = None,
    ) -> None:
        self.message_str = text
        self._user_id = user_id
        self._group_id = group_id
        self._platform_id = "fake"
        self._messages = messages or []
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
        return self._messages

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


def test_delivery_falls_back_to_file_when_onebot_video_fails(ctx: FakeContext) -> None:
    class FakeBot:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, Any]]] = []

        async def call_action(self, action: str, **kwargs: Any) -> dict[str, Any]:
            self.calls.append((action, kwargs))
            if action == "send_group_msg" and kwargs["message"][0]["type"] == "video":
                return {
                    "status": "failed",
                    "retcode": 1200,
                    "message": "rich media transfer failed",
                }
            return {"status": "ok", "retcode": 0, "data": {"message_id": 88}}

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
        platform_id="fake",
    )

    async def _run() -> str:
        return await delivery.send_video(
            target, "https://runninghub.example/video/result.mp4", need_message_id=True
        )

    message_id = asyncio.run(_run())
    assert message_id == "88"
    assert len(bot.calls) == 2
    assert bot.calls[0][1]["message"][0]["type"] == "video"
    assert bot.calls[1][1]["message"][0]["type"] == "file"
    assert bot.calls[1][1]["message"][0]["data"]["file"].endswith("/result.mp4")


def test_delivery_does_not_duplicate_successful_onebot_video(ctx: FakeContext) -> None:
    class FakeBot:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def call_action(self, action: str, **kwargs: Any) -> dict[str, Any]:
            self.calls.append(action)
            return {"status": "ok", "retcode": 0, "data": {"message_id": 89}}

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
        platform_id="fake",
    )

    async def _run() -> str:
        return await delivery.send_video(target, "https://runninghub.example/video/result.mp4")

    assert asyncio.run(_run()) == ""
    assert bot.calls == ["send_group_msg"]


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
    # 页面下拉传的是 prompt/xxx.txt 相对路径，也必须能解析
    relative = star._safe_prompt_template("prompt/anima3_prompt_template.txt")
    assert relative is not None
    assert relative.name == "anima3_prompt_template.txt"
    assert star._safe_prompt_template("../metadata.yaml") is None
    assert star._safe_prompt_template("bad.exe") is None
    assert star._safe_prompt_template("") is None


def test_prompt_template_list_contains_preset(
    star: plugin_main.RunningHubGenericPlugin,
) -> None:
    templates = star._list_prompt_templates()
    paths = [item["path"] for item in templates]
    assert "prompt/anima3_prompt_template.txt" in paths

def test_upload_file_uses_legacy_workflow_endpoint() -> None:
    from unittest.mock import patch

    from rh_generic_lib import runninghub_client as client_module

    client = client_module.RunningHubClient(
        api_key="k", base_url="https://www.runninghub.cn", timeout=10, workflow_id=""
    )
    with patch("rh_generic_lib.runninghub_client.requests.post") as mock_post:
        mock_post.return_value.json.return_value = {
            "code": 0,
            "msg": "success",
            "data": {"fileName": "api/abc.png", "fileType": "input"},
        }

        async def _run() -> str:
            return await client.upload_file(b"imgdata", "ref.png")

        file_name = asyncio.run(_run())
    assert file_name == "api/abc.png"
    called = mock_post.call_args
    assert called.args[0].endswith("/task/openapi/upload")
    assert called.kwargs["data"]["apiKey"] == "k"
    assert called.kwargs["data"]["fileType"] == "input"

def test_prompt_templates_live_in_persistent_data_dir(
    star: plugin_main.RunningHubGenericPlugin,
) -> None:
    directory = star._prompt_templates_dir()
    assert "plugin_data" in directory.parts
    assert directory != (PLUGIN_DIR / "prompt")
    # 内置种子模板会自动复制到持久化目录
    assert (directory / "anima3_prompt_template.txt").is_file()

def test_template_list_merges_files_from_plugin_dir(
    star: plugin_main.RunningHubGenericPlugin,
    tmp_path: Path,
    monkeypatch,
) -> None:
    fake_plugin = tmp_path / "fake_plugin"
    (fake_plugin / "prompt").mkdir(parents=True)
    (fake_plugin / "prompt" / "manual_ref2va.md").write_text("manual", encoding="utf-8")
    monkeypatch.setattr(plugin_main, "_PLUGIN_DIR", fake_plugin)
    paths = [item["path"] for item in star._list_prompt_templates()]
    assert "prompt/manual_ref2va.md" in paths

def test_load_llm_template_from_persistent_dir_with_prompt_prefix(
    star: plugin_main.RunningHubGenericPlugin,
) -> None:
    directory = star._prompt_templates_dir()
    target = directory / "load_test_template.md"
    target.write_text("PERSISTED_TEMPLATE", encoding="utf-8")
    try:
        workflow = plugin_main.WorkflowItemSection(
            name="读取测试",
            workflow_id="1",
            llm_template_path="prompt/load_test_template.md",
        )
        assert star._load_llm_template(workflow) == "PERSISTED_TEMPLATE"
    finally:
        target.unlink(missing_ok=True)


def test_page_account_endpoint_reports_both_regions(
    star: plugin_main.RunningHubGenericPlugin,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        plugin_main.RunningHubGenericPlugin,
        "_web_jsonify",
        staticmethod(lambda payload: payload),
    )
    star._apply_config_dict({"server": {"api_key": "overseas_key", "api_key_cn": ""}})

    class FakeClient:
        async def account_status(self) -> dict:
            return {
                "remainCoins": "123.4",
                "currentTaskCounts": "2",
                "remainMoney": "5.6",
                "currency": "USD",
                "apiType": "NORMAL",
            }

    monkeypatch.setattr(star, "_get_client", lambda region: FakeClient())
    payload = asyncio.run(star.handle_page_get_account())
    assert payload["success"] is True
    regions = payload["data"]["regions"]
    overseas = next(item for item in regions if item["region"] == "overseas")
    assert overseas["status"] == "ok"
    assert overseas["account"]["remain_coins"] == "123.4"
    domestic = next(item for item in regions if item["region"] == "domestic")
    assert domestic["status"] == "missing"



def test_account_status_requests_uc_endpoint() -> None:
    from unittest.mock import patch

    from rh_generic_lib import runninghub_client as client_module

    client = client_module.RunningHubClient(
        api_key="k", base_url="https://www.runninghub.cn", timeout=10, workflow_id=""
    )
    with patch("rh_generic_lib.runninghub_client.requests.post") as mock_post:
        mock_post.return_value.json.return_value = {
            "code": 0,
            "msg": "success",
            "data": {
                "remainCoins": "888",
                "currentTaskCounts": "1",
                "remainMoney": "12.5",
                "currency": "CNY",
                "apiType": "NORMAL",
            },
        }

        async def _run() -> dict:
            return await client.account_status()

        account = asyncio.run(_run())
    assert account["remainCoins"] == "888"
    called = mock_post.call_args
    assert called.args[0].endswith("/uc/openapi/accountStatus")
    assert called.kwargs["json"] == {"apikey": "k"}
    assert called.kwargs["headers"]["Authorization"] == "Bearer k"


def test_task_history_lives_in_merged_editor_page() -> None:
    assert not (PLUGIN_DIR / "pages" / "task-history").exists()
    page = PLUGIN_DIR / "pages" / "workflow-editor" / "index.html"
    text = page.read_text(encoding="utf-8")
    assert "page/tasks" in text
    assert "page/tasks/clear" in text
    assert "page/account" in text
    for name in ("history.svg", "wallet.svg", "refresh.svg", "delete.svg"):
        assert (PLUGIN_DIR / "pages" / "workflow-editor" / "assets" / "icons" / name).is_file()


def test_consume_coins_parses_v2_usage() -> None:
    star = plugin_main.RunningHubGenericPlugin
    assert star._consume_coins_from_result({"usage": {"consumeCoins": "3.25"}}) == "3.25"
    assert star._consume_coins_from_result({"usage": {"consume_coins": "7"}}) == "7"
    assert star._consume_coins_from_result({"consumeCoins": 2}) == "2"
    assert star._consume_coins_from_result({"status": "RUNNING"}) == "0"


def test_task_history_stores_only_minimal_fields_and_dedupes(
    ctx: FakeContext,
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(plugin_main, "get_astrbot_plugin_data_path", lambda: str(tmp_path))
    star = plugin_main.RunningHubGenericPlugin(ctx, None)

    async def _run() -> None:
        await star._record_task_history("task_1", "测试工作流", "1.5")
        await star._record_task_history("task_1", "重复任务", "9")

    asyncio.run(_run())
    assert star._task_history == [
        {"task_id": "task_1", "workflow": "测试工作流", "coins": "1.5"}
    ]
    path = star._task_history_path()
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert raw == star._task_history
    assert set(raw[0].keys()) == set(plugin_main._TASK_HISTORY_FIELDS)

    reloaded = plugin_main.RunningHubGenericPlugin(ctx, None)
    reloaded._load_task_history()
    assert reloaded._task_history == raw


def test_task_history_caps_records(
    ctx: FakeContext,
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(plugin_main, "get_astrbot_plugin_data_path", lambda: str(tmp_path))
    monkeypatch.setattr(plugin_main, "_TASK_HISTORY_MAX", 2)
    star = plugin_main.RunningHubGenericPlugin(ctx, None)

    async def _run() -> None:
        for index in range(3):
            await star._record_task_history(f"task_{index}", "工作流", "0.5")

    asyncio.run(_run())
    assert len(star._task_history) == 2
    assert star._task_history[0]["task_id"] == "task_2"
    assert star._task_history[-1]["task_id"] == "task_1"


def test_recent_prompt_cache_is_per_user_persistent_and_caps_at_five(
    ctx: FakeContext,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(plugin_main, "get_astrbot_plugin_data_path", lambda: str(tmp_path))
    star = plugin_main.RunningHubGenericPlugin(ctx, None)
    workflow = plugin_main.WorkflowItemSection(
        name="动漫生图", workflow_id="42", region="overseas"
    )

    async def _run() -> None:
        for index in range(6):
            await star._record_recent_prompt(
                workflow,
                f"原始描述 {index}",
                f"扩写提示词 {index}",
                user_id="10001",
                platform_id="fake",
            )
        await star._record_recent_prompt(
            workflow,
            "另一个用户",
            "另一个用户的扩写",
            user_id="10002",
            platform_id="fake",
        )

    asyncio.run(_run())
    owner_key = star._prompt_owner_key("10001", "fake")
    recent = star._prompt_entries(owner_key, "recent")
    assert len(recent) == 5
    assert recent[0]["original_prompt"] == "原始描述 5"
    assert recent[-1]["original_prompt"] == "原始描述 1"

    reloaded = plugin_main.RunningHubGenericPlugin(ctx, None)
    reloaded._load_prompt_library()
    assert reloaded._prompt_entries(owner_key, "recent") == recent
    other_key = reloaded._prompt_owner_key("10002", "fake")
    assert len(reloaded._prompt_entries(other_key, "recent")) == 1


def test_cached_prompt_run_skips_llm_enhancement_and_records_submission(
    star: plugin_main.RunningHubGenericPlugin,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    star._apply_config_dict(
        {
            "server": {"api_key": "test-key"},
            "workflows": [
                {
                    "__template_key": "workflow",
                    "name": "复用测试",
                    "workflow_id": "42",
                    "region": "overseas",
                    "llm_enhance": True,
                    "input_nodes": [
                        {
                            "node_id": "353",
                            "field_name": "prompt",
                            "value_type": "prompt",
                        }
                    ],
                }
            ],
        }
    )
    star._refresh_workflows()
    star._client = object()
    submitted: list[dict[str, str]] = []
    recorded: list[tuple[str, str]] = []

    async def fail_enhance(*args: Any, **kwargs: Any) -> str:
        raise AssertionError("缓存提示词不应再次扩写")

    async def fake_submit(
        client: Any,
        workflow: Any,
        node_info_list: list[dict[str, str]],
        stream_id: str,
        kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        submitted.extend(node_info_list)
        return {"success": True, "task_id": "task-1", "message": "已运行"}

    async def fake_record(
        workflow: Any,
        original_prompt: str,
        enhanced_prompt: str,
        **kwargs: Any,
    ) -> None:
        recorded.append((original_prompt, enhanced_prompt))

    monkeypatch.setattr(star, "_enhance_text", fail_enhance)
    monkeypatch.setattr(star, "_submit_and_poll", fake_submit)
    monkeypatch.setattr(star, "_record_recent_prompt", fake_record)

    async def _run() -> dict[str, Any]:
        return await star._start_workflow(
            "复用测试",
            "扩写前描述",
            reused_prompt="已经扩写完成的提示词",
            stream_id="fake:group_message:20001",
            user_id="10001",
            group_id="20001",
            platform_id="fake",
        )

    result = asyncio.run(_run())
    assert result["success"] is True
    assert submitted[0]["fieldValue"] == "已经扩写完成的提示词"
    assert recorded == [("扩写前描述", "已经扩写完成的提示词")]


def test_save_prompt_command_uses_number_then_description(
    ctx: FakeContext,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(plugin_main, "get_astrbot_plugin_data_path", lambda: str(tmp_path))
    star = plugin_main.RunningHubGenericPlugin(ctx, None)
    owner_key = star._prompt_owner_key("10001", "fake")
    star._prompt_library[owner_key] = {
        "recent": [
            {
                "workflow_name": "动漫生图",
                "workflow_id": "42",
                "region": "overseas",
                "original_prompt": "一只窗边的猫",
                "enhanced_prompt": "完整扩写提示词",
                "description": "",
                "created_at": 1.0,
                "saved_at": 0.0,
            }
        ],
        "saved": [],
    }

    async def _run() -> None:
        await star.handle_prompt_save_command(FakeEvent("/wf 保存提示词"))
        await star.handle_input_collector(FakeEvent("1"))
        await star.handle_input_collector(FakeEvent("窗边猫模板"))

    asyncio.run(_run())
    saved = star._prompt_entries(owner_key, "saved")
    assert len(saved) == 1
    assert saved[0]["description"] == "窗边猫模板"
    messages = [item[1].get_plain_text() for item in ctx.sent]
    assert any("一只窗边的猫" in message for message in messages)
    assert any("请发送这条提示词的保存描述" in message for message in messages)
    assert any("已保存提示词：窗边猫模板" in message for message in messages)


def test_prompt_commands_are_registered_as_astrbot_command_group() -> None:
    from astrbot.core.star.filter.command import CommandFilter
    from astrbot.core.star.filter.command_group import CommandGroupFilter
    from astrbot.core.star.star_handler import star_handlers_registry

    handlers = {handler.handler_name: handler for handler in star_handlers_registry}
    group = handlers["prompt_command_group"]
    assert any(isinstance(item, CommandGroupFilter) for item in group.event_filters)

    expected = {
        "handle_prompt_save_command": "wf 保存提示词",
        "handle_prompt_rerun_command": "wf 提示词重跑",
        "handle_prompt_list_command": "wf 提示词",
        "handle_prompt_upload_command": "wf 上传提示词",
        "handle_prompt_upload_template_command": "wf 上传提示词模板",
    }
    for handler_name, command_name in expected.items():
        handler = handlers[handler_name]
        command_filter = next(
            item for item in handler.event_filters if isinstance(item, CommandFilter)
        )
        assert command_filter.get_complete_command_names() == [command_name]
        assert handler.extras_configs.get("sub_command") is True


def test_prompt_command_can_interrupt_existing_prompt_interaction(
    star: plugin_main.RunningHubGenericPlugin,
) -> None:
    """WakingCheckStage 会先去掉 /，新命令仍须优先于旧的数字交互。"""
    stream_id = "fake:group_message:20001"
    key = star._session_key("10001", stream_id)
    event = FakeEvent("wf 提示词")

    async def _run() -> None:
        star._register_prompt_interaction(
            plugin_main.PromptInteraction(
                user_id="10001",
                stream_id=stream_id,
                owner_key="fake:10001",
                phase="save_select",
                entries=[{"original_prompt": "测试提示词"}],
            )
        )
        await star.handle_input_collector(event)

    asyncio.run(_run())
    assert key not in star._prompt_interactions
    assert star._is_consumed(event) is False


def test_saved_prompt_list_can_delete_by_number(
    ctx: FakeContext,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(plugin_main, "get_astrbot_plugin_data_path", lambda: str(tmp_path))
    star = plugin_main.RunningHubGenericPlugin(ctx, None)
    owner_key = star._prompt_owner_key("10001", "fake")
    entries = [
        {
            "workflow_name": "工作流一",
            "workflow_id": "1",
            "region": "overseas",
            "original_prompt": "原描述一",
            "enhanced_prompt": "扩写一",
            "description": "第一个",
            "created_at": 1.0,
            "saved_at": 3.0,
        },
        {
            "workflow_name": "工作流二",
            "workflow_id": "2",
            "region": "overseas",
            "original_prompt": "原描述二",
            "enhanced_prompt": "扩写二",
            "description": "第二个",
            "created_at": 2.0,
            "saved_at": 3.0,
        },
    ]
    star._prompt_library[owner_key] = {"recent": [], "saved": entries}

    async def _run() -> None:
        await star.handle_prompt_list_command(FakeEvent("/wf 提示词"))
        await star.handle_input_collector(FakeEvent("删除1"))

    asyncio.run(_run())
    saved = star._prompt_entries(owner_key, "saved")
    assert [item["description"] for item in saved] == ["第二个"]
    messages = [item[1].get_plain_text() for item in ctx.sent]
    assert any("已删除：第一个" in message for message in messages)
    assert any("第二个" in message and "回复数字运行" in message for message in messages)


def test_upload_prompt_file_command_saves_txt_template(
    ctx: FakeContext,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(plugin_main, "get_astrbot_plugin_data_path", lambda: str(tmp_path))
    star = plugin_main.RunningHubGenericPlugin(ctx, None)
    source = tmp_path / "my_prompt.md"
    source.write_text("# prompt\n描写一只雨夜的猫", encoding="utf-8")
    file_event = FakeEvent(
        messages=[FileComponent("my_prompt.md", file=str(source))],
    )

    async def _run() -> None:
        await star.handle_prompt_upload_template_command(FakeEvent("/wf 上传提示词模板"))
        await star.handle_input_collector(file_event)

    asyncio.run(_run())
    target = star._prompt_templates_dir() / "my_prompt.md"
    assert target.read_text(encoding="utf-8") == "# prompt\n描写一只雨夜的猫"
    messages = [item[1].get_plain_text() for item in ctx.sent]
    assert any("提示词模板已上传：my_prompt.md" in message for message in messages)


def test_upload_prompt_file_adds_saved_prompt_for_selected_workflow(
    ctx: FakeContext,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(plugin_main, "get_astrbot_plugin_data_path", lambda: str(tmp_path))
    star = plugin_main.RunningHubGenericPlugin(ctx, None)
    star._workflows = [
        plugin_main.WorkflowItemSection(
            name="工作流一", workflow_id="1", region="overseas"
        ),
        plugin_main.WorkflowItemSection(
            name="工作流二", workflow_id="2", region="domestic"
        ),
    ]
    source = tmp_path / "rainy_cat.md"
    source.write_text("# prompt\n描写一只雨夜的猫", encoding="utf-8")

    async def _run() -> None:
        await star.handle_prompt_upload_command(FakeEvent("/wf 上传提示词"))
        await star.handle_input_collector(FakeEvent("2"))
        await star.handle_input_collector(
            FakeEvent(messages=[FileComponent("rainy_cat.md", file=str(source))])
        )
        await star.handle_prompt_list_command(FakeEvent("/wf 提示词"))

    asyncio.run(_run())
    owner_key = star._prompt_owner_key("10001", "fake")
    saved = star._prompt_entries(owner_key, "saved")
    assert len(saved) == 1
    assert saved[0]["description"] == "rainy_cat"
    assert saved[0]["workflow_name"] == "工作流二"
    assert saved[0]["region"] == "domestic"
    assert saved[0]["enhanced_prompt"] == "# prompt\n描写一只雨夜的猫"
    messages = [item[1].get_plain_text() for item in ctx.sent]
    assert any("已加入提示词：rainy_cat" in message for message in messages)
    assert any("rainy_cat" in message and "回复数字运行" in message for message in messages)


@pytest.mark.parametrize("encoding", ["gb18030", "big5", "utf-16"])
def test_upload_prompt_file_auto_decodes_common_encodings(
    ctx: FakeContext,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    encoding: str,
) -> None:
    monkeypatch.setattr(plugin_main, "get_astrbot_plugin_data_path", lambda: str(tmp_path))
    star = plugin_main.RunningHubGenericPlugin(ctx, None)
    content = (
        "# 提示詞\n描寫一隻雨夜裡的貓"
        if encoding == "big5"
        else "# 提示词\n描写一只雨夜的猫"
    )
    filename = f"encoded_{encoding.replace('-', '_')}.txt"
    source = tmp_path / filename
    source.write_bytes(content.encode(encoding))

    async def _run() -> None:
        await star.handle_prompt_upload_template_command(FakeEvent("/wf 上传提示词模板"))
        await star.handle_input_collector(
            FakeEvent(messages=[FileComponent(filename, file=str(source))])
        )

    asyncio.run(_run())
    target = star._prompt_templates_dir() / filename
    assert target.read_text(encoding="utf-8") == content
    messages = [item[1].get_plain_text() for item in ctx.sent]
    assert any("已转 UTF-8" in message for message in messages)


def test_napcat_file_result_downloads_url_instead_of_decoding_as_base64(
    star: plugin_main.RunningHubGenericPlugin,
) -> None:
    class FakeClient:
        async def download_bytes(self, url: str) -> bytes:
            assert url == "https://files.example.test/text.md"
            return "# 提示词\n一只猫".encode("utf-8")

    star._client = FakeClient()

    result = asyncio.run(
        star._extract_bytes_from_napcat_result(
            {"data": {"file": "https://files.example.test/text.md"}}
        )
    )
    assert result == "# 提示词\n一只猫".encode("utf-8")


def test_prompt_file_component_skips_napcat_group_download_url(
    star: plugin_main.RunningHubGenericPlugin,
) -> None:
    event = FakeEvent(
        messages=[
            FileComponent(
                "text.md",
                url="https://gzc-download.ftn.qq.com/ftn_handler/bad.zip",
            )
        ]
    )
    assert asyncio.run(star._extract_prompt_file_from_event(event)) is None
