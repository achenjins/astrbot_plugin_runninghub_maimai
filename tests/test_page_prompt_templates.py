"""A rejected Pages template upload must preserve the last usable template."""
import asyncio
from io import BytesIO
from pathlib import Path

import pytest
from quart import Quart
from quart.datastructures import FileStorage

import main as plugin_main
from .test_plugin import FakeContext


@pytest.mark.parametrize("content", [b"", b"\xff\xfe invalid UTF-8", b"x" * (2 * 1024 * 1024 + 1)],
                         ids=["empty", "invalid-utf8", "oversized"])
def test_rejected_multipart_template_overwrite_preserves_existing_file(tmp_path, monkeypatch, content):
    monkeypatch.setattr(plugin_main, "get_astrbot_plugin_data_path", lambda: str(tmp_path))
    star = plugin_main.RunningHubGenericPlugin(FakeContext(), None)
    target = star._safe_prompt_template("custom.md")
    target.write_text("existing usable template", encoding="utf-8")
    app = Quart(__name__)
    app.add_url_rule("/upload", view_func=star.handle_page_upload_prompt_template, methods=["POST"])

    async def run():
        response = await app.test_client().post(
            "/upload", files={"file": FileStorage(stream=BytesIO(content), filename="custom.md")})
        result = await response.get_json()
        assert result["success"] is False
        assert target.is_file(), "a rejected upload deleted the existing template"
        assert target.read_text(encoding="utf-8") == "existing usable template"

    asyncio.run(run())


def test_template_replace_failure_preserves_existing_file_and_cleans_candidate(tmp_path, monkeypatch):
    monkeypatch.setattr(plugin_main, "get_astrbot_plugin_data_path", lambda: str(tmp_path))
    star = plugin_main.RunningHubGenericPlugin(FakeContext(), None)
    target = star._safe_prompt_template("custom.md")
    target.write_text("existing usable template", encoding="utf-8")
    app = Quart(__name__)
    app.add_url_rule("/upload", view_func=star.handle_page_upload_prompt_template, methods=["POST"])

    def fail_replace(source, destination):
        raise OSError("replacement refused")

    monkeypatch.setattr(Path, "replace", fail_replace)

    async def run():
        response = await app.test_client().post("/upload", json={"filename": "custom.md", "content": "new template"})
        result = await response.get_json()
        assert result["success"] is False
        assert target.read_text(encoding="utf-8") == "existing usable template"
        assert not list(target.parent.glob(".prompt-upload-*"))

    asyncio.run(run())


@pytest.mark.parametrize("multipart", [True, False])
def test_valid_template_overwrite_replaces_existing_file(tmp_path, monkeypatch, multipart):
    monkeypatch.setattr(plugin_main, "get_astrbot_plugin_data_path", lambda: str(tmp_path))
    star = plugin_main.RunningHubGenericPlugin(FakeContext(), None)
    target = star._safe_prompt_template("custom.md")
    target.write_text("old template", encoding="utf-8")
    app = Quart(__name__)
    app.add_url_rule("/upload", view_func=star.handle_page_upload_prompt_template, methods=["POST"])

    async def run():
        content = "# New template\n新的扩写提示词"
        kwargs = ({"files": {"file": FileStorage(stream=BytesIO(content.encode("utf-8")), filename="custom.md")}}
                  if multipart else {"json": {"filename": "custom.md", "content": content}})
        response = await app.test_client().post("/upload", **kwargs)
        result = await response.get_json()
        assert result["success"] is True
        assert result["data"]["overwritten"] is True
        assert target.read_text(encoding="utf-8") == content

    asyncio.run(run())
