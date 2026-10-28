"""Concurrent recognized workflows must merge against the latest saved config."""
import asyncio
import threading

from .test_plugin import ctx, star  # noqa: F401


async def concurrent_appends(star, monkeypatch, second_name):
    entered, release = threading.Event(), threading.Event()
    save = star._astrbot_config.save_config

    def blocked_save(data):
        entered.set()
        assert release.wait(10)
        save(data)

    # AstrBotConfig.__setattr__ writes a config key, so patch the class method.
    monkeypatch.setattr(type(star._astrbot_config), "save_config", lambda config, data: blocked_save(data))
    first = asyncio.create_task(star._append_workflow_to_config(
        workflow_name="first", workflow_id="1", nodes=[{"node_id": "10", "value_type": "prompt"}]))
    second = None
    try:
        assert await asyncio.to_thread(entered.wait, 8)
        second = asyncio.create_task(star._append_workflow_to_config(
            workflow_name=second_name, workflow_id="2", nodes=[{"node_id": "20", "value_type": "prompt"}]))
        await asyncio.sleep(0)
    finally:
        release.set()
    return await asyncio.gather(first, second, return_exceptions=True)


def test_concurrent_different_workflow_appends_preserve_both_configs_and_nodes(star, monkeypatch):
    outcomes = asyncio.run(concurrent_appends(star, monkeypatch, "second"))
    assert outcomes == [None, None]
    assert star._workflow_names() == ["first", "second"]
    assert [(item["name"], item["workflow_id"]) for item in star._astrbot_config["workflows"]] == [
        ("first", "1"), ("second", "2")]
    assert [(item["workflow_name"], item["node_id"]) for item in star._astrbot_config["workflow_nodes"]] == [
        ("first", "10"), ("second", "20")]


def test_concurrent_duplicate_workflow_name_is_rejected_without_overwriting_first(star, monkeypatch):
    outcomes = asyncio.run(concurrent_appends(star, monkeypatch, "first"))
    assert outcomes[0] is None
    assert isinstance(outcomes[1], ValueError) and "同名工作流" in str(outcomes[1])
    assert star._workflow_names() == ["first"]
    assert star._astrbot_config["workflows"][0]["workflow_id"] == "1"
    assert star._astrbot_config["workflow_nodes"][0]["node_id"] == "10"
