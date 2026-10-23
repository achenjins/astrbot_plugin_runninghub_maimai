"""Offline provenance, path and update-safety checks for the single-source core."""
import copy
import json
from pathlib import Path
import subprocess

import pytest

from scripts import sync_upstream_lib as sync


@pytest.fixture
def originals():
    lock = sync.check()
    files = {name: (sync.ROOT / sync.DESTINATION / name).read_bytes() for name in sync.FILES}
    files["file_source.py"] = files["file_source.py"].replace(sync.IMPORT_AFTER, sync.IMPORT_BEFORE)
    return lock["commit"], files


@pytest.fixture
def installation(tmp_path, originals):
    commit, files = originals
    sync.install(tmp_path, commit, files)
    return tmp_path


def test_repository_core_verifies_offline(monkeypatch):
    monkeypatch.setattr(sync, "git", lambda *args: pytest.fail("offline check invoked Git/network"))
    lock = sync.check()
    assert sync.SHA.fullmatch(lock["commit"])
    assert lock["files"]["file_source.py"]["transforms"] == [sync.TRANSFORM]


def test_local_hand_edit_is_detected_and_not_overwritten(installation, originals):
    target = installation / sync.DESTINATION / "task_queue.py"
    target.write_bytes(target.read_bytes() + b"\n# local edit\n")
    with pytest.raises(sync.SyncError, match="modified"):
        sync.install(installation, *originals)
    assert target.read_bytes().endswith(b"# local edit\n")


def test_original_hash_is_verified_after_reversing_the_only_transform(installation):
    path = installation / sync.LOCK_NAME
    lock = json.loads(path.read_bytes())
    lock["files"]["file_source.py"]["source_sha256"] = "0" * 64
    path.write_text(json.dumps(lock), encoding="utf-8")
    with pytest.raises(sync.SyncError, match="Source hash"):
        sync.check(installation)


@pytest.mark.parametrize("relative", ["../escape", "/outside", "folder/../outside", "a\\b", "a//b"])
def test_managed_paths_reject_traversal(tmp_path, relative):
    with pytest.raises(sync.SyncError):
        sync.managed_path(tmp_path, relative)


def test_directory_symlink_cannot_redirect_core_writes(tmp_path, originals):
    outside = tmp_path / "outside"
    outside.mkdir()
    root = tmp_path / "plugin"
    root.mkdir()
    try:
        (root / "rh_generic_lib").symlink_to(outside, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("Creating a Windows symlink requires additional privileges")
    with pytest.raises(sync.SyncError, match="linked"):
        sync.install(root, *originals)
    assert list(outside.iterdir()) == []


def test_changed_source_import_or_new_host_dependency_requires_review(tmp_path, originals):
    commit, baseline = originals
    for name, data in (("file_source.py", b"# import removed\n"),
                       ("task_queue.py", b"from maibot_sdk import Command\n"),
                       ("media_plan.py", b"from .new_unreviewed_module import helper\n")):
        files = copy.copy(baseline)
        files[name] = data
        with pytest.raises(sync.SyncError):
            sync.install(tmp_path, commit, files)
        assert not (tmp_path / sync.LOCK_NAME).exists()
        assert not (tmp_path / sync.DESTINATION).exists()


def test_existing_unlocked_file_is_never_overwritten(tmp_path, originals):
    path = tmp_path / sync.DESTINATION / "runninghub_client.py"
    path.parent.mkdir(parents=True)
    path.write_text("keep me", encoding="utf-8")
    with pytest.raises(sync.SyncError, match="unlocked"):
        sync.install(tmp_path, *originals)
    assert path.read_text() == "keep me"


def test_import_uses_committed_blobs_and_checks_source_origin(tmp_path, originals):
    _, files = originals
    source = tmp_path / "upstream"
    source.mkdir()
    for name, relative in sync.FILES.items():
        path = source / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(files[name])
    def run(*args):
        return subprocess.run(["git", "-C", str(source), *args], check=True, capture_output=True).stdout
    run("init")
    run("remote", "add", "origin", sync.REPOSITORY)
    run("add", ".")
    run("-c", "user.name=Offline Test", "-c", "user.email=offline@example.test", "commit", "-m", "fixture")
    commit = run("rev-parse", "HEAD").decode().strip()
    (source / "rh_generic_lib" / "runninghub_client.py").write_text("uncommitted changes", encoding="utf-8")
    resolved, restored = sync.read_commit(source, commit)
    assert resolved == commit
    assert restored == files
    run("remote", "set-url", "origin", "https://github.com/other/untrusted.git")
    with pytest.raises(sync.SyncError, match="origin"):
        sync.read_commit(source, commit)


def test_remote_symbolic_ref_is_rejected_before_network(monkeypatch):
    monkeypatch.setattr(sync, "git", lambda *args: pytest.fail("unverified symbolic ref reached Git"))
    with pytest.raises(SystemExit) as caught:
        sync.main(["--ref", "main"])
    assert caught.value.code == 1
