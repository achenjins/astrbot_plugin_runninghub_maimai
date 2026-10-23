"""Reproduce the framework-neutral RunningHub core from a verified upstream commit.

Examples:
    python scripts/sync_upstream_lib.py --check
    python scripts/sync_upstream_lib.py --source-dir ../runninghub-workflow-adapter --ref <40-char SHA>
    python scripts/sync_upstream_lib.py --ref <40-char SHA>

Only this allowlist is managed. AstrBot adapters remain outside upstream/.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = "https://github.com/achenjins/runninghub-workflow-adapter.git"
LOCK_NAME = "upstream-core.lock.json"
DESTINATION = "rh_generic_lib/upstream"
MODULES = (
    "__init__.py", "background_io.py", "runninghub_client.py", "workflow_runner.py",
    "workflow_presets.py", "media_plan.py", "task_queue.py", "image_memory.py",
    "task_journal.py", "file_source.py", "avatar_source.py", "delivery.py", "session_machine.py",
)
FILES = {name: f"rh_generic_lib/{name}" for name in MODULES} | {"LICENSE": "LICENSE"}
IMPORT_BEFORE = b"from rh_generic_lib.runninghub_client import RunningHubError"
IMPORT_AFTER = b"from .runninghub_client import RunningHubError"
TRANSFORM = "file-source-relative-client-import"
SHA = re.compile(r"[0-9a-f]{40}\Z")
DIGEST = re.compile(r"[0-9a-f]{64}\Z")


class SyncError(RuntimeError):
    pass


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def managed_path(root: Path, relative: str) -> Path:
    """Reject symlinks in every existing component before reading or replacing it."""
    if not relative or "\\" in relative:
        raise SyncError("Invalid managed path")
    parts = relative.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise SyncError("Invalid managed path")
    root = root.resolve(strict=True)
    result = root
    for part in parts:
        result = result / part
        if result.is_symlink() or (hasattr(result, "is_junction") and result.is_junction()):
            raise SyncError(f"Refusing linked managed path: {relative}")
    if not result.resolve(strict=False).is_relative_to(root):
        raise SyncError(f"Managed path escapes repository: {relative}")
    return result


def transform(name: str, data: bytes) -> tuple[bytes, list[str]]:
    if name != "file_source.py":
        return data, []
    if data.count(IMPORT_BEFORE) != 1 or IMPORT_AFTER in data:
        raise SyncError("Upstream file_source import changed; review the recorded transform")
    return data.replace(IMPORT_BEFORE, IMPORT_AFTER), [TRANSFORM]


def validate_python(files: dict[str, bytes]) -> None:
    modules = {Path(name).stem for name in MODULES}
    for name, content in files.items():
        if not name.endswith(".py"):
            continue
        try:
            tree = ast.parse(content, filename=name)
        except (SyntaxError, ValueError) as exc:
            raise SyncError(f"Invalid upstream Python: {name}") from exc
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.level and (node.level != 1 or (node.module or "").split(".")[0] not in modules):
                    raise SyncError(f"New core dependency requires review: {name}: {node.module}")
                imported = node.module or ""
            elif isinstance(node, ast.Import):
                imported = " ".join(alias.name for alias in node.names)
            else:
                continue
            if any(token in imported for token in ("maibot_sdk", "astrbot", "rh_generic_lib")):
                raise SyncError(f"Core must not import a host SDK or top-level plugin: {name}")


def check(root: Path = ROOT) -> dict[str, Any]:
    path = managed_path(root, LOCK_NAME)
    try:
        lock = json.loads(path.read_bytes())
    except (OSError, ValueError) as exc:
        raise SyncError("Missing or invalid upstream lock file") from exc
    if (not isinstance(lock, dict) or lock.get("format_version") != 1
            or lock.get("repository") != REPOSITORY or not SHA.fullmatch(str(lock.get("commit", "")))
            or lock.get("destination") != DESTINATION or not isinstance(lock.get("files"), dict)
            or set(lock["files"]) != set(FILES)):
        raise SyncError("Upstream lock metadata does not match the managed allowlist")
    installed: dict[str, bytes] = {}
    for name, source in FILES.items():
        record = lock["files"][name]
        if (not isinstance(record, dict) or record.get("source") != source
                or not DIGEST.fullmatch(str(record.get("sha256", "")))
                or not DIGEST.fullmatch(str(record.get("source_sha256", "")))):
            raise SyncError(f"Invalid locked entry: {name}")
        path = managed_path(root, f"{DESTINATION}/{name}")
        if not path.is_file():
            raise SyncError(f"Missing vendored core file: {name}")
        content = path.read_bytes()
        if digest(content) != record["sha256"]:
            raise SyncError(f"Vendored core was modified: {name}; change the upstream project instead")
        expected_transforms = [TRANSFORM] if name == "file_source.py" else []
        if record.get("transforms") != expected_transforms:
            raise SyncError(f"Unexpected transform policy: {name}")
        original = content
        if expected_transforms:
            if content.count(IMPORT_AFTER) != 1 or IMPORT_BEFORE in content:
                raise SyncError("Vendored file_source import does not match the recorded transform")
            original = content.replace(IMPORT_AFTER, IMPORT_BEFORE)
        if digest(original) != record["source_sha256"]:
            raise SyncError(f"Source hash does not match the recorded transform: {name}")
        installed[name] = content
    directory = managed_path(root, DESTINATION)
    extras = {p.name for p in directory.iterdir() if p.name not in FILES and p.name != "__pycache__"}
    if extras:
        raise SyncError(f"Unmanaged files in vendored core: {', '.join(sorted(extras))}")
    validate_python(installed)
    return lock


def git(directory: Path, *args: str) -> bytes:
    command = ["git", "-c", f"safe.directory={directory.resolve().as_posix()}", "-C", str(directory), *args]
    try:
        return subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              timeout=120).stdout
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise SyncError(f"Git failed during upstream verification: {' '.join(args[:2])}") from exc


def verify_origin(directory: Path) -> None:
    origin = git(directory, "remote", "get-url", "origin").decode().strip().rstrip("/")
    allowed = {REPOSITORY, REPOSITORY.removesuffix(".git"),
               "git@github.com:achenjins/runninghub-workflow-adapter.git",
               "ssh://git@github.com/achenjins/runninghub-workflow-adapter.git"}
    if origin not in allowed:
        raise SyncError("Local source origin is not the expected RunningHub upstream repository")


def read_commit(directory: Path, reference: str) -> tuple[str, dict[str, bytes]]:
    verify_origin(directory)
    commit = git(directory, "rev-parse", "--verify", f"{reference}^{{commit}}").decode().strip()
    if not SHA.fullmatch(commit) or (reference != "HEAD" and reference != commit):
        raise SyncError("Use the complete 40-character commit SHA")
    files = {}
    for name, source in FILES.items():
        entry = git(directory, "ls-tree", commit, "--", source).decode().strip()
        parts = entry.split(None, 3)
        if len(parts) != 4 or parts[0] not in {"100644", "100755"} or parts[1] != "blob" or parts[3] != source:
            raise SyncError(f"Upstream tracked regular file not found: {source}")
        data = git(directory, "cat-file", "blob", parts[2])
        if len(data) > 4 * 1024 * 1024:
            raise SyncError(f"Unexpectedly large core file: {source}")
        files[name] = data
    return commit, files


def install(root: Path, commit: str, originals: dict[str, bytes]) -> None:
    if not SHA.fullmatch(commit) or set(originals) != set(FILES):
        raise SyncError("Invalid source snapshot")
    installed: dict[str, bytes] = {}
    entries = {}
    for name, data in originals.items():
        content, changes = transform(name, data)
        installed[name] = content
        entries[name] = {"source": FILES[name], "source_sha256": digest(data),
                         "sha256": digest(content), "transforms": changes}
    validate_python(installed)
    lock = {"format_version": 1, "repository": REPOSITORY, "commit": commit,
            "destination": DESTINATION, "files": entries}
    replacements = {f"{DESTINATION}/{name}": data for name, data in installed.items()}
    replacements[LOCK_NAME] = (json.dumps(lock, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    # Validate every destination before writing anything, and retain bytes for rollback.
    previous: dict[str, bytes | None] = {}
    for relative in replacements:
        path = managed_path(root, relative)
        if path.exists() and not path.is_file():
            raise SyncError(f"Managed destination is not a regular file: {relative}")
        previous[relative] = path.read_bytes() if path.exists() else None
    if previous[LOCK_NAME] is not None:
        check(root)
    elif any(data is not None for key, data in previous.items() if key != LOCK_NAME):
        raise SyncError("Refusing to overwrite an unlocked vendor directory")
    written: list[str] = []
    try:
        for relative, content in replacements.items():
            path = managed_path(root, relative)
            path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".core-sync-", delete=False) as handle:
                temporary = Path(handle.name)
                try:
                    handle.write(content)
                    handle.flush()
                    os.fsync(handle.fileno())
                except BaseException:
                    temporary.unlink(missing_ok=True)
                    raise
            try:
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
            written.append(relative)
    except BaseException:
        for relative in reversed(written):
            path = managed_path(root, relative)
            if previous[relative] is None:
                path.unlink(missing_ok=True)
            else:
                path.write_bytes(previous[relative])
        raise
    check(root)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Validate installed core and lock offline")
    parser.add_argument("--source-dir", type=Path, help="Read tracked blobs from a local upstream checkout")
    parser.add_argument("--ref", help="Complete upstream commit SHA (local source defaults to HEAD)")
    options = parser.parse_args(argv)
    try:
        if options.check:
            if options.source_dir or options.ref:
                raise SyncError("--check is offline and cannot be combined with source/ref options")
            lock = check()
            print(f"Core verified: {lock['commit']} ({len(FILES)} files)")
            return 0
        if options.source_dir:
            commit, files = read_commit(options.source_dir.resolve(strict=True), options.ref or "HEAD")
        else:
            if not SHA.fullmatch(options.ref or ""):
                raise SyncError("Remote updates require --ref with a complete upstream commit SHA")
            with tempfile.TemporaryDirectory(prefix="runninghub-core-") as temporary:
                directory = Path(temporary)
                git(directory, "init", "--bare")
                git(directory, "remote", "add", "origin", REPOSITORY)
                git(directory, "fetch", "--no-tags", "--depth=1", "origin", options.ref)
                commit, files = read_commit(directory, options.ref)
        install(ROOT, commit, files)
        print(f"Core synchronized: {commit} ({len(FILES)} files)")
        return 0
    except (SyncError, OSError) as exc:
        parser.exit(1, f"Core sync failed: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
