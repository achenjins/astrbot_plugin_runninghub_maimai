"""maibot config.toml → AstrBot 插件配置 JSON 转换工具。

用法（在插件目录下执行）：
    python tools/convert_config.py path/to/config.toml [output.json]

不带 output 时输出到 config.toml 同目录的 astrbot_config.json。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parents[1]
if str(PLUGIN_DIR) not in sys.path:
    sys.path.insert(0, str(PLUGIN_DIR))

from rh_generic_lib.legacy_config import convert_toml_file  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="转换 maibot config.toml 为 AstrBot 插件配置")
    parser.add_argument("source", help="旧 config.toml 路径")
    parser.add_argument("destination", nargs="?", help="输出 JSON 路径（可选）")
    args = parser.parse_args()

    source = Path(args.source)
    if not source.is_file():
        print(f"找不到配置文件: {source}")
        return 2

    destination = Path(args.destination) if args.destination else source.with_name("astrbot_config.json")
    converted = convert_toml_file(source, destination)
    print(
        f"转换成功：{len(converted.get('workflows') or [])} 个工作流 -> {destination}"
    )
    print("如果 AstrBot 已启动，建议把本 JSON 内容粘贴到插件 WebUI 配置中；")
    print("或者放到 AstrBot 的 data/config/astrobt-runninghub_config.json 后重启 AstrBot。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
