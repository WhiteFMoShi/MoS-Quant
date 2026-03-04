from __future__ import annotations

import argparse
import sys


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="main.py")
    parser.add_argument(
        "--headless",
        action="store_true",
        help="在终端运行 loader（不启动 GUI）。",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.headless:
        from mos_quant.core.loader import MoSQuantLoader

        MoSQuantLoader().run(progress_cb=print)
        return 0

    try:
        from mos_quant.ui.qt_app import main as gui_main
    except Exception as exc:
        print(
            "GUI 依赖不可用。\n\n"
            f"- Python: {sys.executable}\n"
            f"- 错误: {type(exc).__name__}: {exc}\n\n"
            "解决方法：\n"
            "- 安装依赖：python3 -m pip install -r requirements.txt\n"
            "- 或仅终端运行：python3 main.py --headless\n",
            file=sys.stderr,
        )
        return 1

    return gui_main()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
