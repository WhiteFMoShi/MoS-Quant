from __future__ import annotations

import argparse
import sys


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="mos_quant")
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run loader in terminal mode (no GUI).",
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
            "GUI dependencies are not available.\n\n"
            f"- Python: {sys.executable}\n"
            f"- Error: {type(exc).__name__}: {exc}\n\n"
            "Fix:\n"
            "- Install deps: python3 -m pip install -r requirements.txt\n"
            "- Or run headless: python3 -m mos_quant --headless\n",
            file=sys.stderr,
        )
        return 1

    return gui_main()


if __name__ == "__main__":
    raise SystemExit(main())
