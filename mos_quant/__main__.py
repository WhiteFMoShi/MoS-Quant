from __future__ import annotations

def main() -> int:
    from mos_quant.ui.qt_app import main as gui_main

    return gui_main()


if __name__ == "__main__":
    raise SystemExit(main())
