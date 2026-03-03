"""PySide6 GUI (optional)."""


def main() -> int:
    from .qt_app import main as gui_main

    return gui_main()


__all__ = ["main"]
