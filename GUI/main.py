from core.runtime_env import ensure_runtime_compat

ensure_runtime_compat()

from GUI.windows.main_window import run

__all__ = ["run"]


if __name__ == "__main__":
    run()
