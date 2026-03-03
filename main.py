from core.runtime_env import ensure_runtime_compat

ensure_runtime_compat()

from GUI.main import run


if __name__ == "__main__":
    run()
