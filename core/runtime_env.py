from __future__ import annotations

import ssl


def _parse_major(version_text: str) -> int:
    raw = str(version_text or "").strip()
    if not raw:
        return 0
    head = raw.split(".", 1)[0]
    try:
        return int(head)
    except Exception:
        return 0


def ensure_runtime_compat() -> None:
    """
    Guard against a known incompatible runtime:
    - LibreSSL + urllib3 v2 may cause unstable HTTPS behavior
      (e.g. Connection aborted / RemoteDisconnected).
    """
    try:
        import urllib3  # type: ignore
    except Exception:
        return

    openssl_text = str(getattr(ssl, "OPENSSL_VERSION", ""))
    if "LibreSSL" not in openssl_text:
        return

    urllib3_version = str(getattr(urllib3, "__version__", ""))
    if _parse_major(urllib3_version) < 2:
        return

    raise RuntimeError(
        "检测到不兼容运行环境: LibreSSL + urllib3>=2。\n"
        f"当前 SSL: {openssl_text}\n"
        f"当前 urllib3: {urllib3_version}\n"
        "这会导致 HTTPS 请求不稳定，可能出现 Connection aborted / RemoteDisconnected。\n"
        "请执行以下命令修复后重试:\n"
        "python3 -m pip install --upgrade 'urllib3<2'\n"
        "或重新安装项目依赖:\n"
        "python3 -m pip install -r requirements.txt"
    )
