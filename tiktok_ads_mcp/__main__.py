#!/usr/bin/env python3
"""Entry point for running tiktok_ads_mcp as a module"""

from .main import cli


def _start_orphan_watchdog():
    # Exit if parent (Claude Code) dies — prevents stdio MCP process leak.
    # macOS lacks PDEATHSIG; poll getppid. See memory/mcp_session_process_leak.
    import os
    import threading
    import time

    _ppid = os.getppid()

    def _watch():
        while os.getppid() == _ppid:
            time.sleep(5)
        os._exit(0)

    threading.Thread(target=_watch, daemon=True).start()


if __name__ == "__main__":
    _start_orphan_watchdog()
    cli()
