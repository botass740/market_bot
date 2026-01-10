# bot/utils/chrome_manager.py

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import sys
from pathlib import Path

log = logging.getLogger(__name__)

CHROME_PATHS = [
    # Windows
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
    # Linux
    "/usr/bin/google-chrome",
    "/usr/bin/chromium-browser",
    "/usr/bin/chromium",
    # Mac
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
]

CDP_PORT = int(os.getenv("OZON_CDP_PORT", "9222"))
CHROME_USER_DATA_DIR = Path(os.getenv("OZON_CHROME_PROFILE_DIR", str(Path.home() / ".ozon_parser_chrome")))

# На Windows полезно не плодить процессы: если уже запущен — просто используем
STARTUP_TIMEOUT_SEC = int(os.getenv("OZON_CHROME_STARTUP_TIMEOUT_SEC", "30"))


class ChromeManager:
    """
    Управление Chrome для парсинга Ozon.

    Chrome живёт как внешний сервис и слушает CDP-порт.
    Мы его запускаем при необходимости, но не управляем завершением (stop = no-op).
    """

    def __init__(self) -> None:
        self._chrome_path: str | None = None
        self._starting_lock = asyncio.Lock()

    def _find_chrome(self) -> str | None:
        for path in CHROME_PATHS:
            expanded = os.path.expandvars(path)
            if os.path.exists(expanded):
                return expanded
        return None

    async def start(self) -> bool:
        # Защита от параллельного запуска из нескольких корутин
        async with self._starting_lock:
            if await self.is_running():
                log.info("Chrome already running on port %d", CDP_PORT)
                return True

            self._chrome_path = self._find_chrome()
            if not self._chrome_path:
                log.error("Chrome not found. Install Google Chrome.")
                return False

            log.info("Found Chrome: %s", self._chrome_path)

            CHROME_USER_DATA_DIR.mkdir(parents=True, exist_ok=True)

            args = [
                self._chrome_path,
                f"--remote-debugging-port={CDP_PORT}",
                f"--user-data-dir={CHROME_USER_DATA_DIR}",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-background-networking",
                "--disable-client-side-phishing-detection",
                "--disable-default-apps",
                "--disable-hang-monitor",
                "--disable-popup-blocking",
                "--disable-prompt-on-repost",
                "--disable-sync",
                "--disable-translate",
                "--metrics-recording-only",
                "--safebrowsing-disable-auto-update",
                "https://www.ozon.ru/",
            ]

            # Headless может быть заблокирован Ozon, включайте осознанно
            if os.getenv("CHROME_HEADLESS", "false").lower() in ("true", "1", "yes"):
                args.insert(1, "--headless=new")

            try:
                popen_kwargs = {
                    "stdout": subprocess.DEVNULL,
                    "stderr": subprocess.DEVNULL,
                    # важно: не наследовать лишние дескрипторы
                    "close_fds": True if sys.platform != "win32" else False,
                }

                if sys.platform == "win32":
                    # Скрываем окно + отключаем наследование handles (часто помогает от warning)
                    startupinfo = subprocess.STARTUPINFO()
                    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW

                    creationflags = subprocess.CREATE_NO_WINDOW | subprocess.DETACHED_PROCESS

                    subprocess.Popen(
                        args,
                        startupinfo=startupinfo,
                        creationflags=creationflags,
                        **popen_kwargs,
                    )
                else:
                    subprocess.Popen(args, **popen_kwargs)

                # Ждём готовности CDP
                for _ in range(STARTUP_TIMEOUT_SEC):
                    await asyncio.sleep(1)
                    if await self.is_running():
                        log.info("Chrome is ready for CDP")
                        await asyncio.sleep(1)  # небольшой прогрев
                        return True

                log.error("Chrome did not become available on CDP port in time")
                return False

            except Exception as e:
                log.error("Failed to start Chrome: %s", e)
                return False

    async def is_running(self) -> bool:
        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"http://localhost:{CDP_PORT}/json/version",
                    timeout=aiohttp.ClientTimeout(total=2),
                ) as resp:
                    return resp.status == 200
        except Exception:
            return False

    async def stop(self) -> None:
        """
        No-op: Chrome runs as an external service.
        If you need to stop it — do it via OS task manager / systemd / supervisor.
        """
        return

    async def ensure_running(self) -> bool:
        if await self.is_running():
            return True
        return await self.start()


_chrome_manager: ChromeManager | None = None


async def get_chrome_manager() -> ChromeManager:
    global _chrome_manager
    if _chrome_manager is None:
        _chrome_manager = ChromeManager()
    return _chrome_manager


async def ensure_chrome_running() -> bool:
    manager = await get_chrome_manager()
    return await manager.ensure_running()


async def stop_chrome() -> None:
    global _chrome_manager
    if _chrome_manager:
        await _chrome_manager.stop()
        _chrome_manager = None