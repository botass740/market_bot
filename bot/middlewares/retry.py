# bot/middlewares/retry.py
"""
Middleware для retry при сетевых ошибках на Windows.
"""

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message
from aiogram.exceptions import TelegramNetworkError

log = logging.getLogger(__name__)

# Настройки retry
RETRY_ATTEMPTS = 3
RETRY_DELAY = 1.0  # секунды


class RetryMiddleware(BaseMiddleware):
    """
    Middleware для повторных попыток при сетевых ошибках.
    Особенно полезно на Windows с WinError 121.
    """

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        last_error = None
        
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                return await handler(event, data)
            except TelegramNetworkError as e:
                last_error = e
                error_msg = str(e)
                
                # WinError 121 — таймаут семафора на Windows
                if "WinError 121" in error_msg or "семафора" in error_msg:
                    if attempt < RETRY_ATTEMPTS:
                        log.warning(
                            "WinError 121 (attempt %d/%d), retrying in %.1fs...",
                            attempt, RETRY_ATTEMPTS, RETRY_DELAY
                        )
                        await asyncio.sleep(RETRY_DELAY)
                        continue
                    else:
                        log.error("WinError 121: all %d attempts failed", RETRY_ATTEMPTS)
                        raise
                else:
                    # Другие сетевые ошибки — пробрасываем сразу
                    raise
        
        # Если все попытки исчерпаны
        if last_error:
            raise last_error