"""External marketplace parsers (placeholders)."""

from bot.parsers.base import BaseParser
from bot.parsers.detmir import DetmirParser
from bot.parsers.ozon import OzonParser
from bot.parsers.wb import WildberriesParser

__all__ = [
    "BaseParser",
    "DetmirParser",
    "OzonParser",
    "WildberriesParser",
]
