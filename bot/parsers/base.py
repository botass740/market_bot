#bot/parsers/base.py

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterable


class BaseParser(ABC):
    @abstractmethod
    async def fetch_products(self) -> Iterable[Any]:
        # TODO: implement fetching product list from platform API/pages
        raise NotImplementedError

    @abstractmethod
    async def parse_product(self, raw: Any) -> dict[str, Any]:
        # TODO: implement conversion from raw platform payload to normalized dict
        raise NotImplementedError
