# bot/services/settings_manager.py

from __future__ import annotations

import os
import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from bot.db.models.settings import BotSettings

log = logging.getLogger(__name__)


class SettingsManager:
    """Управление настройками бота через БД."""
    
    # Значения по умолчанию
    DEFAULTS = {
        BotSettings.KEY_MIN_PRICE: "0",
        BotSettings.KEY_MAX_PRICE: "0",
        BotSettings.KEY_MIN_DISCOUNT: "0",
        BotSettings.KEY_MIN_PRICE_DROP: "10.0",
        BotSettings.KEY_MIN_DISCOUNT_INCREASE: "20.0",
        BotSettings.KEY_CATEGORIES: "смартфон,ноутбук,наушники,планшет,телевизор,платье,кроссовки,футболка,джинсы,куртка,сумка,часы,парфюм,косметика,пылесос,микроволновка,чайник,холодильник,видеорегистратор,автокресло,автомагнитола",
        BotSettings.KEY_ADMIN_IDS: "",
        BotSettings.KEY_DETMIR_SLUGS: "igry_i_igrushki,children_clothes,obuv,hygiene_care,diapers_pants,bottles_cups,hobbies_creativity,knigy,dom,childrens_room,kolyaski,avtokresla,konstruktory,myagkie_igrushki,nastolnye_igry,radioupravlyaemye_igrushki,all_mom,bady_pravilnoe_i_sportivnoe_pitanie",
    }
    
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._cache: dict[str, str] = {}
    
    async def get(self, key: str) -> str:
        """Получает значение настройки."""
        # Сначала проверяем кэш
        if key in self._cache:
            return self._cache[key]
        
        async with self._session_factory() as session:
            stmt = select(BotSettings).where(BotSettings.key == key)
            result = await session.execute(stmt)
            setting = result.scalar_one_or_none()
            
            if setting and setting.value is not None:
                self._cache[key] = setting.value
                return setting.value
        
        # Возвращаем значение по умолчанию
        default = self.DEFAULTS.get(key, "")
        
        # Для некоторых ключей проверяем ENV
        env_mapping = {
            BotSettings.KEY_MIN_PRICE: "FILTER_MIN_PRICE",
            BotSettings.KEY_MAX_PRICE: "FILTER_MAX_PRICE",
            BotSettings.KEY_MIN_DISCOUNT: "FILTER_MIN_DISCOUNT_PERCENT",
            BotSettings.KEY_MIN_PRICE_DROP: "MIN_PRICE_DROP_PERCENT",
            BotSettings.KEY_MIN_DISCOUNT_INCREASE: "MIN_DISCOUNT_INCREASE",
            BotSettings.KEY_CATEGORIES: "WB_CATEGORIES",
            BotSettings.KEY_ADMIN_IDS: "ADMIN_IDS",
        }
        
        if key in env_mapping:
            env_value = os.getenv(env_mapping[key], "").strip()
            if env_value:
                return env_value
        
        return default
    
    async def set(self, key: str, value: str) -> None:
        """Устанавливает значение настройки."""
        async with self._session_factory() as session:
            stmt = select(BotSettings).where(BotSettings.key == key)
            result = await session.execute(stmt)
            setting = result.scalar_one_or_none()
            
            if setting:
                setting.value = value
            else:
                setting = BotSettings(key=key, value=value)
                session.add(setting)
            
            await session.commit()
        
        # Обновляем кэш
        self._cache[key] = value
        log.info(f"Setting updated: {key} = {value}")
    
    async def get_float(self, key: str) -> float:
        """Получает числовое значение."""
        value = await self.get(key)
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    async def get_int(self, key: str) -> int:
        """Получает целочисленное значение."""
        value = await self.get(key)
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return 0
    
    async def get_list(self, key: str) -> list[str]:
        """Получает список (разделитель — запятая)."""
        value = await self.get(key)
        if not value:
            return []
        return [item.strip() for item in value.split(",") if item.strip()]
    
    async def add_to_list(self, key: str, item: str) -> list[str]:
        """Добавляет элемент в список."""
        items = await self.get_list(key)
        item = item.strip()
        if item and item not in items:
            items.append(item)
            await self.set(key, ",".join(items))
        return items
    
    async def remove_from_list(self, key: str, item: str) -> list[str]:
        """Удаляет элемент из списка."""
        items = await self.get_list(key)
        item = item.strip()
        if item in items:
            items.remove(item)
            await self.set(key, ",".join(items))
        return items
    
    async def get_admin_ids(self) -> list[int]:
        """Получает список ID администраторов."""
        value = await self.get(BotSettings.KEY_ADMIN_IDS)
        if not value:
            return []
        
        ids = []
        for item in value.split(","):
            try:
                ids.append(int(item.strip()))
            except ValueError:
                pass
        return ids
    
    async def is_admin(self, user_id: int) -> bool:
        """Проверяет, является ли пользователь админом."""
        admin_ids = await self.get_admin_ids()
        
        # Если админов нет — первый пользователь становится админом
        if not admin_ids:
            return True
        
        return user_id in admin_ids
    
    async def add_admin(self, user_id: int) -> None:
        """Добавляет администратора."""
        await self.add_to_list(BotSettings.KEY_ADMIN_IDS, str(user_id))
    
    async def get_all_settings(self) -> dict[str, Any]:
        """Получает все настройки для отображения."""
        return {
            "min_price": await self.get_float(BotSettings.KEY_MIN_PRICE),
            "max_price": await self.get_float(BotSettings.KEY_MAX_PRICE),
            "min_discount": await self.get_float(BotSettings.KEY_MIN_DISCOUNT),
            "min_price_drop": await self.get_float(BotSettings.KEY_MIN_PRICE_DROP),
            "min_discount_increase": await self.get_float(BotSettings.KEY_MIN_DISCOUNT_INCREASE),
            "categories": await self.get_list(BotSettings.KEY_CATEGORIES),
            "admin_ids": await self.get_admin_ids(),
        }
    
    def clear_cache(self) -> None:
        """Очищает кэш настроек."""
        self._cache.clear()