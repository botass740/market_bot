# bot/db/models/settings.py

from __future__ import annotations

from sqlalchemy import Column, Integer, String, Float, Text
from bot.db.base import Base


class BotSettings(Base):
    """Настройки бота, хранимые в БД."""
    
    __tablename__ = "bot_settings"
    
    id = Column(Integer, primary_key=True)
    key = Column(String(100), unique=True, nullable=False, index=True)
    value = Column(Text, nullable=True)
    
    # Ключи настроек
    KEY_MIN_PRICE = "min_price"
    KEY_MAX_PRICE = "max_price"
    KEY_MIN_DISCOUNT = "min_discount_percent"
    KEY_MIN_PRICE_DROP = "min_price_drop_percent"
    KEY_MIN_DISCOUNT_INCREASE = "min_discount_increase"
    KEY_CATEGORIES = "categories"
    KEY_ADMIN_IDS = "admin_ids"