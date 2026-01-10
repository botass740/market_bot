# bot/config.py

from typing import Annotated
import os

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from dotenv import load_dotenv

load_dotenv()


class ParsingIntervals(BaseModel):
    wb_seconds: int = Field(default=300)
    ozon_seconds: int = Field(default=300)
    detmir_seconds: int = Field(default=300)


class PublishingLimits(BaseModel):
    max_posts_per_run: int = Field(default=20)
    max_posts_per_day: int = Field(default=200)


class PostingSettings(BaseModel):
    channel: str = Field(default="")
    max_posts_per_hour: int = Field(default=50)


class FilteringThresholds(BaseModel):
    min_price: float = Field(default=0.0)
    max_price: float = Field(default=0.0)
    min_stock: int = Field(default=0)
    min_discount_percent: float = Field(default=0.0)
    categories: list[str] = Field(default_factory=list)
    
    # Пороги для публикации
    min_price_drop_percent: float = Field(default=1.0)
    min_discount_increase: float = Field(default=5.0)

    @field_validator("categories", mode="before")
    @classmethod
    def _parse_categories(cls, v):
        if v is None:
            return []
        if isinstance(v, list):
            return [str(x).strip() for x in v if str(x).strip()]
        s = str(v).strip()
        if not s:
            return []
        return [part.strip() for part in s.split(",") if part.strip()]


class Settings(BaseSettings):
    bot_token: str = Field(validation_alias="BOT_TOKEN")
    postgres_dsn: str = Field(validation_alias="DATABASE_DSN")

    wb_nm_ids: Annotated[list[int], NoDecode] = Field(default_factory=list, validation_alias="WB_NM_IDS")

    @field_validator("wb_nm_ids", mode="before")
    @classmethod
    def _parse_wb_nm_ids(cls, v):
        if v is None:
            return []
        if isinstance(v, list):
            result: list[int] = []
            for x in v:
                try:
                    result.append(int(x))
                except (TypeError, ValueError):
                    continue
            return result
        s = str(v).strip()
        if not s:
            return []
        result: list[int] = []
        for part in s.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                result.append(int(part))
            except ValueError:
                continue
        return result

    parsing: ParsingIntervals = Field(default_factory=ParsingIntervals)
    publishing: PublishingLimits = Field(default_factory=PublishingLimits)
    posting: PostingSettings = Field(default_factory=PostingSettings)
    filtering: FilteringThresholds = Field(default_factory=FilteringThresholds)
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="",
        extra="ignore",
    )
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # Вручную читаем вложенные настройки из ENV
        self._load_nested_settings()
    
    def _load_nested_settings(self):
        """Загружает вложенные настройки из переменных окружения."""
        
        # Parsing intervals
        if val := os.getenv("PARSING_WB_SECONDS"):
            self.parsing.wb_seconds = int(val)
        if val := os.getenv("PARSING_OZON_SECONDS"):
            self.parsing.ozon_seconds = int(val)
        if val := os.getenv("PARSING_DETMIR_SECONDS"):
            self.parsing.detmir_seconds = int(val)
        
        # Publishing limits
        if val := os.getenv("PUBLISHING_MAX_POSTS_PER_RUN"):
            self.publishing.max_posts_per_run = int(val)
        if val := os.getenv("PUBLISHING_MAX_POSTS_PER_DAY"):
            self.publishing.max_posts_per_day = int(val)
        
        # Posting settings
        if val := os.getenv("POSTING_CHANNEL"):
            self.posting.channel = val
        if val := os.getenv("POSTING_MAX_POSTS_PER_HOUR"):
            self.posting.max_posts_per_hour = int(val)
        
        # Filtering thresholds
        if val := os.getenv("FILTER_MIN_PRICE"):
            self.filtering.min_price = float(val)
        if val := os.getenv("FILTER_MAX_PRICE"):
            self.filtering.max_price = float(val)
        if val := os.getenv("FILTER_MIN_STOCK"):
            self.filtering.min_stock = int(val)
        if val := os.getenv("FILTER_MIN_DISCOUNT_PERCENT"):
            self.filtering.min_discount_percent = float(val)
        if val := os.getenv("FILTER_CATEGORIES"):
            self.filtering.categories = [c.strip() for c in val.split(",") if c.strip()]
        
        # Пороги публикации
        if val := os.getenv("MIN_PRICE_DROP_PERCENT"):
            self.filtering.min_price_drop_percent = float(val)
        if val := os.getenv("MIN_DISCOUNT_INCREASE"):
            self.filtering.min_discount_increase = float(val)


def load_settings() -> Settings:
    return Settings()