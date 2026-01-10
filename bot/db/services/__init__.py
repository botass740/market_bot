"""Database services layer."""

from bot.db.services.change_detection import detect_and_save_changes

__all__ = ["detect_and_save_changes"]
