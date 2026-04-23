"""
Configuration module for Telegram Queue Bot.
Loads environment variables and defines bot settings.
"""

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv


# Load local .env for development without overriding Railway-provided vars.
load_dotenv(override=False)


@dataclass
class BotConfig:
    """Bot configuration loaded from environment variables."""

    # Telegram
    bot_token: str

    # MongoDB
    mongo_uri: str

    # Channels
    intake_channel_id: str
    storage_channel_id: str

    # Admin
    admin_id: str

    # Settings
    delete_intake_messages: bool = False  # Default: keep messages
    timeout_hours: int = 24  # Alert after X hours waiting

    # Database names
    db_name: str = "queue_bot"


def load_config() -> BotConfig:
    """
    Load configuration from environment variables.
    Raises ValueError if required variables are missing.
    """
    required_vars = [
        "BOT_TOKEN",
        "MONGO_URI",
        "INTAKE_CHANNEL_ID",
        "STORAGE_CHANNEL_ID",
        "ADMIN_ID"
    ]

    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    return BotConfig(
        bot_token=os.getenv("BOT_TOKEN"),
        mongo_uri=os.getenv("MONGO_URI"),
        intake_channel_id=os.getenv("INTAKE_CHANNEL_ID"),
        storage_channel_id=os.getenv("STORAGE_CHANNEL_ID"),
        admin_id=os.getenv("ADMIN_ID"),
        delete_intake_messages=os.getenv("DELETE_INTAKE_MESSAGES", "false").lower() == "true",
        timeout_hours=int(os.getenv("TIMEOUT_HOURS", "24")),
        db_name=os.getenv("DB_NAME", "queue_bot")
    )


# Global config instance
config: Optional[BotConfig] = None


def get_config() -> BotConfig:
    """Get the global config instance, loading if necessary."""
    global config
    if config is None:
        config = load_config()
    return config
