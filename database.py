"""
MongoDB database module for Queue Bot.
Manages posts collection and current state tracking.
"""

import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from urllib.parse import parse_qsl, urlsplit

import certifi
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import PyMongoError

from config import BotConfig

logger = logging.getLogger(__name__)


class DatabaseConnectionError(RuntimeError):
    """Raised when the bot cannot connect to MongoDB at startup."""


class DatabaseManager:
    """
    Manages MongoDB operations for the queue bot.
    Collections:
    - posts: stores individual posts with status
    - state: stores current bot state (single document)
    """

    def __init__(self, config: BotConfig):
        client_options = self._build_client_options(config.mongo_uri)
        self.client = MongoClient(config.mongo_uri, **client_options)
        self.db: Database = self.client[config.db_name]
        self.posts: Collection = self.db["posts"]
        self.state: Collection = self.db["state"]

        try:
            self.client.admin.command("ping")
            logger.info("MongoDB connection established successfully")
            self._ensure_indexes()
        except PyMongoError as exc:
            self.close()
            raise DatabaseConnectionError(
                self._format_connection_error(config.mongo_uri, exc)
            ) from exc

    def _build_client_options(self, mongo_uri: str) -> Dict[str, Any]:
        """Build safe MongoClient options for Atlas and Railway deployments."""
        uri_options = {
            key.lower(): value
            for key, value in parse_qsl(urlsplit(mongo_uri).query, keep_blank_values=True)
        }

        is_srv_uri = mongo_uri.startswith("mongodb+srv://")
        is_atlas_uri = "mongodb.net" in mongo_uri.lower()
        tls_enabled = is_srv_uri or uri_options.get("tls", "").lower() == "true" or uri_options.get("ssl", "").lower() == "true"

        client_options: Dict[str, Any] = {
            "serverSelectionTimeoutMS": 30000,
            "connectTimeoutMS": 20000,
            "socketTimeoutMS": 20000,
            "appname": "telegram-queue-bot",
        }

        if is_atlas_uri and "tls" not in uri_options and "ssl" not in uri_options:
            client_options["tls"] = True
            tls_enabled = True

        if tls_enabled and "tlscafile" not in uri_options and "ssl_ca_certs" not in uri_options:
            client_options["tlsCAFile"] = certifi.where()
            logger.info("Using certifi CA bundle for MongoDB TLS")

        return client_options

    def _format_connection_error(self, mongo_uri: str, exc: Exception) -> str:
        """Return an actionable startup error message for MongoDB failures."""
        exc_text = str(exc)
        lowered = exc_text.lower()

        if "ssl handshake failed" in lowered or "certificate_verify_failed" in lowered or "tlsv1" in lowered:
            return (
                "MongoDB TLS handshake failed. The bot is using certifi for CA validation, "
                "so if this still fails, verify that MONGO_URI is the exact Atlas/Railway URI "
                "and that your MongoDB provider allows connections from Railway. "
                f"Original error: {exc_text}"
            )

        if "mongodb.net" in mongo_uri.lower():
            return (
                "Could not connect to MongoDB Atlas. Check that MONGO_URI is copied exactly "
                "from Atlas (prefer the mongodb+srv URI) and that network access is allowed. "
                f"Original error: {exc_text}"
            )

        return f"Could not connect to MongoDB. Original error: {exc_text}"

    def _ensure_indexes(self):
        """Create indexes for efficient queries."""
        # Index for finding queued posts by creation time
        self.posts.create_index([("created_at", 1)])
        # Index for status queries
        self.posts.create_index([("status", 1)])
        # Unique index on message_id to prevent duplicates
        self.posts.create_index([("intake_message_ids", 1)], unique=True, sparse=True)

    # ==================== POST OPERATIONS ====================

    def add_post(self, intake_message_ids: List[int],
                 media_group_id: Optional[str],
                 status: str = "queued",
                 storage_message_ids: Optional[List[int]] = None) -> str:
        """
        Add a new post to the queue.

        Args:
            intake_message_ids: List of message IDs from intake channel
            media_group_id: Media group ID if applicable (for grouping)
            status: Initial status (default: queued)
            storage_message_ids: List of message IDs sent to Storage Channel

        Returns:
            Inserted document ID
        """
        now = datetime.now(timezone.utc)
        doc = {
            "intake_message_ids": intake_message_ids,
            "media_group_id": media_group_id,
            "status": status,
            "storage_message_ids": storage_message_ids or [],  # Track storage messages for cleanup
            "created_at": now,
            "updated_at": now,
            "sent_at": None,
            "done_at": None
        }
        result = self.posts.insert_one(doc)
        logger.info(f"Added new post to queue: {result.inserted_id}")
        return str(result.inserted_id)

    def get_oldest_queued(self) -> Optional[Dict[str, Any]]:
        """Get the oldest queued post (FIFO order)."""
        return self.posts.find_one(
            {"status": "queued"},
            sort=[("created_at", 1)]
        )

    def get_post_by_id(self, post_id: str) -> Optional[Dict[str, Any]]:
        """Get a post by its ID."""
        from bson import ObjectId
        return self.posts.find_one({"_id": ObjectId(post_id)})

    def mark_sent(self, post_id: str, storage_message_ids: Optional[List[int]] = None) -> bool:
        """Mark a post as sent (moved to Storage Channel)."""
        from bson import ObjectId
        update_data = {
            "status": "sent",
            "sent_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        if storage_message_ids:
            update_data["storage_message_ids"] = storage_message_ids

        result = self.posts.update_one(
            {"_id": ObjectId(post_id)},
            {"$set": update_data}
        )
        return result.modified_count > 0

    def update_storage_message_ids(self, post_id: str, storage_message_ids: List[int]) -> bool:
        """Update storage message IDs for a post (used when sending to storage)."""
        from bson import ObjectId
        result = self.posts.update_one(
            {"_id": ObjectId(post_id)},
            {
                "$set": {
                    "storage_message_ids": storage_message_ids,
                    "updated_at": datetime.now(timezone.utc)
                }
            }
        )
        return result.modified_count > 0

    def mark_done(self, post_id: str) -> bool:
        """Mark a post as done (Main Bot confirmed posting)."""
        from bson import ObjectId
        result = self.posts.update_one(
            {"_id": ObjectId(post_id)},
            {
                "$set": {
                    "status": "done",
                    "done_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc)
                }
            }
        )
        return result.modified_count > 0

    def get_current_sent(self) -> Optional[Dict[str, Any]]:
        """Get the currently active sent post."""
        return self.posts.find_one({"status": "sent"})

    def count_by_status(self, status: str) -> int:
        """Count posts by status."""
        return self.posts.count_documents({"status": status})

    # ==================== STATE OPERATIONS ====================

    def get_state(self) -> Dict[str, Any]:
        """
        Get the current bot state.
        Creates default state if not exists.
        """
        state = self.state.find_one({"_id": "current"})
        if not state:
            state = {
                "_id": "current",
                "current_post_id": None,
                "waiting_for_done": False,
                "last_check_time": None,
                "updated_at": datetime.now(timezone.utc)
            }
            self.state.insert_one(state)
        return state

    def update_state(self,
                     current_post_id: Optional[str] = None,
                     waiting_for_done: Optional[bool] = None) -> None:
        """
        Update the bot state.

        Args:
            current_post_id: ID of currently active post (None if none)
            waiting_for_done: Whether bot is waiting for "post done"
        """
        update = {"updated_at": datetime.now(timezone.utc)}

        if current_post_id is not None:
            update["current_post_id"] = current_post_id

        if waiting_for_done is not None:
            update["waiting_for_done"] = waiting_for_done

        self.state.update_one(
            {"_id": "current"},
            {"$set": update}
        )

    def clear_current_post(self) -> None:
        """Clear the current post (post is done)."""
        self.update_state(current_post_id=None, waiting_for_done=False)

    def set_waiting(self, post_id: str) -> None:
        """Set current post and mark as waiting for done."""
        self.update_state(current_post_id=post_id, waiting_for_done=True)

    # ==================== UTILITY ====================

    def close(self):
        """Close the MongoDB connection."""
        self.client.close()

    def get_queue_stats(self) -> Dict[str, int]:
        """Get statistics about the queue."""
        return {
            "queued": self.count_by_status("queued"),
            "sent": self.count_by_status("sent"),
            "done": self.count_by_status("done")
        }
