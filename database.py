"""
MongoDB database module for Queue Bot.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
from urllib.parse import parse_qsl, urlsplit

import certifi
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import PyMongoError
from pymongo import ReturnDocument

from config import BotConfig

logger = logging.getLogger(__name__)
_UNSET = object()


class DatabaseConnectionError(RuntimeError):
    """Raised when the bot cannot connect to MongoDB at startup."""


class DatabaseManager:
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
        exc_text = str(exc)
        lowered = exc_text.lower()

        if "ssl handshake failed" in lowered or "certificate_verify_failed" in lowered or "tlsv1" in lowered:
            return (
                "MongoDB TLS handshake failed. "
                "IMPORTANT: Add TLS parameters to your MONGO_URI:\n"
                "Example: mongodb+srv://user:pass@cluster.mongodb.net/db?tls=true&tlsminversion=TLSv1_2\n"
                "Original error: " + exc_text
            )

        if "mongodb.net" in mongo_uri.lower():
            return (
                "Could not connect to MongoDB Atlas. Check that:\n"
                "1. MONGO_URI is copied exactly from Atlas\n"
                "2. Network access allows Railway IPs\n"
                "3. Add ?tls=true to the URI\n"
                f"Original error: {exc_text}"
            )

        return f"Could not connect to MongoDB. Original error: {exc_text}"

    def _ensure_indexes(self):
        self.posts.create_index([("created_at", 1)])
        self.posts.create_index([("status", 1)])
        self.posts.create_index([("intake_message_ids", 1)], unique=True, sparse=True)
        self.posts.create_index([("media_unique_key", 1)], unique=True, sparse=True)

    # ==================== POST OPERATIONS ====================

    def add_post(self, intake_message_ids: List[int],
                 media_group_id: Optional[str],
                 status: str = "queued",
                 storage_message_ids: Optional[List[int]] = None,
                 media_unique_ids: Optional[List[str]] = None) -> str:
        now = datetime.now(timezone.utc)
        media_unique_key = None
        if media_unique_ids:
            media_unique_key = "|".join(sorted(media_unique_ids))
        doc = {
            "intake_message_ids": intake_message_ids,
            "media_group_id": media_group_id,
            "media_unique_ids": media_unique_ids or [],
            "status": status,
            "storage_message_ids": storage_message_ids or [],
            "created_at": now,
            "updated_at": now,
            "sent_at": None,
            "done_at": None
        }
        if media_unique_key:
            doc["media_unique_key"] = media_unique_key
        result = self.posts.insert_one(doc)
        logger.info(f"Added new post to queue: {result.inserted_id}")
        return str(result.inserted_id)

    def get_oldest_queued(self) -> Optional[Dict[str, Any]]:
        return self.posts.find_one({"status": "queued"}, sort=[("created_at", 1)])

    def claim_oldest_queued(self) -> Optional[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        return self.posts.find_one_and_update(
            {"status": "queued"},
            {"$set": {"status": "sending", "claimed_at": now, "updated_at": now}},
            sort=[("created_at", 1)],
            return_document=ReturnDocument.AFTER,
        )

    def find_by_any_intake_id(self, intake_message_ids: List[int]) -> Optional[Dict[str, Any]]:
        return self.posts.find_one(
            {"intake_message_ids": {"$in": intake_message_ids}},
            {"_id": 1, "status": 1, "intake_message_ids": 1},
        )

    def find_by_media_unique_ids(self, media_unique_ids: Optional[List[str]]) -> Optional[Dict[str, Any]]:
        if not media_unique_ids:
            return None
        media_unique_key = "|".join(sorted(media_unique_ids))
        return self.posts.find_one(
            {"media_unique_key": media_unique_key},
            {"_id": 1, "status": 1, "media_unique_key": 1},
        )

    def get_post_by_id(self, post_id: str) -> Optional[Dict[str, Any]]:
        from bson import ObjectId
        return self.posts.find_one({"_id": ObjectId(post_id)})

    def mark_sent(self, post_id: str, storage_message_ids: Optional[List[int]] = None) -> bool:
        from bson import ObjectId
        update_data = {"status": "sent", "sent_at": datetime.now(timezone.utc), "updated_at": datetime.now(timezone.utc)}
        if storage_message_ids:
            update_data["storage_message_ids"] = storage_message_ids
        result = self.posts.update_one({"_id": ObjectId(post_id)}, {"$set": update_data})
        return result.modified_count > 0

    def update_storage_message_ids(self, post_id: str, storage_message_ids: List[int]) -> bool:
        from bson import ObjectId
        result = self.posts.update_one(
            {"_id": ObjectId(post_id)},
            {"$set": {"storage_message_ids": storage_message_ids, "updated_at": datetime.now(timezone.utc)}}
        )
        return result.modified_count > 0

    def mark_done(self, post_id: str) -> bool:
        from bson import ObjectId
        result = self.posts.update_one(
            {"_id": ObjectId(post_id)},
            {"$set": {"status": "done", "done_at": datetime.now(timezone.utc), "updated_at": datetime.now(timezone.utc)}}
        )
        return result.modified_count > 0

    def mark_failed(self, post_id: str, reason: str) -> bool:
        from bson import ObjectId
        result = self.posts.update_one(
            {"_id": ObjectId(post_id)},
            {"$set": {"status": "failed", "failed_at": datetime.now(timezone.utc), "updated_at": datetime.now(timezone.utc), "last_error": reason}}
        )
        return result.modified_count > 0

    def mark_stale_sending_failed(self, max_age_minutes: int = 10) -> int:
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(minutes=max_age_minutes)
        result = self.posts.update_many(
            {"status": "sending", "updated_at": {"$lt": cutoff}},
            {"$set": {"status": "failed", "failed_at": now, "updated_at": now, "last_error": "Stale sending claim after restart."}}
        )
        return result.modified_count

    def get_current_sent(self) -> Optional[Dict[str, Any]]:
        return self.posts.find_one({"status": "sent"})

    def count_by_status(self, status: str) -> int:
        return self.posts.count_documents({"status": status})

    # ==================== STATE OPERATIONS ====================

    def get_state(self) -> Dict[str, Any]:
        state = self.state.find_one({"_id": "current"})
        if not state:
            state = {"_id": "current", "current_post_id": None, "waiting_for_done": False, "last_check_time": None, "updated_at": datetime.now(timezone.utc)}
            self.state.insert_one(state)
        return state

    def update_state(self,
                     current_post_id: Any = _UNSET,
                     waiting_for_done: Any = _UNSET) -> None:
        """
        Update the bot state.

        Args:
            current_post_id: ID of currently active post (None if none)
            waiting_for_done: Whether bot is waiting for "post done"
        """
        update = {"updated_at": datetime.now(timezone.utc)}

        if current_post_id is not _UNSET:
            update["current_post_id"] = current_post_id

        if waiting_for_done is not _UNSET:
            update["waiting_for_done"] = waiting_for_done
        self.state.update_one({"_id": "current"}, {"$set": update})

    def clear_current_post(self) -> None:
        self.update_state(current_post_id=None, waiting_for_done=False)

    def set_waiting(self, post_id: str) -> None:
        self.update_state(current_post_id=post_id, waiting_for_done=True)

    def close(self):
        self.client.close()

    def get_queue_stats(self) -> Dict[str, int]:
        return {"queued": self.count_by_status("queued"), "sent": self.count_by_status("sent"), "done": self.count_by_status("done")}
