"""
Full fresh database reset for rebuilding with new intake/storage channels.

Default mode is dry-run. Set FULL_RESET_APPLY=true on Railway to apply.

This clears documents from known queue/admin collections with delete_many({})
instead of dropping databases, so collection/index structure is preserved.
"""

import os
import sys
from typing import Iterable

from pymongo import MongoClient


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


APPLY = env_bool("FULL_RESET_APPLY", False)
MONGO_URI = os.getenv("MONGO_URI") or os.getenv("MONGODB_URI")
QUEUE_DB_NAME = os.getenv("QUEUE_DB_NAME") or os.getenv("DB_NAME", "queue_bot")
ADMIN_DB_NAME = os.getenv("ADMIN_DB_NAME", "tg_bot_pro_db")

QUEUE_COLLECTIONS = ("posts", "state")
ADMIN_COLLECTIONS = ("files", "scheduled_posts", "users", "downloads", "runtime")


def require_config() -> None:
    if not MONGO_URI:
        raise SystemExit("Missing required env: MONGO_URI or MONGODB_URI")


def clear_collections(db, collection_names: Iterable[str], label: str) -> None:
    for name in collection_names:
        collection = db[name]
        count = collection.count_documents({})
        if APPLY:
            result = collection.delete_many({})
            print(f"DELETED {label}.{name}: {result.deleted_count}")
        else:
            print(f"DRY-RUN delete {label}.{name}: would delete {count}")


def main() -> int:
    require_config()
    client = MongoClient(MONGO_URI)
    queue_db = client[QUEUE_DB_NAME]
    admin_db = client[ADMIN_DB_NAME]

    print("Mode:", "APPLY" if APPLY else "DRY-RUN")
    print("Queue DB:", QUEUE_DB_NAME)
    print("Admin DB:", ADMIN_DB_NAME)
    print("Queue collections:", ", ".join(QUEUE_COLLECTIONS))
    print("Admin collections:", ", ".join(ADMIN_COLLECTIONS))

    clear_collections(queue_db, QUEUE_COLLECTIONS, QUEUE_DB_NAME)
    clear_collections(admin_db, ADMIN_COLLECTIONS, ADMIN_DB_NAME)

    if not APPLY:
        print("\nNo changes made. Re-run with FULL_RESET_APPLY=true to clear these documents.")
    else:
        print("\nFull fresh reset complete. Start admin-bot first, then queue-bot.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
