"""
Reset output-side data for a fresh storage rebuild.

Default mode is dry-run. Set RESET_REBUILD_APPLY=true on Railway to apply.

This keeps queue posts and intake message IDs, so the queue bot can resend
existing intake media to a clean storage channel one post at a time.
"""

import os
import sys
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from pymongo import MongoClient


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


APPLY = env_bool("RESET_REBUILD_APPLY", False)
CLEAR_ANALYTICS = env_bool("RESET_CLEAR_ANALYTICS", False)
MONGO_URI = os.getenv("MONGO_URI") or os.getenv("MONGODB_URI")
QUEUE_DB_NAME = os.getenv("QUEUE_DB_NAME") or os.getenv("DB_NAME", "queue_bot")
ADMIN_DB_NAME = os.getenv("ADMIN_DB_NAME", "tg_bot_pro_db")


def require_config() -> None:
    if not MONGO_URI:
        raise SystemExit("Missing required env: MONGO_URI or MONGODB_URI")


def status_counts(posts: list[dict[str, Any]]) -> str:
    counts = Counter(str(post.get("status", "missing")) for post in posts)
    if not counts:
        return "none"
    return ", ".join(f"{status}={count}" for status, count in sorted(counts.items()))


def reset_queue_posts(queue_db, now: datetime, total_posts: int) -> None:
    update = {
        "$set": {
            "status": "queued",
            "storage_message_ids": [],
            "sent_at": None,
            "done_at": None,
            "claimed_at": None,
            "failed_at": None,
            "last_error": None,
            "updated_at": now,
        }
    }

    if APPLY:
        result = queue_db.posts.update_many({}, update)
        print(f"RESET queue posts: matched={result.matched_count} modified={result.modified_count}")
    else:
        print(f"DRY-RUN reset queue posts: would update {total_posts} records to queued")


def reset_queue_state(queue_db, now: datetime) -> None:
    state = {
        "_id": "current",
        "current_post_id": None,
        "waiting_for_done": False,
        "last_check_time": None,
        "updated_at": now,
    }
    if APPLY:
        queue_db.state.replace_one({"_id": "current"}, state, upsert=True)
        print("RESET queue state: current_post_id=None waiting_for_done=False")
    else:
        print("DRY-RUN reset queue state: current_post_id=None waiting_for_done=False")


def clear_admin_outputs(admin_db) -> None:
    files_count = admin_db.files.count_documents({})
    scheduled_count = admin_db.scheduled_posts.count_documents({})

    if APPLY:
        files_result = admin_db.files.delete_many({})
        scheduled_result = admin_db.scheduled_posts.delete_many({})
        print(f"DELETED admin files: {files_result.deleted_count}")
        print(f"DELETED admin scheduled_posts: {scheduled_result.deleted_count}")
    else:
        print(f"DRY-RUN delete admin files: would delete {files_count}")
        print(f"DRY-RUN delete admin scheduled_posts: would delete {scheduled_count}")


def maybe_clear_analytics(admin_db) -> None:
    users_count = admin_db.users.count_documents({})
    downloads_count = admin_db.downloads.count_documents({})

    if not CLEAR_ANALYTICS:
        print(f"KEEP admin users: {users_count}")
        print(f"KEEP admin downloads: {downloads_count}")
        return

    if APPLY:
        users_result = admin_db.users.delete_many({})
        downloads_result = admin_db.downloads.delete_many({})
        print(f"DELETED admin users: {users_result.deleted_count}")
        print(f"DELETED admin downloads: {downloads_result.deleted_count}")
    else:
        print(f"DRY-RUN delete admin users: would delete {users_count}")
        print(f"DRY-RUN delete admin downloads: would delete {downloads_count}")


def main() -> int:
    require_config()
    client = MongoClient(MONGO_URI)
    queue_db = client[QUEUE_DB_NAME]
    admin_db = client[ADMIN_DB_NAME]
    now = datetime.now(timezone.utc)

    posts = list(queue_db.posts.find({}))
    posts_with_intake = sum(1 for post in posts if post.get("intake_message_ids"))
    posts_with_storage = sum(1 for post in posts if post.get("storage_message_ids"))

    print("Mode:", "APPLY" if APPLY else "DRY-RUN")
    print("Queue DB:", QUEUE_DB_NAME, "Admin DB:", ADMIN_DB_NAME)
    print("Queue posts:", len(posts), f"({status_counts(posts)})")
    print("Queue posts with intake_message_ids:", posts_with_intake)
    print("Queue posts with old storage_message_ids:", posts_with_storage)
    print("Clear analytics:", CLEAR_ANALYTICS)

    if not posts:
        print("WARNING: queue posts collection is empty. Old intake cannot be rebuilt automatically.")

    reset_queue_posts(queue_db, now, len(posts))
    reset_queue_state(queue_db, now)
    clear_admin_outputs(admin_db)
    maybe_clear_analytics(admin_db)

    if not APPLY:
        print("\nNo changes made. Re-run with RESET_REBUILD_APPLY=true to apply this reset.")
    else:
        print("\nReset complete. Start admin-bot first, then queue-bot.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
