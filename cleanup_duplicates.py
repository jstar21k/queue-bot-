"""
Safe duplicate cleanup for queue/admin bot data.

Default mode is dry-run. Set CLEANUP_APPLY=true to delete proven duplicates.
The script keeps one original per duplicate group and only removes records that
can be tied to the same intake/media key or saved storage message ids.
"""

import os
import sys
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from pymongo import MongoClient


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


APPLY = env_bool("CLEANUP_APPLY", False)
DELETE_STORAGE = env_bool("CLEANUP_DELETE_STORAGE", True)
DELETE_PUBLIC = env_bool("CLEANUP_DELETE_PUBLIC", True)
MONGO_URI = os.getenv("MONGO_URI") or os.getenv("MONGODB_URI")
QUEUE_DB_NAME = os.getenv("QUEUE_DB_NAME") or os.getenv("DB_NAME", "queue_bot")
ADMIN_DB_NAME = os.getenv("ADMIN_DB_NAME", "tg_bot_pro_db")
BOT_TOKEN = os.getenv("BOT_TOKEN")
STORAGE_CHANNEL_ID = os.getenv("STORAGE_CHANNEL_ID")
POST_CHANNEL_ID = os.getenv("POST_CHANNEL_ID")


def require_config() -> None:
    missing = []
    if not MONGO_URI:
        missing.append("MONGO_URI or MONGODB_URI")
    if APPLY and DELETE_STORAGE and (not BOT_TOKEN or not STORAGE_CHANNEL_ID):
        missing.append("BOT_TOKEN and STORAGE_CHANNEL_ID for storage deletes")
    if APPLY and DELETE_PUBLIC and (not BOT_TOKEN or not POST_CHANNEL_ID):
        print("POST_CHANNEL_ID missing; public channel deletes will be skipped.")
    if missing:
        raise SystemExit("Missing required env: " + ", ".join(missing))


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def sorted_key(values: list[Any]) -> str | None:
    cleaned = [str(value) for value in values if value is not None]
    if not cleaned:
        return None
    return "|".join(sorted(cleaned))


def telegram_delete(chat_id: str | int | None, message_id: int | None, label: str) -> bool:
    if not APPLY:
        print(f"DRY-RUN delete {label}: chat={chat_id} message={message_id}")
        return False
    if not BOT_TOKEN or not chat_id or not message_id:
        print(f"SKIP delete {label}: missing token/chat/message")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage"
    data = urllib.parse.urlencode({"chat_id": str(chat_id), "message_id": str(int(message_id))}).encode()
    try:
        with urllib.request.urlopen(url, data=data, timeout=15) as response:
            ok = response.status == 200
            print(f"{'DELETED' if ok else 'DELETE?'} {label}: chat={chat_id} message={message_id}")
            return ok
    except Exception as exc:
        print(f"DELETE FAILED {label}: chat={chat_id} message={message_id}: {exc}")
        return False


def admin_refs_for_storage_ids(admin_db, storage_ids: list[int]) -> list[dict[str, Any]]:
    refs = []
    for storage_id in storage_ids:
        file_doc = admin_db.files.find_one({"storage_msg_id": int(storage_id)})
        if not file_doc:
            continue
        token = file_doc.get("token")
        scheduled = list(admin_db.scheduled_posts.find({"token": token}))
        refs.append({"storage_id": storage_id, "file": file_doc, "scheduled": scheduled})
    return refs


def has_admin_refs(admin_refs: list[dict[str, Any]]) -> bool:
    return bool(admin_refs)


def choose_keeper(group: list[dict[str, Any]], admin_refs_by_post: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    def score(post: dict[str, Any]) -> tuple[int, datetime]:
        refs = admin_refs_by_post.get(str(post["_id"]), [])
        created = post.get("created_at") or datetime.min.replace(tzinfo=timezone.utc)
        return (1 if has_admin_refs(refs) else 0, created)

    return sorted(group, key=score, reverse=True)[0]


def build_duplicate_groups(posts: list[dict[str, Any]]) -> list[tuple[str, str, list[dict[str, Any]]]]:
    buckets: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for post in posts:
        media_key = post.get("media_unique_key")
        if media_key:
            buckets[("media_unique_key", media_key)].append(post)

        intake_key = sorted_key(as_list(post.get("intake_message_ids")))
        if intake_key:
            buckets[("intake_message_ids", intake_key)].append(post)

        post_label = (post.get("post_label") or "").strip()
        if post_label:
            buckets[("post_label", post_label)].append(post)

    groups = []
    seen_sets = set()
    for (kind, key), group in buckets.items():
        if len(group) < 2:
            continue
        id_set = tuple(sorted(str(item["_id"]) for item in group))
        if id_set in seen_sets:
            continue
        seen_sets.add(id_set)
        groups.append((kind, key, sorted(group, key=lambda item: item.get("created_at") or datetime.min.replace(tzinfo=timezone.utc))))
    return groups


def remove_admin_duplicate_refs(admin_db, refs: list[dict[str, Any]]) -> None:
    for ref in refs:
        token = ref["file"].get("token")
        for scheduled in ref["scheduled"]:
            target_id = scheduled.get("target_message_id")
            if target_id and POST_CHANNEL_ID and DELETE_PUBLIC:
                telegram_delete(POST_CHANNEL_ID, int(target_id), f"public token={token}")
            if APPLY:
                admin_db.scheduled_posts.delete_one({"_id": scheduled["_id"]})
                print(f"DELETED admin scheduled token={token} id={scheduled['_id']}")
            else:
                print(f"DRY-RUN delete admin scheduled token={token} id={scheduled['_id']}")

        if APPLY:
            admin_db.files.delete_one({"_id": ref["file"]["_id"]})
            print(f"DELETED admin file token={token}")
        else:
            print(f"DRY-RUN delete admin file token={token}")


def cleanup_queue_duplicate(queue_db, admin_db, duplicate_post: dict[str, Any], admin_refs: list[dict[str, Any]]) -> None:
    post_id = duplicate_post["_id"]
    storage_ids = [int(value) for value in as_list(duplicate_post.get("storage_message_ids")) if value is not None]

    remove_admin_duplicate_refs(admin_db, admin_refs)

    if DELETE_STORAGE:
        for storage_id in storage_ids:
            telegram_delete(STORAGE_CHANNEL_ID, storage_id, f"storage queue_post={post_id}")

    if APPLY:
        queue_db.posts.delete_one({"_id": post_id})
        print(f"DELETED queue post id={post_id}")
    else:
        print(f"DRY-RUN delete queue post id={post_id}")


def main() -> int:
    require_config()
    client = MongoClient(MONGO_URI)
    queue_db = client[QUEUE_DB_NAME]
    admin_db = client[ADMIN_DB_NAME]

    posts = list(queue_db.posts.find({"status": {"$in": ["queued", "sending", "sent", "done"]}}))
    admin_refs_by_post = {
        str(post["_id"]): admin_refs_for_storage_ids(admin_db, as_list(post.get("storage_message_ids")))
        for post in posts
    }
    groups = build_duplicate_groups(posts)

    print("Mode:", "APPLY" if APPLY else "DRY-RUN")
    print("Queue DB:", QUEUE_DB_NAME, "Admin DB:", ADMIN_DB_NAME)
    print("Duplicate groups found:", len(groups))

    total_duplicates = 0
    for kind, key, group in groups:
        keeper = choose_keeper(group, admin_refs_by_post)
        print(f"\nGroup {kind}={key}")
        print(f"KEEP queue_post={keeper['_id']} status={keeper.get('status')} storage={keeper.get('storage_message_ids')}")

        for post in group:
            if post["_id"] == keeper["_id"]:
                continue
            total_duplicates += 1
            refs = admin_refs_by_post.get(str(post["_id"]), [])
            print(
                f"REMOVE queue_post={post['_id']} status={post.get('status')} "
                f"storage={post.get('storage_message_ids')} admin_tokens={[ref['file'].get('token') for ref in refs]}"
            )
            cleanup_queue_duplicate(queue_db, admin_db, post, refs)

    print("\nDuplicate queue posts selected:", total_duplicates)
    if not APPLY:
        print("No changes made. Re-run with CLEANUP_APPLY=true to delete these proven duplicates.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
