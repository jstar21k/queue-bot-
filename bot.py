"""
Main Queue Bot - Telegram Bot for managing post queue.
"""

import asyncio
import logging
import signal
import ssl
import sys
from contextlib import suppress
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

from aiogram import Bot, Dispatcher, F
from aiogram.client.bot import DefaultBotProperties
from aiogram.enums import ContentType, ParseMode
from aiogram.filters import Command
from aiogram.types import Message

from config import load_config, BotConfig
from database import DatabaseManager, DatabaseConnectionError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class IntakeHandler:
    """Handles intake message processing for STYLE A (separate) and STYLE B (media group)."""

    def __init__(self, bot_instance: 'QueueBot'):
        self.bot_instance = bot_instance
        self.pending_pairs: Dict[int, Dict[str, Any]] = {}
        self.media_groups: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    def _make_group_key(self, message: Message) -> int:
        return message.message_id

    def _get_file_unique_id(self, message: Message) -> Optional[str]:
        if message.content_type == ContentType.PHOTO and message.photo:
            return message.photo[-1].file_unique_id
        if message.content_type == ContentType.VIDEO and message.video:
            return message.video.file_unique_id
        return None

    def _get_post_label(self, *messages: Message) -> Optional[str]:
        for message in messages:
            text = (message.caption or "").strip()
            if text:
                return text.splitlines()[0].strip()
        return None

    def _find_matching_pair(self, message: Message) -> Optional[int]:
        content_type = message.content_type
        for key in sorted(self.pending_pairs.keys(), reverse=True):
            pair = self.pending_pairs[key]
            other_message = None

            if content_type == ContentType.PHOTO and pair["image"] is None and pair["video"] is not None:
                other_message = pair["video"]
            elif content_type == ContentType.VIDEO and pair["video"] is None and pair["image"] is not None:
                other_message = pair["image"]

            if other_message is None:
                continue

            if abs(message.message_id - other_message.message_id) <= 5:
                return key
        return None

    async def process_message(self, message: Message) -> bool:
        async with self._lock:
            content_type = message.content_type
            if content_type not in [ContentType.PHOTO, ContentType.VIDEO]:
                return False
            if message.media_group_id:
                return await self._handle_media_group(message)
            return await self._handle_standalone(message)

    async def _handle_standalone(self, message: Message) -> bool:
        content_type = message.content_type
        await self._cleanup_old_pairs()

        group_key = self._find_matching_pair(message)
        if group_key is None:
            group_key = self._make_group_key(message)
            self.pending_pairs[group_key] = {
                "timestamp": datetime.now(timezone.utc),
                "image": None,
                "video": None,
            }

        pair = self.pending_pairs[group_key]
        pair["timestamp"] = datetime.now(timezone.utc)

        if content_type == ContentType.PHOTO:
            pair["image"] = message
        elif content_type == ContentType.VIDEO:
            pair["video"] = message

        if pair["image"] and pair["video"]:
            image_msg = pair["image"]
            video_msg = pair["video"]
            del self.pending_pairs[group_key]
            return await self.bot_instance.queue_post(
                message_ids=[image_msg.message_id, video_msg.message_id],
                media_unique_ids=[
                    uid for uid in [
                        self._get_file_unique_id(image_msg),
                        self._get_file_unique_id(video_msg),
                    ] if uid
                ],
                post_label=self._get_post_label(image_msg, video_msg),
                media_group_id=None
            )
        return False

    async def _handle_media_group(self, message: Message) -> bool:
        group_id = message.media_group_id
        if not group_id:
            return False

        if group_id not in self.media_groups:
            self.media_groups[group_id] = {
                'messages': [],
                'photo_count': 0,
                'video_count': 0,
                'last_update': datetime.now(timezone.utc)
            }

        group = self.media_groups[group_id]
        if message.message_id not in [m.message_id for m in group['messages']]:
            group['messages'].append(message)
            if message.content_type == ContentType.PHOTO:
                group['photo_count'] += 1
            elif message.content_type == ContentType.VIDEO:
                group['video_count'] += 1
            group['last_update'] = datetime.now(timezone.utc)
        return False

    async def _process_media_group(self, group_id: str) -> bool:
        if group_id not in self.media_groups:
            return False

        group = self.media_groups[group_id]
        messages = group['messages']

        photos = [m for m in messages if m.content_type == ContentType.PHOTO]
        videos = [m for m in messages if m.content_type == ContentType.VIDEO]

        if len(photos) != 1 or len(videos) != 1:
            logger.warning(f"Invalid media group {group_id}: {len(photos)} photos, {len(videos)} videos")
            del self.media_groups[group_id]
            return False

        image_msg = photos[0]
        video_msg = videos[0]
        del self.media_groups[group_id]
        return await self.bot_instance.queue_post(
            message_ids=[image_msg.message_id, video_msg.message_id],
            media_unique_ids=[
                uid for uid in [
                    self._get_file_unique_id(image_msg),
                    self._get_file_unique_id(video_msg),
                ] if uid
            ],
            post_label=self._get_post_label(image_msg, video_msg),
            media_group_id=group_id
        )

    async def _cleanup_old_pairs(self) -> None:
        now = datetime.now(timezone.utc)
        timeout = timedelta(seconds=30)
        to_remove = []
        for key, pair in self.pending_pairs.items():
            if now - pair['timestamp'] > timeout:
                to_remove.append(key)
        for key in to_remove:
            del self.pending_pairs[key]


class QueueBot:
    def __init__(self, config: BotConfig):
        self.config = config
        self.bot = Bot(token=config.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        self.dp = Dispatcher()
        self.db = DatabaseManager(config)
        self.intake_handler = IntakeHandler(self)
        self.media_group_tasks: Dict[str, asyncio.Task[Any]] = {}
        self.send_lock = asyncio.Lock()
        self.sent_time: Optional[datetime] = None
        self.is_running = False
        self._setup_handlers()

    def _setup_handlers(self):
        dp = self.dp

        dp.message.register(self.cmd_start, Command("start"))
        dp.message.register(self.cmd_status, Command("status"))
        dp.message.register(self.cmd_stats, Command("stats"))
        dp.message.register(self.cmd_help, Command("help"))
        dp.message.register(self.cmd_process_next, Command("process_next"))

        # Intake channel posts arrive as channel_post updates, not message updates.
        dp.channel_post.register(
            self.handle_intake,
            F.chat.id == int(self.config.intake_channel_id),
            F.content_type.in_([ContentType.PHOTO, ContentType.VIDEO]),
        )

        # Storage channel posts also arrive as channel_post updates.
        dp.channel_post.register(
            self.handle_post_done,
            F.chat.id == int(self.config.storage_channel_id),
            F.text.regexp(r"^post done$"),
        )

        # Storage channel - ignore other messages
        dp.channel_post.register(
            self.ignore_storage_message,
            F.chat.id == int(self.config.storage_channel_id),
            lambda message: message.text != "post done",
        )

    # ==================== INTAKE HANDLING ====================

    async def handle_intake(self, message: Message) -> None:
        logger.info(f"Received message in intake: ID={message.message_id}, type={message.content_type}, media_group={message.media_group_id}")

        try:
            result = await self.intake_handler.process_message(message)

            if message.media_group_id:
                self._schedule_media_group_processing(message.media_group_id)
            elif result:
                logger.info(f"Message {message.message_id} completed a post and was queued")
            else:
                logger.info(f"Message {message.message_id} did not complete a post (waiting for pair)")
        except Exception as e:
            logger.error(f"Error handling intake message: {e}")

    def _schedule_media_group_processing(self, group_id: str) -> None:
        """Schedule one delayed finalize task per media group."""
        existing_task = self.media_group_tasks.get(group_id)

        if existing_task and not existing_task.done():
            return

        self.media_group_tasks[group_id] = asyncio.create_task(
            self._process_media_group_delayed(group_id)
        )

    async def _process_media_group_delayed(self, group_id: str) -> None:
        """Process media group after a short delay to ensure all messages are received."""
        try:
            await asyncio.sleep(2)
            result = await self.intake_handler._process_media_group(group_id)
            if result:
                logger.info(f"Media group {group_id} completed a post and was queued")
            else:
                logger.info(f"Media group {group_id} was incomplete or invalid")
        finally:
            self.media_group_tasks.pop(group_id, None)

    async def queue_post(self, message_ids: List[int],
                         media_unique_ids: Optional[List[str]] = None,
                         post_label: Optional[str] = None,
                         media_group_id: Optional[str] = None) -> bool:
        try:
            existing_by_message = self.db.find_by_any_intake_id(message_ids)
            if existing_by_message:
                logger.warning(
                    "Skipping duplicate intake messages %s; already stored in post %s",
                    message_ids,
                    existing_by_message.get("_id"),
                )
                return False

            existing_by_media = self.db.find_by_media_unique_ids(media_unique_ids)
            if existing_by_media:
                logger.warning(
                    "Skipping duplicate media pair %s; already stored in post %s",
                    media_unique_ids,
                    existing_by_media.get("_id"),
                )
                return False

            post_id = self.db.add_post(
                intake_message_ids=message_ids,
                media_group_id=media_group_id,
                media_unique_ids=media_unique_ids,
                post_label=post_label,
                status="queued"
            )
            logger.info(f"Post queued: {post_id} (IDs: {message_ids})")
            await self.check_and_send_next()
            return True
        except Exception as e:
            logger.error(f"Failed to queue post: {e}")
            return False

    async def _delete_messages(self, message_ids: List[int]) -> None:
        try:
            for msg_id in message_ids:
                await self.bot.delete_message(
                    chat_id=self.config.intake_channel_id,
                    message_id=msg_id
                )
            logger.info(f"Deleted {len(message_ids)} intake messages")
        except Exception as e:
            logger.warning(f"Failed to delete intake messages: {e}")

    # ==================== STORAGE HANDLING ====================

    async def handle_post_done(self, message: Message) -> None:
        logger.info("Received 'post done' confirmation")
        current_post = self.db.get_current_sent()

        if not current_post:
            logger.warning("Received 'post done' but no active sent post found")
            return

        post_id = str(current_post["_id"])
        self.db.mark_done(post_id)

        self.db.clear_current_post()
        logger.info(f"Post {post_id} marked as done. Queue continuing...")
        await self.check_and_send_next()

    async def _delete_storage_messages(self, message_ids: List[int]) -> None:
        try:
            for msg_id in message_ids:
                await self.bot.delete_message(
                    chat_id=self.config.storage_channel_id,
                    message_id=msg_id
                )
            logger.info(f"Deleted {len(message_ids)} storage messages")
        except Exception as e:
            logger.warning(f"Failed to delete storage messages: {e}")

    async def ignore_storage_message(self, message: Message) -> None:
        pass

    # ==================== QUEUE MANAGEMENT ====================

    async def check_and_send_next(self) -> bool:
        async with self.send_lock:
            state = self.db.get_state()
            if state.get("waiting_for_done") and state.get("current_post_id"):
                logger.info("Already waiting for 'post done', not sending next")
                return False

            next_post = self.db.claim_oldest_queued()
            if not next_post:
                logger.info("No queued posts available")
                return False

            post_id = str(next_post["_id"])
            logger.info(f"Sending next post to storage: {post_id}")

            success, storage_ids = await self.send_to_storage(next_post)

            if success:
                self.db.update_storage_message_ids(post_id, storage_ids)
                self.db.mark_sent(post_id)
                self.db.set_waiting(post_id)

                # Only remove intake messages after the post is safely in Storage.
                if self.config.delete_intake_messages:
                    await self._delete_messages(next_post.get("intake_message_ids", []))

                self.sent_time = datetime.now(timezone.utc)
                logger.info(f"Post {post_id} sent to storage (msgs: {storage_ids}), waiting for confirmation")
                return True

            self.db.mark_failed(post_id, "Failed to forward both intake messages to storage.")
            logger.error(f"Failed to send post {post_id} to storage; marked failed")
            return False

    async def send_to_storage(self, post: Dict[str, Any]) -> Tuple[bool, List[int]]:
        intake_ids = post.get("intake_message_ids", [])
        if not intake_ids:
            logger.error("Post has no intake message IDs")
            return False, []

        try:
            intake_chat = int(self.config.intake_channel_id)
            storage_chat = int(self.config.storage_channel_id)
            storage_msg_ids: List[int] = []

            for msg_id in intake_ids:
                try:
                    sent_msg = await self.bot.forward_message(
                        chat_id=storage_chat,
                        from_chat_id=intake_chat,
                        message_id=msg_id
                    )
                    storage_msg_ids.append(sent_msg.message_id)
                    logger.info(f"Forwarded message {msg_id} -> storage msg {sent_msg.message_id}")
                except Exception as e:
                    logger.error(f"Failed to forward message {msg_id}: {e}")

            return len(storage_msg_ids) >= 2, storage_msg_ids
        except Exception as e:
            logger.error(f"Failed to send post to storage: {e}")
            return False, []

    # ==================== TIMEOUT MONITORING ====================

    async def check_timeouts(self) -> None:
        if not self.sent_time:
            return

        state = self.db.get_state()
        if not state.get("waiting_for_done"):
            return

        elapsed = datetime.now(timezone.utc) - self.sent_time
        timeout = timedelta(hours=self.config.timeout_hours)

        if elapsed >= timeout:
            logger.warning(f"Timeout reached ({self.config.timeout_hours}h), alerting admin")
            try:
                await self.bot.send_message(
                    chat_id=self.config.admin_id,
                    text=f"⚠️ ALERT: Queue Bot Timeout\n\n"
                         f"Waiting for 'post done' for {self.config.timeout_hours} hours.\n"
                         f"Manual intervention required.\n\n"
                         f"Post ID: {state.get('current_post_id')}"
                )
            except Exception as e:
                logger.error(f"Failed to send alert: {e}")

    # ==================== STARTUP RECOVERY ====================

    async def startup_recovery(self) -> None:
        logger.info("Running startup recovery...")
        stale_sending_count = self.db.mark_stale_sending_failed()
        if stale_sending_count:
            logger.warning("Marked %s stale sending posts as failed", stale_sending_count)

        state = self.db.get_state()
        current_post_id = state.get("current_post_id")
        waiting = state.get("waiting_for_done", False)
        active_sent = self.db.get_current_sent()

        if active_sent and waiting:
            logger.info(f"Found active sent post: {current_post_id}")
            logger.info("Continuing to wait for 'post done'...")
            self.sent_time = active_sent.get("sent_at")
            if self.sent_time and isinstance(self.sent_time, datetime):
                if self.sent_time.tzinfo is None:
                    self.sent_time = self.sent_time.replace(tzinfo=timezone.utc)
        elif not active_sent:
            logger.info("No active sent post found")
            self.db.clear_current_post()

        stats = self.db.get_queue_stats()
        logger.info(f"Queue stats: {stats}")

        await self.scan_existing_intake_messages()

        try:
            stats = self.db.get_queue_stats()
            await self.bot.send_message(
                chat_id=self.config.admin_id,
                text=f"🤖 Queue Bot Started\n\n"
                     f"Queue: {stats['queued']} queued, {stats['sent']} sent\n"
                     f"Status: {'Waiting for confirmation' if waiting else 'Ready'}"
            )
        except Exception as e:
            logger.warning(f"Could not send startup message: {e}")

    async def scan_existing_intake_messages(self) -> None:
        """
        Scan existing messages in Intake Channel on startup.
        Process valid posts (1 image + 1 video) in chronological order.
        Skip anything already saved in MongoDB.
        """
        logger.info(
            "Skipping historical intake scan: Telegram Bot API does not expose "
            "channel history to bots. New channel_post updates and MongoDB state "
            "will continue the queue safely."
        )

    # ==================== COMMANDS ====================

    async def cmd_start(self, message: Message) -> None:
        await message.answer(
            "🤖 Queue Bot Started\n\n"
            "Monitoring Intake Channel for new posts.\n"
            "Use /help for available commands."
        )

    async def cmd_help(self, message: Message) -> None:
        await message.answer(
            "📖 Available Commands:\n\n"
            "/start - Start the bot\n"
            "/status - Show current queue status\n"
            "/stats - Show queue statistics\n"
            "/process_next - Manually trigger next post\n"
            "/help - Show this help\n\n"
            "The bot automatically processes the queue."
        )

    async def cmd_status(self, message: Message) -> None:
        state = self.db.get_state()
        stats = self.db.get_queue_stats()
        waiting = state.get("waiting_for_done", False)
        current_id = state.get("current_post_id")

        status_icon = "🟢"
        status_text = "Ready"
        if waiting:
            status_icon = "⏳"
            status_text = f"Waiting for 'post done'\nPost ID: {current_id}"

        await message.answer(
            f"{status_icon} Bot Status: {status_text}\n\n"
            f"📊 Queue Stats:\n"
            f"  Queued: {stats['queued']}\n"
            f"  Sent: {stats['sent']}\n"
            f"  Done: {stats['done']}"
        )

    async def cmd_stats(self, message: Message) -> None:
        stats = self.db.get_queue_stats()
        total = stats['queued'] + stats['sent'] + stats['done']
        await message.answer(
            f"📈 Queue Statistics:\n\n"
            f"Queued: {stats['queued']}\n"
            f"Sent (waiting): {stats['sent']}\n"
            f"Completed: {stats['done']}\n\n"
            f"Total: {total}"
        )

    async def cmd_process_next(self, message: Message) -> None:
        success = await self.check_and_send_next()
        if success:
            await message.answer("✅ Next post sent to storage")
        else:
            await message.answer("⚠️ Could not send next post (queue empty or still waiting)")

    # ==================== LIFECYCLE ====================

    async def start(self) -> None:
        logger.info("Starting Queue Bot...")
        await self.startup_recovery()
        self.is_running = True
        logger.info("Bot is running, polling for messages...")
        asyncio.create_task(self._timeout_checker())
        await self.dp.start_polling(self.bot)

    async def _timeout_checker(self) -> None:
        while self.is_running:
            await asyncio.sleep(60)
            await self.check_timeouts()

    async def stop(self) -> None:
        logger.info("Stopping Queue Bot...")
        self.is_running = False
        self.db.close()
        await self.bot.session.close()


def setup_signal_handlers(bot: QueueBot):
    def signal_handler(signum, frame):
        logger.info("Received shutdown signal")
        asyncio.create_task(bot.stop())
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


async def main():
    try:
        config = load_config()
        logger.info("Configuration loaded successfully")
        logger.info(
            "Runtime info: Python %s | OpenSSL %s",
            sys.version.split()[0],
            ssl.OPENSSL_VERSION,
        )
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    retry_delay_seconds = 15

    while True:
        bot: Optional[QueueBot] = None
        try:
            bot = QueueBot(config)
            setup_signal_handlers(bot)
            await bot.start()
            return
        except DatabaseConnectionError as e:
            logger.error(f"Database connection error: {e}")
            logger.info(f"Retrying startup in {retry_delay_seconds} seconds...")

            if bot is not None:
                with suppress(Exception):
                    await bot.stop()

            await asyncio.sleep(retry_delay_seconds)
        except Exception as e:
            logger.error(f"Bot error: {e}")

            if bot is not None:
                with suppress(Exception):
                    await bot.stop()

            sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
