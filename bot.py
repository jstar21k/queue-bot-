"""
Main Queue Bot - Telegram Bot for managing post queue.

Core Flow:
1. Monitor Intake Channel for new posts (image + video)
2. Store valid posts in MongoDB as queued
3. When no active post, send oldest queued to Storage Channel
4. Wait for "post done" confirmation
5. Repeat

Supports two intake styles:
- STYLE A: Separate image and video messages (close together)
- STYLE B: Media group (album) with image and video
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class IntakeHandler:
    """
    Handles intake message processing.
    Supports STYLE A (separate messages) and STYLE B (media groups).
    """

    def __init__(self, bot_instance: 'QueueBot'):
        self.bot_instance = bot_instance

        # For STYLE A: Track messages by (chat_id, approximate_time)
        # Key: group_key, Value: {'image': Message, 'video': Message, 'timestamp': datetime}
        self.pending_pairs: Dict[int, Dict[str, Any]] = {}

        # For STYLE B: Track media group completion
        # Key: media_group_id, Value: {'messages': [], 'processed': bool}
        self.media_groups: Dict[str, Dict[str, Any]] = {}

        # Lock to prevent race conditions
        self._lock = asyncio.Lock()

    def _make_group_key(self, message: Message) -> int:
        """
        Create a group key for pairing messages.
        The first standalone message in a pair becomes the group anchor.
        """
        return message.message_id

    def _find_matching_pair(self, message: Message) -> Optional[int]:
        """
        Reuse the newest incomplete pair that can be completed by this message.
        """
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
        """
        Process a single message from Intake Channel.
        Determines if it completes a valid post.

        Returns True if post was successfully queued.
        """
        async with self._lock:
            content_type = message.content_type

            # Skip non-media messages
            if content_type not in [ContentType.PHOTO, ContentType.VIDEO]:
                return False

            # If it's part of a media group, handle differently
            if message.media_group_id:
                return await self._handle_media_group(message)

            # STYLE A: Standalone messages
            return await self._handle_standalone(message)

    async def _handle_standalone(self, message: Message) -> bool:
        """
        Handle STYLE A: Separate image and video messages.
        Pair messages that arrive close together.
        """
        content_type = message.content_type

        # Clean up stale pairs before trying to reuse one.
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

        # Check if this message completes the pair
        if content_type == ContentType.PHOTO:
            pair["image"] = message
        elif content_type == ContentType.VIDEO:
            pair["video"] = message

        # Check if we have a complete pair
        if pair["image"] and pair["video"]:
            # Complete post found!
            image_msg = pair["image"]
            video_msg = pair["video"]

            # Determine order (image before video based on message_id)
            if image_msg.message_id > video_msg.message_id:
                # Image came after video, swap order
                message_ids = [video_msg.message_id, image_msg.message_id]
            else:
                message_ids = [image_msg.message_id, video_msg.message_id]

            # Clean up
            del self.pending_pairs[group_key]

            # Queue the post - image first
            return await self.bot_instance.queue_post(
                message_ids=message_ids,
                media_group_id=None
            )

        return False

    async def _handle_media_group(self, message: Message) -> bool:
        """
        Handle STYLE B: Media group (album) messages.
        """
        group_id = message.media_group_id

        if not group_id:
            return False

        # Initialize or get group
        if group_id not in self.media_groups:
            self.media_groups[group_id] = {
                'messages': [],
                'photo_count': 0,
                'video_count': 0,
                'last_update': datetime.now(timezone.utc)
            }

        group = self.media_groups[group_id]

        # Add message if not already tracked
        if message.message_id not in [m.message_id for m in group['messages']]:
            group['messages'].append(message)

            if message.content_type == ContentType.PHOTO:
                group['photo_count'] += 1
            elif message.content_type == ContentType.VIDEO:
                group['video_count'] += 1

            group['last_update'] = datetime.now(timezone.utc)

        # Check for timeout (group should be complete within a few seconds)
        time_since_update = datetime.now(timezone.utc) - group['last_update']

        if time_since_update > timedelta(seconds=3):
            # Group appears complete, process it
            return await self._process_media_group(group_id)

        return False

    async def _process_media_group(self, group_id: str) -> bool:
        """
        Process a completed media group.
        Validates and queues if it contains exactly 1 image + 1 video.
        """
        if group_id not in self.media_groups:
            return False

        group = self.media_groups[group_id]
        messages = group['messages']

        # Count media types
        photos = [m for m in messages if m.content_type == ContentType.PHOTO]
        videos = [m for m in messages if m.content_type == ContentType.VIDEO]

        # Validate: exactly 1 photo and 1 video
        if len(photos) != 1 or len(videos) != 1:
            logger.warning(
                f"Invalid media group {group_id}: {len(photos)} photos, {len(videos)} videos"
            )
            # Clean up invalid group
            del self.media_groups[group_id]
            return False

        # Get message IDs in correct order (image before video)
        image_msg = photos[0]
        video_msg = videos[0]

        if image_msg.message_id < video_msg.message_id:
            message_ids = [image_msg.message_id, video_msg.message_id]
        else:
            message_ids = [video_msg.message_id, image_msg.message_id]

        # Clean up
        del self.media_groups[group_id]

        # Queue the post - image first
        return await self.bot_instance.queue_post(
            message_ids=message_ids,
            media_group_id=group_id
        )

    async def _cleanup_old_pairs(self) -> None:
        """Remove old pending pairs (timeout after 30 seconds)."""
        now = datetime.now(timezone.utc)
        timeout = timedelta(seconds=30)

        to_remove = []
        for key, pair in self.pending_pairs.items():
            if now - pair['timestamp'] > timeout:
                to_remove.append(key)

        for key in to_remove:
            del self.pending_pairs[key]


class QueueBot:
    """
    Main queue bot class.
    Handles intake monitoring, queue management, and storage sending.
    """

    def __init__(self, config: BotConfig):
        self.config = config
        self.bot = Bot(token=config.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        self.dp = Dispatcher()
        self.db = DatabaseManager(config)

        # Intake handler
        self.intake_handler = IntakeHandler(self)

        # Track time when sent post was sent (for timeout monitoring)
        self.sent_time: Optional[datetime] = None

        # Flag for graceful shutdown
        self.is_running = False

        # Register handlers
        self._setup_handlers()

    def _setup_handlers(self):
        """Register all message handlers."""
        dp = self.dp

        # Commands
        dp.message.register(self.cmd_start, Command("start"))
        dp.message.register(self.cmd_status, Command("status"))
        dp.message.register(self.cmd_stats, Command("stats"))
        dp.message.register(self.cmd_help, Command("help"))
        dp.message.register(self.cmd_process_next, Command("process_next"))

        # Intake channel - all messages
        dp.message.register(
            self.handle_intake,
            F.chat.id == int(self.config.intake_channel_id),
        )

        # Storage channel - monitor for "post done"
        dp.message.register(
            self.handle_post_done,
            F.chat.id == int(self.config.storage_channel_id),
            F.text.regexp(r"^post done$"),
        )

        # Storage channel - ignore other messages
        dp.message.register(
            self.ignore_storage_message,
            F.chat.id == int(self.config.storage_channel_id),
            lambda message: message.text != "post done",
        )

    # ==================== INTAKE HANDLING ====================

    async def handle_intake(self, message: Message) -> None:
        """
        Handle any message from Intake Channel.
        """
        try:
            # Check if this is a media group (album)
            if message.media_group_id:
                # Media groups require special handling
                # We'll process after a small delay to collect all messages
                asyncio.create_task(self._process_media_group_delayed(message))
            else:
                # Single message - process immediately via intake handler
                await self.intake_handler.process_message(message)

        except Exception as e:
            logger.error(f"Error handling intake message: {e}")

    async def _process_media_group_delayed(self, message: Message) -> None:
        """
        Process media group after a short delay to ensure all messages are received.
        """
        group_id = message.media_group_id

        # Wait for all messages to arrive
        await asyncio.sleep(2)

        # Now process the media group
        await self.intake_handler._process_media_group(group_id)

    async def queue_post(self, message_ids: List[int],
                         media_group_id: Optional[str] = None) -> bool:
        """
        Add a post to the queue.

        Args:
            message_ids: List of message IDs (in order: image, video)
            media_group_id: Media group ID if applicable

        Returns:
            True if successfully queued
        """
        try:
            post_id = self.db.add_post(
                intake_message_ids=message_ids,
                media_group_id=media_group_id,
                status="queued"
            )
            logger.info(f"Post queued: {post_id} (IDs: {message_ids})")

            # Try to send to storage if no active post
            await self.check_and_send_next()

            # Optionally delete intake messages
            if self.config.delete_intake_messages:
                await self._delete_messages(message_ids)

            return True

        except Exception as e:
            logger.error(f"Failed to queue post: {e}")
            return False

    async def _delete_messages(self, message_ids: List[int]) -> None:
        """Delete messages from Intake Channel."""
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
        """
        Handle "post done" confirmation from Main Posting Bot.
        This signals the current post has been successfully posted.
        """
        logger.info("Received 'post done' confirmation")

        # Get current sent post
        current_post = self.db.get_current_sent()

        if not current_post:
            logger.warning("Received 'post done' but no active sent post found")
            return

        # Mark as done
        post_id = str(current_post["_id"])
        self.db.mark_done(post_id)

        # Delete storage messages for this post (cleanup)
        storage_ids = current_post.get("storage_message_ids", [])
        if storage_ids:
            await self._delete_storage_messages(storage_ids)

        # Clear current state
        self.db.clear_current_post()

        logger.info(f"Post {post_id} marked as done. Storage cleaned. Queue continuing...")

        # Send next post
        await self.check_and_send_next()

    async def _delete_storage_messages(self, message_ids: List[int]) -> None:
        """Delete messages from Storage Channel after post is done."""
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
        """Ignore non-'post done' messages in Storage Channel."""
        # Silent ignore - these are expected
        pass

    # ==================== QUEUE MANAGEMENT ====================

    async def check_and_send_next(self) -> bool:
        """
        Check if we can send the next queued post to Storage.

        Returns True if a post was sent, False if still waiting or empty.
        """
        # Check if already waiting for done
        state = self.db.get_state()

        if state.get("waiting_for_done") and state.get("current_post_id"):
            logger.info("Already waiting for 'post done', not sending next")
            return False

        # Get next queued post
        next_post = self.db.get_oldest_queued()

        if not next_post:
            logger.info("No queued posts available")
            return False

        # Send to storage
        post_id = str(next_post["_id"])
        logger.info(f"Sending next post to storage: {post_id}")

        success, storage_ids = await self.send_to_storage(next_post)

        if success:
            # Save storage message IDs and mark as sent
            self.db.update_storage_message_ids(post_id, storage_ids)
            self.db.mark_sent(post_id)
            self.db.set_waiting(post_id)
            self.sent_time = datetime.now(timezone.utc)
            logger.info(f"Post {post_id} sent to storage (msgs: {storage_ids}), waiting for confirmation")
            return True
        else:
            logger.error(f"Failed to send post {post_id} to storage")
            return False

    async def send_to_storage(self, post: Dict[str, Any]) -> Tuple[bool, List[int]]:
        """
        Send a post from queue to Storage Channel.
        Forwards the original messages preserving order.

        Order rule: Image BEFORE Video (for Main Bot compatibility)

        Returns:
            Tuple of (success, list of storage message IDs)
        """
        intake_ids = post.get("intake_message_ids", [])

        if not intake_ids:
            logger.error("Post has no intake message IDs")
            return False, []

        try:
            intake_chat = int(self.config.intake_channel_id)
            storage_chat = int(self.config.storage_channel_id)

            storage_msg_ids: List[int] = []

            # Forward messages in order (first should be image)
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
                    # Continue with other messages

            return len(storage_msg_ids) >= 2, storage_msg_ids

        except Exception as e:
            logger.error(f"Failed to send post to storage: {e}")
            return False, []

    # ==================== TIMEOUT MONITORING ====================

    async def check_timeouts(self) -> None:
        """
        Check if we've been waiting too long for 'post done'.
        Alert admin if timeout exceeded.
        """
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

            # Don't reset sent_time - we want to keep alerting
            # Or we could reset to prevent repeated alerts
            # self.sent_time = None

    # ==================== STARTUP RECOVERY ====================

    async def startup_recovery(self) -> None:
        """
        On bot startup, recover state from MongoDB.
        Ensures no duplicate sending after restart.
        Also scans existing intake messages for pending posts.
        """
        logger.info("Running startup recovery...")

        # Get current state
        state = self.db.get_state()
        current_post_id = state.get("current_post_id")
        waiting = state.get("waiting_for_done", False)

        # Check if there's an active sent post
        active_sent = self.db.get_current_sent()

        if active_sent and waiting:
            # We have a post that's sent and waiting
            logger.info(f"Found active sent post: {current_post_id}")
            logger.info("Continuing to wait for 'post done'...")
            self.sent_time = active_sent.get("sent_at")

            if self.sent_time and isinstance(self.sent_time, datetime):
                # Convert to timezone aware if needed
                if self.sent_time.tzinfo is None:
                    self.sent_time = self.sent_time.replace(tzinfo=timezone.utc)

        elif not active_sent:
            # No active sent post - clear state
            logger.info("No active sent post found")
            self.db.clear_current_post()

        # Log queue stats
        stats = self.db.get_queue_stats()
        logger.info(f"Queue stats: {stats}")

        # Scan existing intake messages for pending posts
        await self.scan_existing_intake_messages()

        # Notify admin that bot has started
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
        logger.info("Scanning existing Intake Channel messages...")

        try:
            # Get recent messages from Intake Channel
            # Telegram limits: getChatHistory returns up to limited messages
            messages = []
            async for msg in self.bot.get_chat_history(
                chat_id=self.config.intake_channel_id,
                limit=100  # Scan last 100 messages
            ):
                messages.append(msg)

            # Reverse to process oldest first
            messages.reverse()

            logger.info(f"Found {len(messages)} messages to scan")

            # Track messages for pairing
            pending_images: Dict[int, Message] = {}  # message_id -> message
            pending_videos: Dict[int, Message] = {}  # message_id -> message

            for msg in messages:
                content_type = msg.content_type

                # Skip non-media
                if content_type not in [ContentType.PHOTO, ContentType.VIDEO]:
                    continue

                # Skip media groups (handled separately)
                if msg.media_group_id:
                    continue

                # Check if already processed (skip if already queued)
                # Simple check: message_id already in any queued post
                # We'll rely on duplicate detection at queue time

                if content_type == ContentType.PHOTO:
                    pending_images[msg.message_id] = msg
                elif content_type == ContentType.VIDEO:
                    pending_videos[msg.message_id] = msg

            # Now pair images and videos that are close together
            # Group by approximate time/window
            posts_to_queue = []
            processed_videos = set()

            # Sort by message_id (sequential = chronological)
            all_video_ids = sorted(pending_videos.keys())

            for video_id in all_video_ids:
                video_msg = pending_videos[video_id]

                # Find closest image before this video
                closest_image = None
                closest_distance = float('inf')

                for img_id, img_msg in pending_images.items():
                    if img_id >= video_id:
                        continue  # Image must come before video
                    distance = video_id - img_id
                    if distance < closest_distance and distance <= 5:  # Within 5 messages
                        closest_distance = distance
                        closest_image = img_msg

                if closest_image:
                    # Found a pair - store message IDs
                    img_id = closest_image.message_id
                    posts_to_queue.append([img_id, video_id])
                    processed_videos.add(video_id)
                    logger.info(f"Found pair: image {img_id}, video {video_id}")

            # Queue all found posts
            for msg_ids in posts_to_queue:
                try:
                    self.db.add_post(
                        intake_message_ids=msg_ids,
                        media_group_id=None,
                        status="queued"
                    )
                    logger.info(f"Queued existing post: {msg_ids}")
                except Exception as e:
                    # Likely duplicate - ignore
                    logger.debug(f"Post already exists or error: {e}")

            logger.info(f"Scanned intake: found {len(posts_to_queue)} posts to queue")

        except Exception as e:
            logger.error(f"Error scanning intake messages: {e}")

    # ==================== COMMANDS ====================

    async def cmd_start(self, message: Message) -> None:
        """Handle /start command."""
        await message.answer(
            "🤖 Queue Bot Started\n\n"
            "Monitoring Intake Channel for new posts.\n"
            "Use /help for available commands."
        )

    async def cmd_help(self, message: Message) -> None:
        """Handle /help command."""
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
        """Handle /status command."""
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
        """Handle /stats command."""
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
        """Manually trigger processing of next queued post."""
        success = await self.check_and_send_next()
        if success:
            await message.answer("✅ Next post sent to storage")
        else:
            await message.answer("⚠️ Could not send next post (queue empty or still waiting)")

    # ==================== LIFECYCLE ====================

    async def start(self) -> None:
        """Start the bot."""
        logger.info("Starting Queue Bot...")

        # Startup recovery
        await self.startup_recovery()

        self.is_running = True

        # Start polling
        logger.info("Bot is running, polling for messages...")

        # Start timeout checker in background
        asyncio.create_task(self._timeout_checker())

        await self.dp.start_polling(self.bot)

    async def _timeout_checker(self) -> None:
        """Background task to check for timeouts periodically."""
        while self.is_running:
            await asyncio.sleep(60)  # Check every minute
            await self.check_timeouts()

    async def stop(self) -> None:
        """Gracefully stop the bot."""
        logger.info("Stopping Queue Bot...")
        self.is_running = False
        self.db.close()
        await self.bot.session.close()


# ==================== MAIN ====================

def setup_signal_handlers(bot: QueueBot):
    """Setup signal handlers for graceful shutdown."""
    def signal_handler(signum, frame):
        logger.info("Received shutdown signal")
        asyncio.create_task(bot.stop())
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


async def main():
    """Main entry point."""
    try:
        # Load configuration
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
            # Create and start bot
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
