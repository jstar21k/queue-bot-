# Telegram Queue Bot

A simple, reliable Telegram bot for managing a post queue between channels. Designed for GitHub + Railway deployment.

## Overview

This bot monitors an **Intake Channel** for new posts (image + video pairs), queues them in MongoDB, and moves them one at a time to a **Storage Channel** where your existing Main Posting Bot can process them.

### Core Flow

```
1. Local script sends posts to Intake Channel (image + video)
2. Queue Bot detects complete posts (1 image + 1 video)
3. Posts are stored in MongoDB with status "queued"
4. If no active post, oldest queued post is sent to Storage Channel
5. Status changes to "sent", bot waits for "post done"
6. Main Posting Bot processes the post, sends "post done" to Storage Channel
7. Queue Bot detects "post done", marks post as "done"
8. Next queued post is sent to Storage Channel
9. Repeat...
```

## Features

- **Two intake styles supported:**
  - **STYLE A**: Separate image and video messages sent close together
  - **STYLE B**: Media group (album) containing image and video

- **One active post at a time** - prevents race conditions

- **Restart safety** - MongoDB state persists, no duplicate sending

- **Timeout alerting** - Admin notified if "post done" not received within X hours

- **Configurable intake cleanup** - Optional deletion of messages after moving

## Project Structure

```
queue-bot/
├── bot.py           # Main bot logic and handlers
├── config.py        # Configuration loading from environment
├── database.py      # MongoDB operations
├── requirements.txt # Python dependencies
├── .env.example     # Environment variables template
├── Procfile        # Railway deployment config
└── README.md        # This file
```

## Prerequisites

1. **Telegram Bot Token** - From [@BotFather](https://t.me/BotFather)
2. **MongoDB Instance** - Recommended: Railway MongoDB or MongoDB Atlas
3. **Telegram Channel IDs** - Numeric IDs for Intake and Storage channels
4. **Admin User ID** - Your Telegram user ID for alerts

### Getting Channel IDs

1. Add [@userinfobot](https://t.me/userinfobot) to your channels
2. Forward any message from each channel to the bot
3. The bot will show you the channel ID

Make sure your bot is an **admin** in both channels with permissions to:
- Read messages
- Delete messages (optional, for cleanup)

## Setup

### 1. Clone and Configure

```bash
# Clone the repository
git clone <your-repo-url>
cd queue-bot

# Copy environment file
cp .env.example .env

# Edit .env with your values
nano .env
```

### 2. Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `BOT_TOKEN` | Yes | Your Telegram bot token |
| `MONGO_URI` | Yes | MongoDB connection string |
| `INTAKE_CHANNEL_ID` | Yes | Intake channel numeric ID |
| `STORAGE_CHANNEL_ID` | Yes | Storage channel numeric ID |
| `ADMIN_ID` | Yes | Your Telegram user ID |
| `DELETE_INTAKE_MESSAGES` | No | Delete intake after move (default: false) |
| `TIMEOUT_HOURS` | No | Hours before timeout alert (default: 24) |
| `DB_NAME` | No | MongoDB database name (default: queue_bot) |

### 3. Local Testing

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the bot
python bot.py
```

## Railway Deployment

### 1. Push to GitHub

```bash
# Initialize git (if not already)
git init
git add .
git commit -m "Initial queue bot setup"

# Create repo on GitHub, then
git remote add origin <your-github-repo-url>
git push -u origin main
```

### 2. Connect to Railway

1. Go to [Railway](https://railway.app)
2. Create a new project
3. Connect your GitHub repository
4. Add environment variables:
   - `BOT_TOKEN`
   - `MONGO_URI`
   - `INTAKE_CHANNEL_ID`
   - `STORAGE_CHANNEL_ID`
   - `ADMIN_ID`
   - `DELETE_INTAKE_MESSAGES` (optional)
   - `TIMEOUT_HOURS` (optional)

### 3. MongoDB on Railway (Recommended)

1. In Railway project, add a **MongoDB** plugin
2. Copy the connection string to `MONGO_URI`

### 4. Deploy

Railway will automatically detect the `Procfile` and deploy.

---

## Bot Commands

| Command | Description |
|---------|-------------|
| `/start` | Start the bot |
| `/help` | Show help |
| `/status` | Show current queue status |
| `/stats` | Show queue statistics |
| `/process_next` | Manually trigger next post |

## MongoDB Schema

### Posts Collection

```json
{
  "_id": ObjectId,
  "intake_message_ids": [123, 456],
  "media_group_id": "abc123",
  "status": "queued|sent|done",
  "created_at": ISODate,
  "updated_at": ISODate,
  "sent_at": ISODate,
  "done_at": ISODate
}
```

### State Collection

```json
{
  "_id": "current",
  "current_post_id": "post_id_or_null",
  "waiting_for_done": true,
  "updated_at": ISODate
}
```

## Important Rules

1. **Only one active post at a time** - The bot will not send a new post until "post done" is received.

2. **Exact "post done" match** - The bot only responds to the exact text "post done" (no extra characters).

3. **Restart safety** - If the bot restarts while waiting for "post done", it will continue waiting and will NOT resend the post.

4. **Timeout handling** - After X hours without "post done", the bot alerts the admin but does NOT automatically send the next post. Manual intervention required.

## Troubleshooting

### Bot not responding to messages

1. Make sure bot is admin in Intake and Storage channels
2. Check that channel IDs in .env are correct
3. Verify bot token is correct

### Posts not being queued

1. Make sure posts contain exactly 1 image and 1 video
2. For STYLE A: messages must arrive close together (within 30 seconds)
3. Check logs for any errors

### Duplicate posts in Storage

1. This should not happen with proper MongoDB state management
2. Check that only one instance of the bot is running
3. Verify MongoDB state after restart

### "post done" not detected

1. Ensure Main Posting Bot sends exactly "post done" (case sensitive)
2. Check that bot is monitoring the correct Storage Channel
3. Look at logs for "Received 'post done'" message

## License

MIT