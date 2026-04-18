from telethon import TelegramClient, events
from telethon.tl.functions.channels import GetParticipantRequest
from telethon.errors import FloodWaitError, ChatAdminRequiredError
from telethon.sessions import StringSession
import asyncio
import logging
from config import API_ID, API_HASH, SESSION_STRING, BOT_TOKEN, OWNER_ID, DEFAULT_DELETE_DELAY
import os
import sys
import json
from collections import deque
import time

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# File to store custom delays per chat
DELAYS_FILE = 'delete_delays.json'

class TelegramMessageDeleter:
    def __init__(self):
        self.user_client = None
        self.bot_client = None
        self.bot_info = None
        self.delete_delays = self.load_delays()
        self.message_queue = asyncio.Queue()
        self.processing = True
        self.batch_size = 5  # Delete 5 messages at once
        self.batch_delay = 2  # Wait 2 seconds between batches

    def load_delays(self):
        """Load custom delays from file"""
        if os.path.exists(DELAYS_FILE):
            try:
                with open(DELAYS_FILE, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def save_delays(self):
        """Save custom delays to file"""
        try:
            with open(DELAYS_FILE, 'w') as f:
                json.dump(self.delete_delays, f)
        except Exception as e:
            logger.error(f"Failed to save delays: {e}")

    def get_delete_delay(self, chat_id):
        """Get delete delay for a specific chat"""
        return self.delete_delays.get(str(chat_id), DEFAULT_DELETE_DELAY)

    def set_delete_delay(self, chat_id, seconds):
        """Set delete delay for a specific chat"""
        self.delete_delays[str(chat_id)] = seconds
        self.save_delays()

    async def delete_message_with_retry(self, message, max_retries=3):
        """Delete message with retry logic and flood wait handling"""
        for attempt in range(max_retries):
            try:
                await message.delete()
                return True
            except FloodWaitError as e:
                logger.warning(f"Flood wait: need to wait {e.seconds} seconds")
                await asyncio.sleep(e.seconds + 1)
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to delete message after {max_retries} attempts: {e}")
                else:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
        return False

    async def message_processor(self):
        """Process messages in batch to avoid rate limits"""
        batch = []
        last_process_time = time.time()
        
        while self.processing:
            try:
                # Get message from queue with timeout
                try:
                    message, delete_delay = await asyncio.wait_for(self.message_queue.get(), timeout=1)
                    batch.append((message, delete_delay))
                except asyncio.TimeoutError:
                    pass
                
                # Process batch if it's full or enough time has passed
                current_time = time.time()
                if len(batch) >= self.batch_size or (batch and current_time - last_process_time >= self.batch_delay):
                    if batch:
                        # Wait for delete delay
                        # Use the shortest delay in batch for waiting
                        min_delay = min(delay for _, delay in batch)
                        await asyncio.sleep(min_delay)
                        
                        # Delete messages in batch
                        for msg, _ in batch:
                            await self.delete_message_with_retry(msg)
                            await asyncio.sleep(0.5)  # Small delay between individual deletes
                        
                        logger.info(f"Deleted {len(batch)} messages in batch")
                        batch = []
                        last_process_time = current_time
                
            except Exception as e:
                logger.error(f"Error in message processor: {e}")
                await asyncio.sleep(1)

    async def start_user_client(self):
        """Start the user client using string session"""
        try:
            logger.info("🔄 Starting user client...")
            
            session = StringSession(SESSION_STRING)
            
            self.user_client = TelegramClient(
                session=session,
                api_id=API_ID,
                api_hash=API_HASH,
                flood_sleep_threshold=60  # Handle flood waits automatically
            )
            
            await self.user_client.start()
            user_me = await self.user_client.get_me()
            logger.info(f"✅ User client started successfully: {user_me.first_name}")
            
            # Start message processor
            asyncio.create_task(self.message_processor())
            
            @self.user_client.on(events.NewMessage())
            async def handler(event):
                try:
                    # Only process group messages
                    if not event.is_group:
                        return
                    
                    # Don't delete messages from our own bot
                    if self.bot_info and event.sender_id == self.bot_info.id:
                        return
                    
                    # Check if we have admin permissions in this chat
                    try:
                        chat = await event.get_chat()
                        if chat.default_banned_rights and chat.default_banned_rights.send_messages:
                            logger.info(f"Skipping chat {chat.id} - no send permissions")
                            return
                    except:
                        pass
                    
                    chat_id = event.chat_id
                    delete_delay = self.get_delete_delay(chat_id)
                    
                    sender = await event.get_sender()
                    sender_type = "🤖 Bot" if sender.bot else "👤 User"
                    
                    # For large groups, don't log every message
                    if event.chat_id < 0:
                        logger.info(f"{sender_type} message detected in chat {chat_id}, queued for deletion in {delete_delay}s")
                    else:
                        logger.info(f"{sender_type} message from {sender.first_name} - queued")
                    
                    # Add to queue for processing
                    await self.message_queue.put((event.message, delete_delay))
                            
                except Exception as e:
                    logger.error(f"Error in message handler: {e}")

            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to start user client: {e}")
            return False

    async def start_bot_client(self):
        """Start the bot client"""
        try:
            logger.info("🔄 Starting bot client...")
            self.bot_client = TelegramClient(
                session='bot_session',
                api_id=API_ID, 
                api_hash=API_HASH,
                flood_sleep_threshold=60
            )
            
            await self.bot_client.start(bot_token=BOT_TOKEN)
            self.bot_info = await self.bot_client.get_me()
            logger.info(f"✅ Bot client started: {self.bot_info.first_name}")
            
            # Owner-only /set command
            @self.bot_client.on(events.NewMessage(pattern='/set(?:\\s+(\\d+))?'))
            async def set_delay_handler(event):
                if event.sender_id != OWNER_ID:
                    await event.reply("❌ **Access Denied!**\nOnly the bot owner can change deletion time.", link_preview=False)
                    return
                
                if not event.is_group:
                    await event.reply("❌ Please use this command in a group chat.", link_preview=False)
                    return
                
                seconds = None
                if event.pattern_match.group(1):
                    seconds = int(event.pattern_match.group(1))
                else:
                    parts = event.raw_text.split()
                    if len(parts) > 1:
                        try:
                            seconds = int(parts[1])
                        except:
                            pass
                
                if seconds is None or seconds < 1:
                    current_delay = self.get_delete_delay(event.chat_id)
                    await event.reply(f"⏰ **Current Deletion Time:** `{current_delay} seconds`\n\n"
                                    f"📝 **Usage:** `/set <seconds>`\n"
                                    f"Example: `/set 15`", link_preview=False)
                    return
                
                if seconds > 300:
                    await event.reply("❌ Maximum delay is 300 seconds (5 minutes).", link_preview=False)
                    return
                
                self.set_delete_delay(event.chat_id, seconds)
                
                await event.reply(f"✅ **Deletion time updated!**\n"
                                f"⏰ Messages will now be deleted after `{seconds} seconds`\n"
                                f"📌 This setting applies only to this group.", link_preview=False)
            
            # /status command
            @self.bot_client.on(events.NewMessage(pattern='/status'))
            async def status_handler(event):
                if not event.is_group:
                    await event.reply("Use this command in a group chat.", link_preview=False)
                    return
                
                current_delay = self.get_delete_delay(event.chat_id)
                
                status_text = f"📊 **Group Status**\n\n"
                status_text += f"⏰ **Delete Delay:** `{current_delay} seconds`\n"
                status_text += f"👑 **Bot Owner:** `{OWNER_ID}`\n"
                status_text += f"🤖 **Bot Status:** Active\n"
                status_text += f"📝 **Queue Size:** `{self.message_queue.qsize()}` messages\n\n"
                
                if event.sender_id == OWNER_ID:
                    status_text += f"💡 **Tip:** Use `/set <seconds>` to change delete time"
                
                await event.reply(status_text, link_preview=False)
            
            # /start command
            @self.bot_client.on(events.NewMessage(pattern='/start'))
            async def start_handler(event):
                creator_text = "🤖 **Auto Message Deleter Bot**\n\n"
                creator_text += "**Features:**\n"
                creator_text += "• Automatically deletes ALL messages\n"
                creator_text += "• **Optimized for large public groups**\n"
                creator_text += "• Batch deletion to avoid rate limits\n"
                creator_text += "• Only bot's own messages are kept\n"
                creator_text += "• Owner can set custom delete time per group\n\n"
                creator_text += "**Commands:**\n"
                creator_text += "/start - Show this message\n"
                creator_text += "/status - Check current settings\n"
                creator_text += "/set <seconds> - Set delete time (Owner only)\n\n"
                
                if event.sender_id == OWNER_ID:
                    creator_text += "👑 **You are the bot owner!**"
                
                await event.reply(creator_text, link_preview=False)
            
            # Group welcome message
            @self.bot_client.on(events.ChatAction())
            async def chat_action_handler(event):
                if event.user_added and await event.get_user() == self.bot_info:
                    welcome_text = f"🤖 **Bot Added!**\n\n"
                    welcome_text += f"I will delete **ALL messages** after `{DEFAULT_DELETE_DELAY} seconds`\n"
                    welcome_text += f"**Optimized for large groups!**\n\n"
                    welcome_text += f"👑 **Owner can customize delete time** using `/set <seconds>`\n\n"
                    welcome_text += f"**Requirements:**\n"
                    welcome_text += "• Bot must be admin with delete permissions\n"
                    welcome_text += "• User account must be admin\n"
                    
                    await event.reply(welcome_text, link_preview=False)

            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to start bot client: {e}")
            return False

    async def run(self):
        """Run both clients"""
        try:
            bot_started = await self.start_bot_client()
            if not bot_started:
                logger.error("❌ Failed to start bot client")
                return
                
            user_started = await self.start_user_client()
            if not user_started:
                logger.error("❌ Failed to start user client")
                return
            
            logger.info("🚀 Auto Message Deleter is now running!")
            logger.info(f"👑 Owner ID: {OWNER_ID}")
            logger.info(f"⏰ Default deletion delay: {DEFAULT_DELETE_DELAY} seconds")
            logger.info("📦 Batch deletion enabled for large groups")
            
            await asyncio.gather(
                self.user_client.run_until_disconnected(),
                self.bot_client.run_until_disconnected(),
                return_exceptions=True
            )
                
        except Exception as e:
            logger.error(f"❌ Fatal error: {e}")
        finally:
            self.processing = False
            try:
                if self.user_client:
                    await self.user_client.disconnect()
                if self.bot_client:
                    await self.bot_client.disconnect()
            except:
                pass

async def main():
    deleter = TelegramMessageDeleter()
    await deleter.run()

if __name__ == "__main__":
    logger.info("🚀 Starting Auto Message Deleter (Large Group Optimized)...")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹️ Bot stopped")
    except Exception as e:
        logger.error(f"❌ Critical error: {e}")
        sys.exit(1)
