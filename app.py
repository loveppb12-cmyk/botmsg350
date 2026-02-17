from telethon import TelegramClient, events
from telethon.sessions import StringSession
import asyncio
import logging
from config import API_ID, API_HASH, SESSION_STRING, BOT_TOKEN
import os
import sys

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Configuration
DELETE_DELAY = 210 # 210 seconds

class TelegramMessageDeleter:
    def __init__(self):
        self.user_client = None
        self.bot_client = None
        self.bot_info = None

    async def start_user_client(self):
        """Start the user client using string session"""
        try:
            logger.info("🔄 Starting user client...")
            
            # Create session from string
            session = StringSession(SESSION_STRING)
            
            self.user_client = TelegramClient(
                session=session,
                api_id=API_ID,
                api_hash=API_HASH
            )
            
            await self.user_client.start()
            user_me = await self.user_client.get_me()
            logger.info(f"✅ User client started successfully: {user_me.first_name}")
            
            @self.user_client.on(events.NewMessage())
            async def handler(event):
                try:
                    # Ignore if no sender or not a group message
                    if not event.sender or not event.is_group:
                        return
                    
                    # Don't delete messages from our own bot
                    if self.bot_info and event.sender.id == self.bot_info.id:
                        return
                    
                    # Check if message is from a bot OR a regular user
                    sender_type = "🤖 Bot" if event.sender.bot else "👤 User"
                    
                    logger.info(f"{sender_type} message detected from {event.sender.first_name} (ID: {event.sender.id})")
                    logger.info(f"📝 Message: {event.text[:100] if event.text else 'Media message'}")
                    logger.info(f"⏰ Will delete in {DELETE_DELAY} seconds...")
                    
                    # Wait 10 seconds then delete
                    await asyncio.sleep(DELETE_DELAY)
                    
                    try:
                        await event.delete()
                        logger.info(f"✅ Successfully deleted {sender_type.lower()} message from {event.sender.first_name} after {DELETE_DELAY} seconds")
                    except Exception as delete_error:
                        logger.error(f"❌ Failed to delete message: {delete_error}")
                            
                except Exception as e:
                    logger.error(f"Error in message handler: {e}")

            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to start user client: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    async def start_bot_client(self):
        """Start the bot client"""
        try:
            logger.info("🔄 Starting bot client...")
            self.bot_client = TelegramClient(
                session='bot_session',
                api_id=API_ID, 
                api_hash=API_HASH
            )
            
            await self.bot_client.start(bot_token=BOT_TOKEN)
            self.bot_info = await self.bot_client.get_me()
            logger.info(f"✅ Bot client started: {self.bot_info.first_name} (@{self.bot_info.username})")
            
            # Add start command handler with creator credit
            @self.bot_client.on(events.NewMessage(pattern='/start'))
            async def start_handler(event):
                creator_text = "🤖 **Auto Message Deleter**\n\n"
                creator_text += "This Bot is created by [@uexnc](https://t.me/uexnc)\n\n"
                creator_text += "**Features:**\n"
                creator_text += "• Automatically detects ALL messages\n"
                creator_text += "• Deletes BOTH bot and user messages\n"
                creator_text += f"• Deletes messages after {DELETE_DELAY} seconds\n"
                creator_text += "• Works in groups where I'm admin\n"
                creator_text += "• Keeps only my own messages\n\n"
                creator_text += "**Requirements:**\n"
                creator_text += "• Bot must be admin with delete permissions\n"
                creator_text += "• User account must be admin with delete permissions\n\n"
                creator_text += "🚀 *Bot is now running and monitoring...*"
                
                await event.reply(creator_text, link_preview=False)
            
            # Add help command
            @self.bot_client.on(events.NewMessage(pattern='/help'))
            async def help_handler(event):
                help_text = "🆘 **Help Guide**\n\n"
                help_text += "**What I do:**\n"
                help_text += f"• I delete ALL messages (both bots and users) after {DELETE_DELAY} seconds\n"
                help_text += "• Only my own messages are kept safe\n"
                help_text += "• Works automatically in all groups where I'm admin\n\n"
                help_text += "**Commands:**\n"
                help_text += "/start - Check bot status\n"
                help_text += "/help - Show this help message\n\n"
                help_text += "**Note:** Make sure both bot and user account have admin rights with delete permissions!"
                
                await event.reply(help_text, link_preview=False)
            
            # Also send creator info when bot is added to group
            @self.bot_client.on(events.ChatAction())
            async def chat_action_handler(event):
                if event.user_added and await event.get_user() == self.bot_info:
                    creator_text = "🤖 **Thanks for adding me!**\n\n"
                    creator_text += "This Bot is created by [@uexnc](https://t.me/uexnc)\n\n"
                    creator_text += "I will automatically delete **ALL messages** (both bots and users) "
                    creator_text += f"**{DELETE_DELAY} seconds** after they are sent.\n\n"
                    creator_text += "**Only my messages are safe from deletion!**\n\n"
                    creator_text += "**Make sure to:**\n"
                    creator_text += "1. Promote me as admin\n"
                    creator_text += "2. Give me 'Delete Messages' permission\n"
                    creator_text += "3. Also promote the user account as admin\n\n"
                    creator_text += "Use /start to check my status\n"
                    creator_text += "Use /help for more information"
                    
                    await event.reply(creator_text, link_preview=False)

            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to start bot client: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    async def check_connections(self):
        """Check if both clients are connected properly"""
        try:
            if self.user_client and self.bot_client:
                user_me = await self.user_client.get_me()
                bot_me = await self.bot_client.get_me()
                
                logger.info(f"🔗 User account: {user_me.first_name} (@{user_me.username})")
                logger.info(f"🔗 Bot account: {bot_me.first_name} (@{bot_me.username})")
                logger.info("✅ Both clients are connected and ready!")
                return True
        except Exception as e:
            logger.error(f"❌ Connection check failed: {e}")
            return False

    async def run(self):
        """Run both clients"""
        try:
            # Start bot client first
            bot_started = await self.start_bot_client()
            if not bot_started:
                logger.error("❌ Failed to start bot client")
                return
                
            # Start user client
            user_started = await self.start_user_client()
            if not user_started:
                logger.error("❌ Failed to start user client")
                return
            
            if await self.check_connections():
                logger.info("🚀 Auto Message Deleter is now running!")
                logger.info("📝 Monitoring for ALL messages (both bots and users)...")
                logger.info(f"⏰ All messages will be deleted after {DELETE_DELAY} seconds")
                logger.info("🛡️ Only bot's own messages are protected from deletion")
                logger.info("👨‍💻 Created by @itz_fizzyll")
                
                # Keep both clients running
                await asyncio.gather(
                    self.user_client.run_until_disconnected(),
                    self.bot_client.run_until_disconnected(),
                    return_exceptions=True
                )
            else:
                logger.error("❌ Failed to establish connections")
                
        except Exception as e:
            logger.error(f"❌ Fatal error: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            # Cleanup
            logger.info("🔄 Disconnecting clients...")
            try:
                if self.user_client:
                    await self.user_client.disconnect()
                if self.bot_client:
                    await self.bot_client.disconnect()
            except:
                pass

async def main():
    """Main async function to run the bot"""
    deleter = TelegramMessageDeleter()
    await deleter.run()

if __name__ == "__main__":
    # For Heroku - simple execution
    logger.info("🚀 Starting Auto Message Deleter...")
    logger.info(f"⏰ Deletion delay set to: {DELETE_DELAY} seconds")
    logger.info("🎯 Target: ALL messages (both bots and users)")
    logger.info("🛡️ Protected: Only bot's own messages")
    logger.info("👨‍💻 Created by @itz_fizzyll")
    
    try:
        # Run the bot
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹️ Bot stopped by user")
    except Exception as e:
        logger.error(f"❌ Critical error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
