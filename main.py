import os
import asyncio
import discord
from discord.ext import commands
from keep_alive import keep_alive

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True


class AuditBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=intents)

    async def setup_hook(self) -> None:
        """Asynchronous setup loop executed before the bot logs in."""
        try:
            # Load the audit logging cog if not already loaded
            if not self.get_cog("RobloxAuditLoggerClient2"):
                await self.load_extension("cogs.log")
                print("Loaded extension: cogs.log")
        except Exception as e:
            print(f"Error loading extension 'cogs.log': {e}")

        try:
            # Global sync for application commands
            synced = await self.tree.sync()
            print(f"Synced {len(synced)} slash command(s) globally.")
        except Exception as e:
            print(f"Error syncing slash commands: {e}")


bot = AuditBot()


@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")
    print("Bot is online and ready.")


if __name__ == "__main__":
    token = os.getenv("TOKEN") or os.getenv("DISCORD_TOKEN")
    if not token:
        print("Error: Discord token environment variable ('TOKEN' or 'DISCORD_TOKEN') is missing.")
    else:
        keep_alive(bot)
        bot.run(token)
