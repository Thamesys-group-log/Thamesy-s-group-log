import os
import asyncio
import discord
from discord.ext import commands
from keep_alive import keep_alive

# Start flask server to keep host alive
keep_alive()

TOKEN = os.getenv("DISCORD_TOKEN")

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True

bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")
    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} slash command(s).")
    except Exception as e:
        print(f"Failed to sync slash commands: {e}")

async def main():
    async with bot:
        # Load the cog inside cogs/log.py
        await bot.load_extension("cogs.log")
        await bot.start(TOKEN)

if __name__ == "__main__":
    if not TOKEN:
        raise ValueError("DISCORD_TOKEN environment variable is not set.")
    asyncio.run(main())

