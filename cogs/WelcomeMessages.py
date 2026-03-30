import discord
import logging
import datetime
import logging

import discord
from discord.ext import commands

from config_helpers import get_embed_colours, load_config


def audit_log(message: str):
    """Append a timestamped message to the audit log file."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("audit.log", "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")


class CommunityWelcome(commands.Cog):
    """Welcome new members and automatically react to introduction messages."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # Load the config file (UTF-8 for special characters)
        try:
            self.config = load_config()
        except Exception:
            self.config = {}

        channels = self.config.get("channels", {})
        welcome_cfg = (
            self.config.get("features", {})
            .get("welcome_messages", {})
        )

        # Welcome settings
        self.welcome_channel_id = channels.get("welcome_channel_id")
        self.new_member_channel_id = channels.get("new_member_guide_channel_id")
        self.welcome_enabled = welcome_cfg.get("enabled", True)
        self.welcome_image_path = "welcome-image.jpg"

        # Introductions reaction settings
        self.introductions_channel_id = channels.get("introductions_channel_id")

        colours = get_embed_colours()
        self.success_colour = colours["success"]
        self.info_colour = colours["info"]
        self.error_colour = colours["error"]

    @commands.Cog.listener()
    async def on_ready(self):
        logging.info("\033[96mCommunityWelcome\033[0m cog synced successfully.")
        audit_log("CommunityWelcome cog synced successfully.")

    # =========================
    # Welcome on member join
    # =========================
    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        # Check if welcome messages are enabled.
        if not self.welcome_enabled:
            logging.info("Welcome messages are disabled in config.")
            audit_log(
                f"Welcome messages disabled in guild '{member.guild.name}' (ID: {member.guild.id}). Skipping welcome for {member.name} (ID: {member.id})."
            )
            return

        guild = member.guild
        channel = guild.get_channel(self.welcome_channel_id)
        if not channel:
            logging.error(
                f"Welcome channel with ID '{self.welcome_channel_id}' not found in guild '{guild.name}'."
            )
            audit_log(
                f"Error: Welcome channel with ID '{self.welcome_channel_id}' not found in guild '{guild.name}' (ID: {guild.id})."
            )
            return

        guide_channel = (
            f"<#{self.new_member_channel_id}>"
            if self.new_member_channel_id
            else "the getting-started channel"
        )
        embed = discord.Embed(
            title="Welcome to the Official Home of Holly Humberstone.",
            description=(
                f"Welcome {member.mention}, we’re so happy you’re here!\n"
                f"Make sure to check out the {guide_channel} to find your way around!"
            ),
            color=self.info_colour,
        )

        embed.set_image(url="attachment://welcome-image.jpg")

        try:
            await channel.send(
                embed=embed,
                file=discord.File(
                    self.welcome_image_path, filename="welcome-image.jpg"
                ),
            )
            logging.info(
                f"Welcome embed sent for '{member.name}' in channel #{channel.name}."
            )
            audit_log(
                f"Sent welcome message for {member.name} (ID: {member.id}) in channel #{channel.name} (ID: {channel.id}) in guild '{guild.name}' (ID: {guild.id})."
            )
        except FileNotFoundError:
            logging.error(
                f"Welcome image not found at path '{self.welcome_image_path}'. Sending embed without image."
            )
            audit_log(
                f"Welcome image missing at '{self.welcome_image_path}'. Sent embed without image for {member.name} in guild '{guild.name}'."
            )
            await channel.send(embed=embed)
        except discord.HTTPException as e:
            logging.error(
                f"Error sending welcome embed in channel #{channel.name} (ID: {channel.id}): {e}"
            )
            audit_log(
                f"Failed to send welcome message for {member.name} (ID: {member.id}) in channel #{channel.name} (ID: {channel.id}) in guild '{guild.name}' (ID: {guild.id}). Error: {e}"
            )

    # =========================
    # Auto react in introductions
    # =========================
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        # Ignore messages sent by bots
        if message.author.bot:
            return

        # Ensure the channel matches the configured introductions channel
        if not self.introductions_channel_id:
            return
        if message.channel.id != self.introductions_channel_id:
            return

        # React when message contains the required phrase
        if "💚 Name:" in message.content:
            try:
                await message.add_reaction("👋")
                logging.info(
                    f"Reacted to message {message.id} in #{message.channel.name}"
                )
                audit_log(
                    f"Reacted with 👋 to message {message.id} in channel #{message.channel.name} (ID: {message.channel.id}) in guild '{message.guild.name}' (ID: {message.guild.id})."
                )
            except discord.HTTPException as e:
                logging.error(
                    f"Failed to react to message {message.id} in #{message.channel.name}: {e}"
                )
                audit_log(
                    f"Error reacting to message {message.id} in channel #{message.channel.name} (ID: {message.channel.id}): {e}"
                )


async def setup(bot: commands.Bot):
    await bot.add_cog(CommunityWelcome(bot))
