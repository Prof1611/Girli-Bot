import discord
import datetime
import logging

import discord
from discord import app_commands
from discord.ext import commands

from config_helpers import get_embed_colours


EMBED_COLOURS = get_embed_colours()


def audit_log(message: str):
    """Append a timestamped message to the audit log file."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("audit.log", "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")


class DMModal(discord.ui.Modal, title="Send a Direct Message"):
    message_input = discord.ui.TextInput(
        label="Message",
        style=discord.TextStyle.long,
        required=True,
        placeholder="Enter your message here...",
    )

    def __init__(self, bot: commands.Bot, user: discord.User, actor: discord.User):
        super().__init__()
        self.bot = bot
        self.user = user
        self.actor = actor

    async def on_submit(self, interaction: discord.Interaction):
        message_value = self.message_input.value

        # Create a processing embed and send it.
        processing_embed = discord.Embed(
            title="Processing DM",
            description="Please wait...",
            color=EMBED_COLOURS["info"],
        )
        await interaction.response.send_message(embed=processing_embed, ephemeral=True)
        original_response = await interaction.original_response()

        # Attempt to send the DM.
        try:
            await self.user.send(content=message_value)
            logging.info(
                f"Direct message successfully sent to '{self.user.name}' (ID: {self.user.id})."
            )
            audit_log(
                f"{self.actor.name} (ID: {self.actor.id}) sent DM to {self.user.name} (ID: {self.user.id})."
            )
            embed = discord.Embed(
                title="DM Sent",
                description=f"Direct message successfully sent to {self.user.mention}!",
                color=EMBED_COLOURS["success"],
            )
        except discord.Forbidden as e:
            logging.error(
                f"Could not send a direct message to '{self.user.name}' (ID: {self.user.id}) (forbidden). Error: {e}"
            )
            audit_log(
                f"{self.actor.name} (ID: {self.actor.id}) failed to send DM to {self.user.name} (ID: {self.user.id}) - forbidden."
            )
            embed = discord.Embed(
                title="Direct Message Blocked",
                description=(
                    "I cannot DM that user because they block messages from bots or shared servers. "
                    "Ask them to open DMs temporarily or contact them another way."
                ),
                color=EMBED_COLOURS["error"],
            )
        except Exception as e:
            logging.error(
                f"Unexpected error while sending DM to '{self.user.name}' (ID: {self.user.id}): {e}"
            )
            audit_log(
                f"{self.actor.name} (ID: {self.actor.id}) encountered unexpected error sending DM to {self.user.name} (ID: {self.user.id}): {e}"
            )
            embed = discord.Embed(
                title="DM Failed",
                description=(
                    "Something went wrong while sending that DM. Please try again shortly."
                ),
                color=EMBED_COLOURS["error"],
            )

        # Edit the original processing embed with the final embed.
        await interaction.followup.edit_message(
            message_id=original_response.id, content="", embed=embed
        )


class Dm(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="dm", description="Send a custom direct message to a user."
    )
    @app_commands.describe(user="The user to send the DM to")
    async def dm_command(self, interaction: discord.Interaction, user: discord.User):
        modal = DMModal(self.bot, user, interaction.user)
        await interaction.response.send_modal(modal)
        audit_log(
            f"{interaction.user.name} (ID: {interaction.user.id}) invoked DM command to send a message to {user.name} (ID: {user.id})."
        )

    @commands.Cog.listener()
    async def on_ready(self):
        logging.info("\033[96mDM\033[0m cog synced successfully.")
        audit_log("DM cog synced successfully.")


async def setup(bot: commands.Bot):
    await bot.add_cog(Dm(bot))
