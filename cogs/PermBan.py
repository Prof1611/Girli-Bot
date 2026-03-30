import datetime
import logging

import discord
from discord import app_commands
from discord.ext import commands

from config_helpers import get_embed_colours, load_config


class Ban(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        # Load configuration for logging and embed appearance.
        try:
            self.config = load_config()
        except Exception:
            self.config = {}

        channels = self.config.get("channels", {})
        self.logs_channel_id = channels.get("moderation_logs_channel_id")

        embed_colours = get_embed_colours()
        self.success_colour = embed_colours["success"]
        self.info_colour = embed_colours["info"]
        self.error_colour = embed_colours["error"]

    def audit_log(self, message: str):
        """Append a timestamped message to the audit log file."""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open("audit.log", "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] {message}\n")

    @commands.Cog.listener()
    async def on_ready(self):
        logging.info(f"\033[96mBan\033[0m cog synced successfully.")
        self.audit_log("Ban cog synced successfully.")

    @app_commands.command(
        name="ban", description="Bans a member and sends them a notice via DM."
    )
    @app_commands.describe(
        user="The member to ban", reason="The reason for banning the member"
    )
    async def ban(
        self, interaction: discord.Interaction, user: discord.Member, *, reason: str
    ):
        # Defer the response to avoid timeout errors
        await interaction.response.defer()

        moderator = interaction.user  # actor performing the action
        guild = interaction.guild
        guild_name = guild.name if guild else "this Discord server"

        try:
            try:
                dm_text = f"""**NOTICE: Permanent Ban from {guild_name} Discord Server**

Dear {user.mention},

We are writing to inform you that you have been permanently banned from the {guild_name} Discord server. This action is a result of your repeated violations of our community rules, despite previous warnings and attempts to rectify the situation.

**Reason for Ban:** {reason}

We take upholding the standards of our community very seriously, and continued disruptive or disrespectful behaviour will not be tolerated. This ban ensures a positive and respectful environment for all users.

**Appeal Process:** If you wish to appeal this ban, you may do so by filling out the following form: https://example.com/appeal-form

Sincerely,
{guild_name} Moderation Team
*Please do not reply to this message as the staff team will not see it.*"""
                await user.send(dm_text)
                logging.info(
                    f"Successfully sent permanent ban notice to {user.name} (ID: {user.id}) via DM."
                )
                self.audit_log(
                    f"{moderator.name} (ID: {moderator.id}) sent permanent ban notice via DM to {user.name} (ID: {user.id})."
                )
            except discord.HTTPException as e:
                if e.status == 403:  # DMs Disabled
                    logging.error(
                        f"DMs disabled when attempting to send ban notice via DM. Error: {e}"
                    )
                    self.audit_log(
                        f"{moderator.name} (ID: {moderator.id}) failed to send DM notice to {user.name} (ID: {user.id}); DMs disabled."
                    )
                    embed = discord.Embed(
                        title="Direct Message Blocked",
                        description=(
                            "I could not send the ban notice because that member blocks server DMs. "
                            "Please let them know through another channel."
                        ),
                        color=self.error_colour,
                    )
                    await interaction.followup.send(embed=embed)
                else:
                    logging.error(
                        f"Error when attempting to send ban notice via DM: {e}"
                    )
                    self.audit_log(
                        f"{moderator.name} (ID: {moderator.id}) error sending DM notice to {user.name} (ID: {user.id}): {e}"
                    )
                    embed = discord.Embed(
                        title="Direct Message Failed",
                        description=(
                            f"I could not send the ban notice to {user.mention}. "
                            "Please check if they accept DMs or reach out manually."
                        ),
                        color=self.error_colour,
                    )
                    await interaction.followup.send(embed=embed)
            try:
                # Try ban the user from the server
                await user.ban(reason=reason, delete_message_days=0)
                guild = interaction.guild
                logging.info(
                    f"Permanently banned {user.name} (ID: {user.id}) from '{guild.name}' (ID: {guild.id})."
                )
                self.audit_log(
                    f"{moderator.name} (ID: {moderator.id}) permanently banned {user.name} (ID: {user.id}) from guild '{guild.name}' (ID: {guild.id}) for reason: {reason}."
                )
                embed = discord.Embed(
                    title="Member Banned",
                    description=f"Permanently banned {user.mention} from the server and sent them a notice via DM.",
                    color=self.success_colour,
                )
                await interaction.followup.send(embed=embed)
            except discord.HTTPException as e:
                if e.status == 403:  # Bot has no permission to ban
                    logging.error(f"No permission to ban. Error: {e}")
                    self.audit_log(
                        f"{moderator.name} (ID: {moderator.id}) failed to ban {user.name} (ID: {user.id}) - insufficient permissions in guild '{guild.name}' (ID: {guild.id})."
                    )
                    embed = discord.Embed(
                        title="Missing Permission",
                        description=(
                            "I cannot ban members with my current permissions or role order. "
                            "Please grant me the Ban Members permission and move my role above the member being banned."
                        ),
                        color=self.error_colour,
                    )
                    await interaction.followup.send(embed=embed)
                else:
                    logging.error(
                        f"Error when attempting to ban {user.name}. Error: {e}"
                    )
                    self.audit_log(
                        f"{moderator.name} (ID: {moderator.id}) error banning {user.name} (ID: {user.id}) in guild '{guild.name}' (ID: {guild.id}): {e}"
                    )
                    embed = discord.Embed(
                        title="Ban Failed",
                        description=(
                            f"I could not remove {user.mention}. Please try again later or review the audit log for details."
                        ),
                        color=self.error_colour,
                    )
                    await interaction.followup.send(embed=embed)

                # Log the moderation action in the log channel
                logs_channel_id = self.logs_channel_id
                guild = interaction.guild
                if not logs_channel_id:
                    embed = discord.Embed(
                        title="Log Channel Not Configured",
                        description=(
                            "Set `channels.moderation_logs_channel_id` in config.yaml so I can post ban logs."
                        ),
                        color=self.error_colour,
                    )
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    return

                logs_channel = guild.get_channel(logs_channel_id)
                log_link = f"https://discord.com/channels/{guild.id}/{logs_channel_id}"
                if logs_channel:
                    try:
                        log_message = f"""**Username:** {user.mention}
**User ID:** {user.id}
**Category of Discipline:** Permanent Ban
**Timespan of Discipline:** Permanent
**Reason of Discipline:** {reason}
**Link to Ticket Transcript:** N/A
**Date of Discipline:** {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
**Moderators Involved:** {moderator.mention}"""
                        await logs_channel.send(log_message)
                        logging.info(
                            f"Permanent ban logged in '#{logs_channel.name}' (ID: {logs_channel.id})."
                        )
                        self.audit_log(
                            f"{moderator.name} (ID: {moderator.id}) logged permanent ban for {user.name} (ID: {user.id}) in log channel #{logs_channel.name} (ID: {logs_channel.id})."
                        )
                        embed = discord.Embed(
                            title="Action Logged",
                            description=f"Permanent ban successfully logged in {log_link}.",
                            color=self.success_colour,
                        )
                        await interaction.followup.send(embed=embed, ephemeral=True)
                    except discord.HTTPException as e:
                        if e.status == 403:
                            logging.error(
                                f"No access to '#{logs_channel.name}' (ID: {logs_channel.id}). Error: {e}"
                            )
                            self.audit_log(
                                f"{moderator.name} (ID: {moderator.id}) failed to log action in log channel #{logs_channel.name} (ID: {logs_channel.id}) for {user.name} (ID: {user.id}); no access."
                            )
                            embed = discord.Embed(
                                title="Log Channel Permission Needed",
                                description=(
                                    f"I cannot post in {log_link}. Give me Send Messages permission or update the log channel ID."
                                ),
                                color=self.error_colour,
                            )
                            await interaction.followup.send(embed=embed)
                        elif e.status == 404:
                            logging.error(f"Channel not found. Error: {e}")
                            self.audit_log(
                                f"{moderator.name} (ID: {moderator.id}) failed to log action; log channel not found for {user.name} (ID: {user.id})."
                            )
                            embed = discord.Embed(
                                title="Log Channel Missing",
                                description=(
                                    "The configured log channel no longer exists. Update the channel ID in config.yaml or create a new log channel."
                                ),
                                color=self.error_colour,
                            )
                            await interaction.followup.send(embed=embed)
                        elif e.status == 429:
                            logging.error(f"RATE LIMIT. Error: {e}")
                            self.audit_log(
                                f"{moderator.name} (ID: {moderator.id}) encountered rate limit when logging ban for {user.name} (ID: {user.id})."
                            )
                            embed = discord.Embed(
                                title="Rate Limited",
                                description=(
                                    "Discord asked me to slow down. Please wait a few seconds and try again."
                                ),
                                color=self.error_colour,
                            )
                            await interaction.followup.send(embed=embed)
                        elif e.status in {500, 502, 503, 504}:
                            logging.error(f"Discord API Error. Error: {e}")
                            self.audit_log(
                                f"{moderator.name} (ID: {moderator.id}) encountered Discord API error when logging ban for {user.name} (ID: {user.id}): {e}"
                            )
                            embed = discord.Embed(
                                title="Log Failed",
                                description=(
                                    f"Discord would not accept my log message for {log_link}. Please try again later."
                                ),
                                color=self.error_colour,
                            )
                            await interaction.followup.send(embed=embed)
                        else:
                            logging.error(
                                f"Failed to log ban in '#{logs_channel.name}' (ID: {logs_channel.id}). Error: {e}"
                            )
                            self.audit_log(
                                f"{moderator.name} (ID: {moderator.id}) unknown error when logging ban for {user.name} (ID: {user.id}) in log channel #{logs_channel.name} (ID: {logs_channel.id}): {e}"
                            )
                            embed = discord.Embed(
                                title="Log Failed",
                                description=(
                                    f"I could not post in {log_link}. Check my permissions or try later."
                                ),
                                color=self.error_colour,
                            )
                            await interaction.followup.send(embed=embed)
        except discord.HTTPException as e:
            logging.error(f"Error when attempting to ban: {e}")
            self.audit_log(
                f"{moderator.name} (ID: {moderator.id}) critical error: Failed to ban and send notice to {user.name} (ID: {user.id}): {e}"
            )
            embed = discord.Embed(
                title="Ban Failed",
                description=(
                    f"Something went wrong while banning {user.mention}. Please check my permissions and try again."
                ),
                color=self.error_colour,
            )
            await interaction.followup.send(embed=embed)


async def setup(bot: commands.Bot):
    await bot.add_cog(Ban(bot))
