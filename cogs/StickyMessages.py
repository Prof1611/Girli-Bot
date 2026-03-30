import re
import discord
import logging
import sqlite3
import asyncio
from discord import app_commands
from discord.ext import commands
import datetime
from typing import Optional

# Define an invisible marker for sticky messages using zero-width characters.
STICKY_MARKER = "\u200b\u200c\u200d\u2060"

# How deep to scan when purging old stickies
STICKY_PURGE_SCAN_LIMIT = 500


def audit_log(message: str):
    """Append a timestamped message to the audit log file."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("audit.log", "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")


def make_embed(title: str, description: str, color: discord.Color) -> discord.Embed:
    return discord.Embed(title=title, description=description, color=color)


# Colour picker reused from CustomEmbed, extended to support custom title flow
class ColourSelect(discord.ui.Select):
    def __init__(self, parent_view: "StickyColourPickView"):
        options = [
            discord.SelectOption(label="Default", value="default", description="Black (#000000)"),
            discord.SelectOption(label="Custom Hex", value="custom_hex", description="Enter your own hex code..."),
            discord.SelectOption(label="Random", value="random", description="Pick a random hue"),
            discord.SelectOption(label="Teal", value="teal", description="Aloha (#1ABC9C)"),
            discord.SelectOption(label="Dark Teal", value="dark_teal", description="Blue Green (#11806A)"),
            discord.SelectOption(label="Green", value="green", description="UFO Green (#2ECC71)"),
            discord.SelectOption(label="Blurple", value="blurple", description="Blue Genie (#5865F2)"),
            discord.SelectOption(label="OG Blurple", value="og_blurple", description="Zeus' Temple (#7289DA)"),
            discord.SelectOption(label="Blue", value="blue", description="Dayflower (#3498DB)"),
            discord.SelectOption(label="Dark Blue", value="dark_blue", description="Deep Water (#206694)"),
            discord.SelectOption(label="Purple", value="purple", description="Deep Lilac (#9B59B6)"),
            discord.SelectOption(label="Dark Purple", value="dark_purple", description="Maximum Purple (#71368A)"),
            discord.SelectOption(label="Gold", value="gold", description="Tanned Leather (#F1C40F)"),
            discord.SelectOption(label="Dark Gold", value="dark_gold", description="Tree Sap (#C27C0E)"),
            discord.SelectOption(label="Orange", value="orange", description="Dark Cheddar (#E67E22)"),
            discord.SelectOption(label="Dark Orange", value="dark_orange", description="Pepperoni (#A84300)"),
            discord.SelectOption(label="Red", value="red", description="Carmine Pink (#E74C3C)"),
            discord.SelectOption(label="Dark Red", value="dark_red", description="Red Birch (#992D22)"),
            discord.SelectOption(label="Greyple", value="greyple", description="Irogon Blue (#99AAB5)"),
            discord.SelectOption(label="Light Grey", value="light_grey", description="Harrison Grey (#979C9F)"),
            discord.SelectOption(label="Darker Grey", value="darker_grey", description="Morro Bay (#546E7A)"),
            discord.SelectOption(label="Dark Theme", value="dark_theme", description="Antarctic Deep (transparent)"),
            discord.SelectOption(label="Yellow", value="yellow", description="Corn (#FEE75C)"),
        ]
        super().__init__(
            placeholder="Choose an embed colour...",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.parent_view = parent_view

    async def callback(self, interaction: discord.Interaction):
        choice = self.values[0]
        if choice == "custom_hex":
            await interaction.response.send_modal(
                HexContentModal(
                    self.parent_view.channel,
                    self.parent_view.sticky_cog,
                    self.parent_view.selected_format
                )
            )
        else:
            try:
                factory = getattr(discord.Color, choice)
                self.parent_view.chosen_colour = factory()
            except Exception:
                self.parent_view.chosen_colour = discord.Color.default()
            await interaction.response.send_modal(
                StickyModal(
                    self.parent_view.bot,
                    self.parent_view.sticky_cog,
                    self.parent_view.selected_format,
                    self.parent_view.chosen_colour
                )
            )
        audit_log(f"{interaction.user} picked colour '{choice}' for sticky embed.")


class StickyColourPickView(discord.ui.View):
    def __init__(self, bot, sticky_cog, channel, selected_format):
        super().__init__(timeout=60)
        self.bot = bot
        self.sticky_cog = sticky_cog
        self.channel = channel
        self.selected_format = selected_format  # always "embed" here
        self.chosen_colour = discord.Color.default()
        self.add_item(ColourSelect(self))

    async def on_timeout(self):
        logging.info(f"ColourPickView timed out in #{self.channel.name}")
        audit_log(f"Colour pick dropdown timed out in #{self.channel.name}.")
        for child in self.children:
            child.disabled = True


class HexContentModal(discord.ui.Modal, title="Custom HEX Sticky"):
    hex_code = discord.ui.TextInput(
        label="HEX Code", style=discord.TextStyle.short, required=True,
        placeholder="#RRGGBB or RRGGBB", max_length=7,
    )
    embed_title = discord.ui.TextInput(
        label="Embed Title", style=discord.TextStyle.short, required=False,
        placeholder="Title for the sticky embed..."
    )
    sticky_message = discord.ui.TextInput(
        label="Sticky Message", style=discord.TextStyle.long, required=True,
        placeholder="Enter your sticky message here..."
    )

    def __init__(self, channel, sticky_cog, selected_format):
        super().__init__()
        self.channel = channel
        self.sticky_cog = sticky_cog
        self.selected_format = selected_format

    async def on_submit(self, interaction: discord.Interaction):
        hex_str = self.hex_code.value.strip().lstrip("#")
        if not re.fullmatch(r"[0-9A-Fa-f]{6}", hex_str):
            err = make_embed("Error", "Invalid hex. Must be exactly 6 hex digits.", discord.Color.red())
            return await interaction.response.send_message(embed=err, ephemeral=True)

        colour = discord.Color(int(hex_str, 16))
        title = self.embed_title.value.strip() if self.embed_title.value else ""
        content = self.sticky_message.value

        # Use the cog helper to create or replace the sticky
        try:
            await self.sticky_cog.create_or_replace_sticky(
                interaction=interaction,
                channel=self.channel,
                title=title,
                content=content,
                fmt="embed",
                colour=colour
            )
        except Exception as e:
            logging.error(f"HexContentModal failed: {e}")
            err = make_embed(
                "Error",
                "Something went wrong while setting the sticky message. Please try again later.",
                discord.Color.red(),
            )
            return await interaction.response.send_message(embed=err, ephemeral=True)


class StickyFormatSelect(discord.ui.Select):
    def __init__(self, sticky_cog: "Sticky"):
        options = [
            discord.SelectOption(label="Normal", value="normal", description="Plain text sticky"),
            discord.SelectOption(label="Embed", value="embed", description="Embed sticky with custom colour and title"),
        ]
        super().__init__(placeholder="Choose message format...", min_values=1, max_values=1, options=options)
        self.sticky_cog = sticky_cog

    async def callback(self, interaction: discord.Interaction):
        choice = self.values[0]
        if choice == "normal":
            await interaction.response.send_modal(
                StickyModal(interaction.client, self.sticky_cog, "normal", None)
            )
        else:  # embed
            view = StickyColourPickView(
                interaction.client, self.sticky_cog, interaction.channel, "embed"
            )
            await interaction.response.send_message(
                embed=discord.Embed(
                    description="Choose a colour for your sticky embed:"
                ),
                view=view,
                ephemeral=True,
            )
        audit_log(f"{interaction.user} selected sticky format '{choice}'.")


class StickyFormatView(discord.ui.View):
    def __init__(self, sticky_cog: "Sticky"):
        super().__init__()
        self.add_item(StickyFormatSelect(sticky_cog))


# Modal for both normal and embed stickies
# If embed, a title field is provided and saved
class StickyModal(discord.ui.Modal, title="Set Sticky Message"):
    sticky_title = discord.ui.TextInput(
        label="Embed Title",
        style=discord.TextStyle.short,
        required=False,
        placeholder="Title for the sticky embed..."
    )
    sticky_message = discord.ui.TextInput(
        label="Sticky Message", style=discord.TextStyle.long, required=True,
        placeholder="Enter your sticky message here..."
    )

    def __init__(self, bot: commands.Bot, sticky_cog: "Sticky",
                 selected_format: str, colour: Optional[discord.Color], prefilled_message: str = None, prefilled_title: str = None):
        super().__init__()
        self.bot = bot
        self.sticky_cog = sticky_cog
        self.selected_format = selected_format
        self.colour = colour or discord.Color.blurple()
        if prefilled_message:
            self.sticky_message.default = prefilled_message
        if prefilled_title:
            self.sticky_title.default = prefilled_title

    async def on_submit(self, interaction: discord.Interaction):
        content = self.sticky_message.value
        title = self.sticky_title.value.strip() if (self.selected_format == "embed" and self.sticky_title.value) else ""
        channel = interaction.channel
        await self.sticky_cog.create_or_replace_sticky(
            interaction=interaction,
            channel=channel,
            title=title,
            content=content,
            fmt=self.selected_format,
            colour=self.colour
        )


class Sticky(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.stickies = {}
        self.db = sqlite3.connect("database.db", check_same_thread=False)
        # Ensure table and columns exist. Add title and color if missing.
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS sticky_messages (channel_id INTEGER PRIMARY KEY, title TEXT, content TEXT, message_id INTEGER, format TEXT, color INTEGER DEFAULT 0)"
        )
        cols = [r[1] for r in self.db.execute("PRAGMA table_info(sticky_messages)").fetchall()]
        if "title" not in cols:
            self.db.execute("ALTER TABLE sticky_messages ADD COLUMN title TEXT DEFAULT ''")
        if "color" not in cols:
            self.db.execute("ALTER TABLE sticky_messages ADD COLUMN color INTEGER DEFAULT 0")
        self.db.commit()
        self.load_stickies()
        self.initialised = False
        self.locks = {}
        self.debounce_tasks = {}
        self.debounce_interval = 1.0

    def load_stickies(self):
        self.stickies = {}
        cursor = self.db.execute(
            "SELECT channel_id, title, content, message_id, format, color FROM sticky_messages"
        )
        for row in cursor.fetchall():
            self.stickies[int(row[0])] = {
                "title": row[1] or "",
                "content": row[2],
                "message_id": row[3],
                "format": row[4],
                "color": row[5],
            }

    def update_sticky_in_db(self, channel_id: int, title: str, content: str, message_id: int, fmt: str, colour: int):
        self.db.execute(
            "INSERT OR REPLACE INTO sticky_messages (channel_id, title, content, message_id, format, color) VALUES (?, ?, ?, ?, ?, ?)",
            (channel_id, title, content, message_id, fmt, colour),
        )
        self.db.commit()

    def delete_sticky_from_db(self, channel_id: int):
        self.db.execute(
            "DELETE FROM sticky_messages WHERE channel_id = ?", (channel_id,)
        )
        self.db.commit()

    @staticmethod
    def _message_is_sticky(bot_user: discord.User, msg: discord.Message) -> bool:
        """Detect if a message is a sticky created by this bot."""
        if msg.author.id != bot_user.id:
            return False
        try:
            if msg.content and STICKY_MARKER in msg.content:
                return True
            for emb in msg.embeds:
                if emb.description and STICKY_MARKER in emb.description:
                    return True
        except Exception:
            return False
        return False

    async def _purge_old_stickies(self, channel: discord.TextChannel, keep_id: Optional[int] = None):
        """Delete all older sticky messages in the channel created by this bot, except keep_id."""
        deleted = 0
        perms = channel.permissions_for(channel.guild.me)
        # We can always delete our own messages. Manage Messages only needed if trying to delete others.
        async for msg in channel.history(limit=STICKY_PURGE_SCAN_LIMIT):
            if self._message_is_sticky(self.bot.user, msg):
                if keep_id is not None and msg.id == keep_id:
                    continue
                try:
                    await msg.delete()
                    deleted += 1
                except discord.Forbidden:
                    # Should not happen for own messages, but log if it does
                    logging.warning(f"Forbidden trying to delete sticky in #{channel.name}. Check permissions.")
                except Exception as e:
                    logging.error(f"Error deleting old sticky in #{channel.name}: {e}")
        if deleted:
            audit_log(f"Purged {deleted} old sticky messages in #{channel.name}.")

    async def update_sticky_for_channel(self, channel: discord.abc.Messageable, sticky: dict, force_update: bool = False):
        if not isinstance(channel, discord.TextChannel):
            logging.warning(f"Channel {channel} is not a TextChannel. Skipping sticky update.")
            return

        perms = channel.permissions_for(channel.guild.me)
        if not perms.send_messages:
            logging.warning(f"Missing Send Messages in #{channel.name}. Cannot update sticky.")
            return

        lock = self.locks.setdefault(channel.id, asyncio.Lock())
        async with lock:
            # Check most recent message to avoid unnecessary reposts
            try:
                latest: Optional[discord.Message] = None
                async for m in channel.history(limit=1):
                    latest = m
                    break
            except Exception as e:
                logging.error(f"Failed to fetch latest message in #{channel.name}: {e}")
                latest = None

            if latest and not force_update:
                # If the latest message is our sticky, just purge older ones and exit
                if self._message_is_sticky(self.bot.user, latest):
                    await self._purge_old_stickies(channel, keep_id=latest.id)
                    return

            # Otherwise delete the previous tracked sticky if it exists, then send a new one
            try:
                if sticky.get("message_id"):
                    try:
                        old_message = await channel.fetch_message(int(sticky["message_id"]))
                        await old_message.delete()
                    except discord.NotFound:
                        pass
                    except Exception as e:
                        logging.error(f"Error deleting old sticky in channel #{channel.name}: {e}")

                fmt = sticky.get("format", "normal")
                colour = sticky.get("color", discord.Color.blurple().value)
                title = sticky.get("title", "") or ""
                new_sticky = await self._send_sticky(channel, title, sticky["content"], fmt, colour)

                self.stickies[channel.id] = {
                    "title":     title,
                    "content":   sticky["content"],
                    "message_id": new_sticky.id,
                    "format":    fmt,
                    "color":     colour,
                }
                self.update_sticky_in_db(channel.id, title, sticky["content"], new_sticky.id, fmt, colour)

                # After sending, purge any other lingering stickies
                await self._purge_old_stickies(channel, keep_id=new_sticky.id)

            except Exception as e:
                logging.error(f"Error updating sticky in channel #{channel.name}: {e}")

    async def _debounced_update(self, channel: discord.abc.Messageable, sticky: dict):
        try:
            await asyncio.sleep(self.debounce_interval)
            await self.update_sticky_for_channel(channel, sticky, force_update=False)
        finally:
            self.debounce_tasks.pop(channel.id, None)

    async def _send_sticky(self, channel: discord.TextChannel, title: str, content: str, fmt: str, colour_value: int):
        if fmt == "embed":
            embed = discord.Embed(
                title=title or "Sticky Message",
                description=f"{content}{STICKY_MARKER}",
                color=discord.Color(colour_value)
            )
            return await channel.send(embed=embed)
        else:
            return await channel.send(f"{content}{STICKY_MARKER}")

    async def create_or_replace_sticky(self, interaction: discord.Interaction, channel: discord.TextChannel, title: str, content: str, fmt: str, colour: discord.Color):
        perms = channel.permissions_for(interaction.guild.me)
        if not perms.send_messages:
            err = make_embed("Error", "I do not have permission to send messages in this channel.", discord.Color.red())
            return await interaction.response.send_message(embed=err, ephemeral=True)
        if fmt == "embed" and not perms.embed_links:
            err = make_embed("Error", "I do not have permission to embed links in this channel.", discord.Color.red())
            return await interaction.response.send_message(embed=err, ephemeral=True)

        # Remove tracked sticky if present
        if channel.id in self.stickies and self.stickies[channel.id].get("message_id"):
            try:
                old_msg = await channel.fetch_message(int(self.stickies[channel.id]["message_id"]))
                await old_msg.delete()
            except Exception:
                pass

        # Send new sticky
        if fmt == "embed":
            sent = await self._send_sticky(channel, title, content, "embed", colour.value)
            colour_value = colour.value
        else:
            sent = await self._send_sticky(channel, "", content, "normal", 0)
            colour_value = 0

        # Save memory and DB
        self.stickies[channel.id] = {
            "title":      title if fmt == "embed" else "",
            "content":    content,
            "message_id": sent.id,
            "format":     fmt,
            "color":      colour_value,
        }
        self.update_sticky_in_db(channel.id, self.stickies[channel.id]["title"], content, sent.id, fmt, colour_value)

        # Purge any lingering older stickies in history
        await self._purge_old_stickies(channel, keep_id=sent.id)

        ok = make_embed("Sticky Set", f"Sticky successfully set in {channel.mention}.", discord.Color.green())
        await interaction.response.send_message(embed=ok, ephemeral=True)
        audit_log(f"{interaction.user} set a '{fmt}' sticky in #{channel.name}.")

    @commands.Cog.listener()
    async def on_ready(self):
        logging.info("\033[96mSticky\033[0m cog synced successfully.")
        audit_log("Sticky cog synced successfully.")
        for channel_id, sticky in list(self.stickies.items()):
            channel = self.bot.get_channel(int(channel_id))
            if channel:
                await self.update_sticky_for_channel(channel, sticky, force_update=False)
        self.initialised = True

    @commands.Cog.listener()
    async def on_resumed(self):
        logging.info("Bot resumed. Updating sticky messages in all channels.")
        audit_log("Bot resumed: Updating sticky messages in all channels.")
        for channel_id, sticky in list(self.stickies.items()):
            channel = self.bot.get_channel(int(channel_id))
            if channel:
                await self.update_sticky_for_channel(channel, sticky, force_update=False)
        self.initialised = True

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author == self.bot.user:
            return
        channel = message.channel
        if channel.id in self.stickies:
            if channel.id in self.debounce_tasks:
                return
            self.debounce_tasks[channel.id] = self.bot.loop.create_task(
                self._debounced_update(channel, self.stickies[channel.id])
            )

    @commands.Cog.listener()
    async def on_message_delete(self, message: discord.Message):
        # If the bot's sticky was manually deleted, cancel pending debounce and re-post immediately
        if message.author == self.bot.user and message.channel.id in self.stickies:
            sticky = self.stickies[message.channel.id]
            if message.id == sticky.get("message_id"):
                task = self.debounce_tasks.pop(message.channel.id, None)
                if task:
                    task.cancel()
                await self.update_sticky_for_channel(message.channel, sticky, force_update=True)

    @app_commands.command(name="setsticky", description="Set a sticky message in the channel.")
    async def set_sticky(self, interaction: discord.Interaction):
        view = StickyFormatView(self)
        await interaction.response.send_message(
            embed=discord.Embed(description="Choose the sticky message format:"),
            view=view,
            ephemeral=True,
        )
        audit_log(
            f"{interaction.user} invoked /setsticky in channel #{interaction.channel.name}."
        )

    @app_commands.command(name="removesticky", description="Remove the sticky message in the channel.")
    async def remove_sticky(self, interaction: discord.Interaction):
        channel = interaction.guild.get_channel(interaction.channel.id)
        if channel.id not in self.stickies:
            err = make_embed("Error", f"No sticky found in {channel.mention}.", discord.Color.red())
            return await interaction.response.send_message(embed=err, ephemeral=True)

        try:
            old_msg = await channel.fetch_message(int(self.stickies[channel.id].get("message_id", 0)))
            await old_msg.delete()
        except Exception:
            pass
        self.delete_sticky_from_db(channel.id)
        self.stickies.pop(channel.id, None)

        ok = make_embed("Sticky Removed", f"Removed sticky from {channel.mention}.", discord.Color.green())
        await interaction.response.send_message(embed=ok, ephemeral=True)
        audit_log(f"{interaction.user} removed sticky in #{channel.name}.")


async def setup(bot: commands.Bot):
    await bot.add_cog(Sticky(bot))
