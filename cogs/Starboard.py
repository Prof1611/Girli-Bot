from __future__ import annotations

from typing import Optional, List, Tuple
import datetime
import logging
import sqlite3

import discord
from discord import app_commands
from discord.ext import commands

from config_helpers import get_embed_colours, load_config

# ============================================================
# Database setup
# ============================================================
conn = sqlite3.connect("database.db", check_same_thread=False)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS starboard_settings (
        guild_id INTEGER PRIMARY KEY,
        channel_id INTEGER,
        threshold INTEGER NOT NULL DEFAULT 3,
        emoji TEXT NOT NULL DEFAULT '⭐',
        allow_self_star INTEGER NOT NULL DEFAULT 0,
        allow_bot_messages INTEGER NOT NULL DEFAULT 0,
        ignore_nsfw INTEGER NOT NULL DEFAULT 1,
        is_enabled INTEGER NOT NULL DEFAULT 0
    )
    """
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS starboard_posts (
        guild_id INTEGER NOT NULL,
        source_channel_id INTEGER NOT NULL,
        source_message_id INTEGER NOT NULL,
        source_author_id INTEGER,
        starboard_channel_id INTEGER,
        starboard_message_id INTEGER,
        star_count INTEGER NOT NULL DEFAULT 0,
        last_known_content TEXT,
        last_updated_at INTEGER NOT NULL,
        PRIMARY KEY (guild_id, source_message_id)
    )
    """
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS starboard_starrers (
        guild_id INTEGER NOT NULL,
        source_message_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        PRIMARY KEY (guild_id, source_message_id, user_id)
    )
    """
)

conn.commit()


def _column_names(table: str) -> List[str]:
    cursor.execute(f"PRAGMA table_info({table})")
    return [row["name"] for row in cursor.fetchall()]


def _ensure_schema() -> None:
    settings_cols = _column_names("starboard_settings")
    if "is_enabled" not in settings_cols:
        cursor.execute(
            "ALTER TABLE starboard_settings ADD COLUMN is_enabled INTEGER NOT NULL DEFAULT 0"
        )

    posts_cols = _column_names("starboard_posts")
    if "last_known_content" not in posts_cols:
        cursor.execute("ALTER TABLE starboard_posts ADD COLUMN last_known_content TEXT")
    if "last_updated_at" not in posts_cols:
        cursor.execute(
            "ALTER TABLE starboard_posts ADD COLUMN last_updated_at INTEGER NOT NULL DEFAULT 0"
        )

    cursor.execute("UPDATE starboard_settings SET is_enabled = COALESCE(is_enabled, 0)")
    cursor.execute(
        "UPDATE starboard_posts SET last_updated_at = COALESCE(last_updated_at, 0)"
    )
    conn.commit()


_ensure_schema()


def audit_log(message: str) -> None:
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("audit.log", "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")


def unix_now() -> int:
    return int(datetime.datetime.now(datetime.timezone.utc).timestamp())


def emoji_to_storage_value(emoji: str) -> str:
    return emoji.strip()


def is_custom_emoji_string(value: str) -> bool:
    return value.startswith("<:") or value.startswith("<a:")


class Starboard(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

        self.config = {}
        self.defaults = {
            "channel_id": None,
            "threshold": 3,
            "emoji": "⭐",
            "allow_self_star": False,
            "allow_bot_messages": False,
            "ignore_nsfw": True,
        }

        try:
            self.config = load_config()
            sb = self.config.get("features", {}).get("starboard", {})
            self.defaults["channel_id"] = (
                int(sb["channel_id"]) if sb.get("channel_id") else None
            )
            self.defaults["threshold"] = int(sb.get("threshold", 3))
            self.defaults["emoji"] = str(sb.get("emoji", "⭐"))
            self.defaults["allow_self_star"] = bool(sb.get("allow_self_star", False))
            self.defaults["allow_bot_messages"] = bool(
                sb.get("allow_bot_messages", False)
            )
            self.defaults["ignore_nsfw"] = bool(sb.get("ignore_nsfw", True))
        except Exception as e:
            logging.warning(
                f"Starboard: failed to load config.yaml, using defaults. {e}"
            )

        colours = get_embed_colours()
        self.success_colour = colours["success"]
        self.info_colour = colours["info"]
        self.error_colour = colours["error"]

    # --------------------------------------------------------
    # Basic helpers
    # --------------------------------------------------------
    def _embed(
        self, title: str, description: str, colour: discord.Color
    ) -> discord.Embed:
        return discord.Embed(title=title, description=description, color=colour)

    def _is_manager(self, member: discord.Member) -> bool:
        return (
            member.guild_permissions.administrator
            or member.guild_permissions.manage_guild
        )

    def _get_settings(self, guild_id: int) -> sqlite3.Row:
        cursor.execute(
            "SELECT * FROM starboard_settings WHERE guild_id = ?",
            (guild_id,),
        )
        row = cursor.fetchone()
        if row:
            return row

        cursor.execute(
            """
            INSERT INTO starboard_settings
                (guild_id, channel_id, threshold, emoji, allow_self_star, allow_bot_messages, ignore_nsfw, is_enabled)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                guild_id,
                self.defaults["channel_id"],
                self.defaults["threshold"],
                self.defaults["emoji"],
                1 if self.defaults["allow_self_star"] else 0,
                1 if self.defaults["allow_bot_messages"] else 0,
                1 if self.defaults["ignore_nsfw"] else 0,
                1 if self.defaults["channel_id"] else 0,
            ),
        )
        conn.commit()
        cursor.execute(
            "SELECT * FROM starboard_settings WHERE guild_id = ?",
            (guild_id,),
        )
        return cursor.fetchone()

    def _update_settings(
        self,
        guild_id: int,
        *,
        channel_id: Optional[int] = None,
        threshold: Optional[int] = None,
        emoji: Optional[str] = None,
        allow_self_star: Optional[bool] = None,
        allow_bot_messages: Optional[bool] = None,
        ignore_nsfw: Optional[bool] = None,
        is_enabled: Optional[bool] = None,
    ) -> None:
        self._get_settings(guild_id)

        fields = []
        values = []

        if channel_id is not None:
            fields.append("channel_id = ?")
            values.append(channel_id)
        if threshold is not None:
            fields.append("threshold = ?")
            values.append(threshold)
        if emoji is not None:
            fields.append("emoji = ?")
            values.append(emoji)
        if allow_self_star is not None:
            fields.append("allow_self_star = ?")
            values.append(1 if allow_self_star else 0)
        if allow_bot_messages is not None:
            fields.append("allow_bot_messages = ?")
            values.append(1 if allow_bot_messages else 0)
        if ignore_nsfw is not None:
            fields.append("ignore_nsfw = ?")
            values.append(1 if ignore_nsfw else 0)
        if is_enabled is not None:
            fields.append("is_enabled = ?")
            values.append(1 if is_enabled else 0)

        if not fields:
            return

        values.append(guild_id)
        cursor.execute(
            f"UPDATE starboard_settings SET {', '.join(fields)} WHERE guild_id = ?",
            tuple(values),
        )
        conn.commit()

    def _fetch_post(
        self, guild_id: int, source_message_id: int
    ) -> Optional[sqlite3.Row]:
        cursor.execute(
            """
            SELECT * FROM starboard_posts
            WHERE guild_id = ? AND source_message_id = ?
            """,
            (guild_id, source_message_id),
        )
        return cursor.fetchone()

    def _upsert_post(
        self,
        guild_id: int,
        source_channel_id: int,
        source_message_id: int,
        source_author_id: Optional[int],
        starboard_channel_id: Optional[int],
        starboard_message_id: Optional[int],
        star_count: int,
        last_known_content: Optional[str],
    ) -> None:
        cursor.execute(
            """
            INSERT INTO starboard_posts
                (guild_id, source_channel_id, source_message_id, source_author_id, starboard_channel_id, starboard_message_id, star_count, last_known_content, last_updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, source_message_id) DO UPDATE SET
                source_channel_id = excluded.source_channel_id,
                source_author_id = excluded.source_author_id,
                starboard_channel_id = excluded.starboard_channel_id,
                starboard_message_id = excluded.starboard_message_id,
                star_count = excluded.star_count,
                last_known_content = excluded.last_known_content,
                last_updated_at = excluded.last_updated_at
            """,
            (
                guild_id,
                source_channel_id,
                source_message_id,
                source_author_id,
                starboard_channel_id,
                starboard_message_id,
                star_count,
                last_known_content,
                unix_now(),
            ),
        )
        conn.commit()

    def _delete_post_record(self, guild_id: int, source_message_id: int) -> None:
        cursor.execute(
            """
            DELETE FROM starboard_posts
            WHERE guild_id = ? AND source_message_id = ?
            """,
            (guild_id, source_message_id),
        )
        cursor.execute(
            """
            DELETE FROM starboard_starrers
            WHERE guild_id = ? AND source_message_id = ?
            """,
            (guild_id, source_message_id),
        )
        conn.commit()

    def _get_starrer_ids(self, guild_id: int, source_message_id: int) -> List[int]:
        cursor.execute(
            """
            SELECT user_id FROM starboard_starrers
            WHERE guild_id = ? AND source_message_id = ?
            ORDER BY user_id ASC
            """,
            (guild_id, source_message_id),
        )
        return [row["user_id"] for row in cursor.fetchall()]

    def _has_starrer(self, guild_id: int, source_message_id: int, user_id: int) -> bool:
        cursor.execute(
            """
            SELECT 1 FROM starboard_starrers
            WHERE guild_id = ? AND source_message_id = ? AND user_id = ?
            """,
            (guild_id, source_message_id, user_id),
        )
        return cursor.fetchone() is not None

    def _add_starrer(self, guild_id: int, source_message_id: int, user_id: int) -> None:
        cursor.execute(
            """
            INSERT OR IGNORE INTO starboard_starrers
                (guild_id, source_message_id, user_id)
            VALUES (?, ?, ?)
            """,
            (guild_id, source_message_id, user_id),
        )
        conn.commit()

    def _remove_starrer(
        self, guild_id: int, source_message_id: int, user_id: int
    ) -> None:
        cursor.execute(
            """
            DELETE FROM starboard_starrers
            WHERE guild_id = ? AND source_message_id = ? AND user_id = ?
            """,
            (guild_id, source_message_id, user_id),
        )
        conn.commit()

    def _clear_starrers(self, guild_id: int, source_message_id: int) -> None:
        cursor.execute(
            """
            DELETE FROM starboard_starrers
            WHERE guild_id = ? AND source_message_id = ?
            """,
            (guild_id, source_message_id),
        )
        conn.commit()

    def _count_starrers(self, guild_id: int, source_message_id: int) -> int:
        cursor.execute(
            """
            SELECT COUNT(*) FROM starboard_starrers
            WHERE guild_id = ? AND source_message_id = ?
            """,
            (guild_id, source_message_id),
        )
        return int(cursor.fetchone()[0])

    def _emoji_matches(self, reaction_emoji: object, configured_emoji: str) -> bool:
        if isinstance(reaction_emoji, discord.PartialEmoji):
            if reaction_emoji.is_custom_emoji():
                if is_custom_emoji_string(configured_emoji):
                    return str(reaction_emoji) == configured_emoji
                return reaction_emoji.name == configured_emoji
            return reaction_emoji.name == configured_emoji

        return str(reaction_emoji) == configured_emoji

    async def _fetch_channel_safe(
        self, guild: discord.Guild, channel_id: int
    ) -> Optional[discord.abc.GuildChannel]:
        channel = guild.get_channel(channel_id)
        if channel is not None:
            return channel
        try:
            fetched = await guild.fetch_channel(channel_id)
            return fetched
        except Exception:
            return None

    async def _fetch_message_safe(
        self, guild: discord.Guild, channel_id: int, message_id: int
    ) -> Optional[discord.Message]:
        channel = await self._fetch_channel_safe(guild, channel_id)
        if channel is None or not isinstance(channel, discord.abc.Messageable):
            return None
        try:
            return await channel.fetch_message(message_id)
        except Exception:
            return None

    async def _find_message_from_payload(
        self, payload: discord.RawReactionActionEvent
    ) -> Tuple[Optional[discord.Guild], Optional[discord.Message]]:
        if payload.guild_id is None:
            return None, None

        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            try:
                guild = await self.bot.fetch_guild(payload.guild_id)
            except Exception:
                return None, None

        message = await self._fetch_message_safe(
            guild, payload.channel_id, payload.message_id
        )
        return guild, message

    async def _find_message_by_ids(
        self, guild_id: int, channel_id: int, message_id: int
    ) -> Tuple[Optional[discord.Guild], Optional[discord.Message]]:
        guild = self.bot.get_guild(guild_id)
        if guild is None:
            try:
                guild = await self.bot.fetch_guild(guild_id)
            except Exception:
                return None, None

        message = await self._fetch_message_safe(guild, channel_id, message_id)
        return guild, message

    def _build_starboard_embed(
        self,
        source_message: discord.Message,
        star_count: int,
        star_emoji: str,
    ) -> discord.Embed:
        description = source_message.content or "*No text content*"
        if len(description) > 4000:
            description = description[:3997] + "..."

        embed = discord.Embed(
            description=description,
            color=self.info_colour,
            timestamp=source_message.created_at,
        )
        embed.set_author(
            name=str(source_message.author),
            icon_url=source_message.author.display_avatar.url,
        )
        embed.add_field(
            name="Source",
            value=f"[Jump to message]({source_message.jump_url})",
            inline=False,
        )
        embed.set_footer(
            text=f"{star_emoji} {star_count} | #{getattr(source_message.channel, 'name', 'unknown')}"
        )

        if source_message.attachments:
            first_attachment = source_message.attachments[0]
            if (
                first_attachment.content_type
                and first_attachment.content_type.startswith("image/")
            ):
                embed.set_image(url=first_attachment.url)
            else:
                attachment_lines = []
                for attachment in source_message.attachments[:5]:
                    attachment_lines.append(
                        f"[{attachment.filename}]({attachment.url})"
                    )
                if attachment_lines:
                    embed.add_field(
                        name="Attachments",
                        value="\n".join(attachment_lines),
                        inline=False,
                    )

        if source_message.embeds:
            for source_embed in source_message.embeds:
                if source_embed.image and source_embed.image.url:
                    if not embed.image.url:
                        embed.set_image(url=source_embed.image.url)
                    break
                if source_embed.thumbnail and source_embed.thumbnail.url:
                    if not embed.thumbnail:
                        embed.set_thumbnail(url=source_embed.thumbnail.url)
                    break

        if source_message.stickers:
            sticker = source_message.stickers[0]
            if getattr(sticker, "url", None) and not embed.image.url:
                embed.set_image(url=sticker.url)

        return embed

    def _build_starboard_content(
        self,
        source_message: discord.Message,
        star_count: int,
        star_emoji: str,
    ) -> str:
        channel_mention = getattr(source_message.channel, "mention", "#unknown")
        return f"{star_emoji} **{star_count}** {channel_mention}"

    async def _validate_source_message(
        self,
        guild: discord.Guild,
        message: discord.Message,
        settings: sqlite3.Row,
        acting_user_id: Optional[int] = None,
    ) -> Tuple[bool, Optional[str]]:
        if message.guild is None or message.guild.id != guild.id:
            return False, "Message is not from this server."

        if message.author is None:
            return False, "Message author could not be resolved."

        if message.author.bot and not bool(settings["allow_bot_messages"]):
            return False, "Bot messages are not allowed on the starboard."

        if acting_user_id is not None:
            if acting_user_id == message.author.id and not bool(
                settings["allow_self_star"]
            ):
                return False, "Self-stars are disabled."

        if message.type not in (
            discord.MessageType.default,
            discord.MessageType.reply,
            discord.MessageType.chat_input_command,
            discord.MessageType.context_menu_command,
            discord.MessageType.thread_starter_message,
        ):
            return False, "That message type is not supported."

        if settings["channel_id"] and message.channel.id == int(settings["channel_id"]):
            return False, "Messages already in the starboard channel cannot be starred."

        if bool(settings["ignore_nsfw"]):
            source_is_nsfw = getattr(message.channel, "is_nsfw", lambda: False)()
            starboard_channel = await self._fetch_channel_safe(
                guild, int(settings["channel_id"])
            )
            starboard_is_nsfw = (
                getattr(starboard_channel, "is_nsfw", lambda: False)()
                if starboard_channel is not None
                else False
            )
            if source_is_nsfw and not starboard_is_nsfw:
                return (
                    False,
                    "NSFW source messages cannot be posted into a non-NSFW starboard.",
                )

        return True, None

    async def _ensure_starboard_channel(
        self, guild: discord.Guild, settings: sqlite3.Row
    ) -> Optional[discord.TextChannel]:
        channel_id = settings["channel_id"]
        if not channel_id:
            return None

        channel = await self._fetch_channel_safe(guild, int(channel_id))
        if channel is None or not isinstance(channel, discord.TextChannel):
            return None

        me = guild.me
        if me is None:
            try:
                me = await guild.fetch_member(self.bot.user.id)
            except Exception:
                return None

        perms = channel.permissions_for(me)
        if not perms.view_channel or not perms.send_messages or not perms.embed_links:
            return None

        return channel

    async def _sync_live_reactors_for_message(
        self,
        guild: discord.Guild,
        message: discord.Message,
        settings: sqlite3.Row,
    ) -> int:
        configured_emoji = str(settings["emoji"])
        self._clear_starrers(guild.id, message.id)

        for reaction in message.reactions:
            if not self._emoji_matches(reaction.emoji, configured_emoji):
                continue
            try:
                async for user in reaction.users():
                    if user.bot:
                        continue
                    if user.id == message.author.id and not bool(
                        settings["allow_self_star"]
                    ):
                        continue
                    self._add_starrer(guild.id, message.id, user.id)
            except Exception:
                logging.warning(
                    f"Starboard: failed to enumerate reaction users for message {message.id} in guild {guild.id}"
                )

        return self._count_starrers(guild.id, message.id)

    async def _delete_starboard_message_if_exists(
        self,
        guild: discord.Guild,
        post_row: sqlite3.Row,
    ) -> None:
        if not post_row["starboard_channel_id"] or not post_row["starboard_message_id"]:
            return

        channel = await self._fetch_channel_safe(
            guild, int(post_row["starboard_channel_id"])
        )
        if channel is None or not isinstance(channel, discord.TextChannel):
            return

        try:
            msg = await channel.fetch_message(int(post_row["starboard_message_id"]))
            await msg.delete()
        except Exception:
            pass

    async def _publish_or_update_starboard_post(
        self,
        guild: discord.Guild,
        message: discord.Message,
        settings: sqlite3.Row,
        star_count: int,
    ) -> None:
        starboard_channel = await self._ensure_starboard_channel(guild, settings)
        if starboard_channel is None:
            raise RuntimeError(
                "Starboard channel is missing or bot lacks required permissions."
            )

        threshold = int(settings["threshold"])
        configured_emoji = str(settings["emoji"])
        existing = self._fetch_post(guild.id, message.id)

        if star_count < threshold:
            if existing and existing["starboard_message_id"]:
                await self._delete_starboard_message_if_exists(guild, existing)
            self._upsert_post(
                guild_id=guild.id,
                source_channel_id=message.channel.id,
                source_message_id=message.id,
                source_author_id=message.author.id if message.author else None,
                starboard_channel_id=starboard_channel.id,
                starboard_message_id=None,
                star_count=star_count,
                last_known_content=message.content,
            )
            return

        embed = self._build_starboard_embed(message, star_count, configured_emoji)
        content = self._build_starboard_content(message, star_count, configured_emoji)

        existing_message = None
        if existing and existing["starboard_message_id"]:
            try:
                existing_message = await starboard_channel.fetch_message(
                    int(existing["starboard_message_id"])
                )
            except Exception:
                existing_message = None

        if existing_message is None:
            posted = await starboard_channel.send(content=content, embed=embed)
            self._upsert_post(
                guild_id=guild.id,
                source_channel_id=message.channel.id,
                source_message_id=message.id,
                source_author_id=message.author.id if message.author else None,
                starboard_channel_id=starboard_channel.id,
                starboard_message_id=posted.id,
                star_count=star_count,
                last_known_content=message.content,
            )
            audit_log(
                f"Starboard posted message {message.id} in guild {guild.id} with {star_count} stars."
            )
            return

        await existing_message.edit(content=content, embed=embed)
        self._upsert_post(
            guild_id=guild.id,
            source_channel_id=message.channel.id,
            source_message_id=message.id,
            source_author_id=message.author.id if message.author else None,
            starboard_channel_id=starboard_channel.id,
            starboard_message_id=existing_message.id,
            star_count=star_count,
            last_known_content=message.content,
        )

    async def _reconcile_message(
        self,
        guild: discord.Guild,
        message: discord.Message,
        settings: sqlite3.Row,
    ) -> None:
        valid, _reason = await self._validate_source_message(guild, message, settings)
        if not valid:
            post_row = self._fetch_post(guild.id, message.id)
            if post_row:
                await self._delete_starboard_message_if_exists(guild, post_row)
                self._upsert_post(
                    guild_id=guild.id,
                    source_channel_id=message.channel.id,
                    source_message_id=message.id,
                    source_author_id=message.author.id if message.author else None,
                    starboard_channel_id=post_row["starboard_channel_id"],
                    starboard_message_id=None,
                    star_count=0,
                    last_known_content=message.content,
                )
            return

        star_count = await self._sync_live_reactors_for_message(
            guild, message, settings
        )
        await self._publish_or_update_starboard_post(
            guild, message, settings, star_count
        )

    # --------------------------------------------------------
    # Raw reaction events
    # --------------------------------------------------------
    @commands.Cog.listener()
    async def on_raw_reaction_add(
        self, payload: discord.RawReactionActionEvent
    ) -> None:
        if payload.guild_id is None or payload.user_id == self.bot.user.id:
            return

        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            try:
                guild = await self.bot.fetch_guild(payload.guild_id)
            except Exception:
                return

        settings = self._get_settings(guild.id)
        if not bool(settings["is_enabled"]):
            return

        if not self._emoji_matches(payload.emoji, str(settings["emoji"])):
            return

        guild, message = await self._find_message_from_payload(payload)
        if guild is None or message is None:
            return

        valid, _reason = await self._validate_source_message(
            guild, message, settings, acting_user_id=payload.user_id
        )
        if not valid:
            return

        if not self._has_starrer(guild.id, message.id, payload.user_id):
            self._add_starrer(guild.id, message.id, payload.user_id)

        star_count = self._count_starrers(guild.id, message.id)
        try:
            await self._publish_or_update_starboard_post(
                guild, message, settings, star_count
            )
        except Exception as e:
            logging.warning(
                f"Starboard: failed to update post on reaction add for message {message.id} in guild {guild.id}: {e}"
            )

    @commands.Cog.listener()
    async def on_raw_reaction_remove(
        self, payload: discord.RawReactionActionEvent
    ) -> None:
        if payload.guild_id is None or payload.user_id == self.bot.user.id:
            return

        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            try:
                guild = await self.bot.fetch_guild(payload.guild_id)
            except Exception:
                return

        settings = self._get_settings(guild.id)
        if not bool(settings["is_enabled"]):
            return

        if not self._emoji_matches(payload.emoji, str(settings["emoji"])):
            return

        self._remove_starrer(guild.id, payload.message_id, payload.user_id)

        guild, message = await self._find_message_from_payload(payload)
        post_row = self._fetch_post(payload.guild_id, payload.message_id)

        if message is None:
            if post_row:
                star_count = self._count_starrers(payload.guild_id, payload.message_id)
                if star_count < int(settings["threshold"]):
                    try:
                        if guild is not None:
                            await self._delete_starboard_message_if_exists(
                                guild, post_row
                            )
                    except Exception:
                        pass
                    if guild is not None:
                        self._upsert_post(
                            guild_id=guild.id,
                            source_channel_id=post_row["source_channel_id"],
                            source_message_id=payload.message_id,
                            source_author_id=post_row["source_author_id"],
                            starboard_channel_id=post_row["starboard_channel_id"],
                            starboard_message_id=None,
                            star_count=star_count,
                            last_known_content=post_row["last_known_content"],
                        )
            return

        star_count = self._count_starrers(guild.id, message.id)
        try:
            await self._publish_or_update_starboard_post(
                guild, message, settings, star_count
            )
        except Exception as e:
            logging.warning(
                f"Starboard: failed to update post on reaction remove for message {message.id} in guild {guild.id}: {e}"
            )

    @commands.Cog.listener()
    async def on_raw_reaction_clear(
        self, payload: discord.RawReactionClearEvent
    ) -> None:
        if payload.guild_id is None:
            return

        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            try:
                guild = await self.bot.fetch_guild(payload.guild_id)
            except Exception:
                return

        settings = self._get_settings(guild.id)
        if not bool(settings["is_enabled"]):
            return

        self._clear_starrers(guild.id, payload.message_id)

        post_row = self._fetch_post(guild.id, payload.message_id)
        if post_row:
            try:
                await self._delete_starboard_message_if_exists(guild, post_row)
            except Exception:
                pass
            self._upsert_post(
                guild_id=guild.id,
                source_channel_id=post_row["source_channel_id"],
                source_message_id=payload.message_id,
                source_author_id=post_row["source_author_id"],
                starboard_channel_id=post_row["starboard_channel_id"],
                starboard_message_id=None,
                star_count=0,
                last_known_content=post_row["last_known_content"],
            )

    @commands.Cog.listener()
    async def on_raw_reaction_clear_emoji(
        self, payload: discord.RawReactionClearEmojiEvent
    ) -> None:
        if payload.guild_id is None:
            return

        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            try:
                guild = await self.bot.fetch_guild(payload.guild_id)
            except Exception:
                return

        settings = self._get_settings(guild.id)
        if not bool(settings["is_enabled"]):
            return

        if not self._emoji_matches(payload.emoji, str(settings["emoji"])):
            return

        self._clear_starrers(guild.id, payload.message_id)

        guild, message = await self._find_message_by_ids(
            payload.guild_id, payload.channel_id, payload.message_id
        )
        post_row = self._fetch_post(payload.guild_id, payload.message_id)

        if message is not None:
            try:
                await self._publish_or_update_starboard_post(
                    guild, message, settings, 0
                )
            except Exception:
                pass
            return

        if post_row:
            try:
                if guild is not None:
                    await self._delete_starboard_message_if_exists(guild, post_row)
            except Exception:
                pass
            if guild is not None:
                self._upsert_post(
                    guild_id=guild.id,
                    source_channel_id=post_row["source_channel_id"],
                    source_message_id=payload.message_id,
                    source_author_id=post_row["source_author_id"],
                    starboard_channel_id=post_row["starboard_channel_id"],
                    starboard_message_id=None,
                    star_count=0,
                    last_known_content=post_row["last_known_content"],
                )

    # --------------------------------------------------------
    # Cleanup listeners
    # --------------------------------------------------------
    @commands.Cog.listener()
    async def on_raw_message_delete(
        self, payload: discord.RawMessageDeleteEvent
    ) -> None:
        if payload.guild_id is None:
            return

        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            try:
                guild = await self.bot.fetch_guild(payload.guild_id)
            except Exception:
                guild = None

        post_row = self._fetch_post(payload.guild_id, payload.message_id)
        if post_row:
            if post_row["starboard_message_id"] and payload.message_id == int(
                post_row["starboard_message_id"]
            ):
                self._upsert_post(
                    guild_id=payload.guild_id,
                    source_channel_id=post_row["source_channel_id"],
                    source_message_id=post_row["source_message_id"],
                    source_author_id=post_row["source_author_id"],
                    starboard_channel_id=post_row["starboard_channel_id"],
                    starboard_message_id=None,
                    star_count=post_row["star_count"],
                    last_known_content=post_row["last_known_content"],
                )
                audit_log(
                    f"Starboard message deleted for source message {post_row['source_message_id']} in guild {payload.guild_id}."
                )
                return

            if payload.message_id == int(post_row["source_message_id"]):
                if guild is not None:
                    try:
                        await self._delete_starboard_message_if_exists(guild, post_row)
                    except Exception:
                        pass
                self._delete_post_record(payload.guild_id, payload.message_id)
                audit_log(
                    f"Source message {payload.message_id} deleted. Starboard record removed in guild {payload.guild_id}."
                )

    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel) -> None:
        guild = channel.guild
        settings = self._get_settings(guild.id)

        if settings["channel_id"] and int(settings["channel_id"]) == channel.id:
            self._update_settings(guild.id, channel_id=0, is_enabled=False)
            audit_log(
                f"Starboard disabled in guild {guild.id} because configured channel {channel.id} was deleted."
            )

    # --------------------------------------------------------
    # Slash commands
    # --------------------------------------------------------
    @app_commands.command(
        name="starboard_setup",
        description="Set up or update the starboard for this server.",
    )
    @app_commands.describe(
        channel="The channel to use as the starboard.",
        threshold="How many stars are required before a message is posted.",
        emoji="The emoji to track. Default: ⭐",
        allow_self_star="Allow users to star their own messages.",
        allow_bot_messages="Allow bot-authored messages onto the starboard.",
        ignore_nsfw="Prevent NSFW messages being sent to a non-NSFW starboard.",
    )
    async def starboard_setup(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel,
        threshold: Optional[int] = None,
        emoji: Optional[str] = None,
        allow_self_star: Optional[bool] = None,
        allow_bot_messages: Optional[bool] = None,
        ignore_nsfw: Optional[bool] = None,
    ) -> None:
        actor = interaction.user
        guild = interaction.guild

        if not isinstance(actor, discord.Member) or guild is None:
            await interaction.response.send_message(
                embed=self._embed(
                    "Server only",
                    "This command must be used in a server.",
                    self.error_colour,
                ),
                ephemeral=True,
            )
            return

        if not self._is_manager(actor):
            await interaction.response.send_message(
                embed=self._embed(
                    "No permission",
                    "You do not have permission to configure the starboard.",
                    self.error_colour,
                ),
                ephemeral=True,
            )
            return

        if threshold is None:
            threshold = self.defaults["threshold"]
        if threshold < 1:
            await interaction.response.send_message(
                embed=self._embed(
                    "Invalid threshold",
                    "Threshold must be at least 1.",
                    self.error_colour,
                ),
                ephemeral=True,
            )
            return

        if emoji is None or not emoji.strip():
            emoji = self.defaults["emoji"]
        emoji = emoji_to_storage_value(emoji)

        if allow_self_star is None:
            allow_self_star = self.defaults["allow_self_star"]
        if allow_bot_messages is None:
            allow_bot_messages = self.defaults["allow_bot_messages"]
        if ignore_nsfw is None:
            ignore_nsfw = self.defaults["ignore_nsfw"]

        me = guild.me
        if me is None:
            try:
                me = await guild.fetch_member(self.bot.user.id)
            except Exception:
                me = None

        if me is None:
            await interaction.response.send_message(
                embed=self._embed(
                    "Bot unavailable",
                    "I could not verify my permissions in that channel.",
                    self.error_colour,
                ),
                ephemeral=True,
            )
            return

        perms = channel.permissions_for(me)
        if not perms.view_channel or not perms.send_messages or not perms.embed_links:
            await interaction.response.send_message(
                embed=self._embed(
                    "Missing permissions",
                    "I need View Channel, Send Messages, and Embed Links in that channel.",
                    self.error_colour,
                ),
                ephemeral=True,
            )
            return

        self._update_settings(
            guild.id,
            channel_id=channel.id,
            threshold=threshold,
            emoji=emoji,
            allow_self_star=allow_self_star,
            allow_bot_messages=allow_bot_messages,
            ignore_nsfw=ignore_nsfw,
            is_enabled=True,
        )

        embed = discord.Embed(
            title="Starboard configured",
            color=self.success_colour,
        )
        embed.add_field(name="Channel", value=channel.mention, inline=False)
        embed.add_field(name="Threshold", value=str(threshold), inline=True)
        embed.add_field(name="Emoji", value=emoji, inline=True)
        embed.add_field(
            name="Allow Self-Star",
            value="Yes" if allow_self_star else "No",
            inline=True,
        )
        embed.add_field(
            name="Allow Bot Messages",
            value="Yes" if allow_bot_messages else "No",
            inline=True,
        )
        embed.add_field(
            name="Ignore NSFW Mismatch",
            value="Yes" if ignore_nsfw else "No",
            inline=True,
        )

        await interaction.response.send_message(embed=embed, ephemeral=True)
        audit_log(
            f"{actor} configured starboard in guild {guild.id}: channel={channel.id}, threshold={threshold}, emoji={emoji}."
        )

    @app_commands.command(
        name="starboard_disable",
        description="Disable the starboard in this server.",
    )
    async def starboard_disable(self, interaction: discord.Interaction) -> None:
        actor = interaction.user
        guild = interaction.guild

        if not isinstance(actor, discord.Member) or guild is None:
            await interaction.response.send_message(
                embed=self._embed(
                    "Server only",
                    "This command must be used in a server.",
                    self.error_colour,
                ),
                ephemeral=True,
            )
            return

        if not self._is_manager(actor):
            await interaction.response.send_message(
                embed=self._embed(
                    "No permission",
                    "You do not have permission to disable the starboard.",
                    self.error_colour,
                ),
                ephemeral=True,
            )
            return

        self._update_settings(guild.id, is_enabled=False)

        await interaction.response.send_message(
            embed=self._embed(
                "Starboard disabled",
                "The starboard has been disabled. Existing posts are left untouched.",
                self.info_colour,
            ),
            ephemeral=True,
        )
        audit_log(f"{actor} disabled starboard in guild {guild.id}.")

    @app_commands.command(
        name="starboard_info",
        description="Show the current starboard configuration.",
    )
    async def starboard_info(self, interaction: discord.Interaction) -> None:
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                embed=self._embed(
                    "Server only",
                    "This command must be used in a server.",
                    self.error_colour,
                ),
                ephemeral=True,
            )
            return

        settings = self._get_settings(guild.id)
        channel = None
        if settings["channel_id"]:
            channel = guild.get_channel(int(settings["channel_id"]))

        cursor.execute(
            "SELECT COUNT(*) FROM starboard_posts WHERE guild_id = ? AND starboard_message_id IS NOT NULL",
            (guild.id,),
        )
        live_posts = int(cursor.fetchone()[0])

        embed = discord.Embed(
            title="Starboard Info",
            color=self.info_colour,
        )
        embed.add_field(
            name="Enabled",
            value="Yes" if bool(settings["is_enabled"]) else "No",
            inline=True,
        )
        embed.add_field(
            name="Channel",
            value=channel.mention if channel else "Not set / unavailable",
            inline=True,
        )
        embed.add_field(name="Threshold", value=str(settings["threshold"]), inline=True)
        embed.add_field(name="Emoji", value=str(settings["emoji"]), inline=True)
        embed.add_field(
            name="Allow Self-Star",
            value="Yes" if bool(settings["allow_self_star"]) else "No",
            inline=True,
        )
        embed.add_field(
            name="Allow Bot Messages",
            value="Yes" if bool(settings["allow_bot_messages"]) else "No",
            inline=True,
        )
        embed.add_field(
            name="Ignore NSFW Mismatch",
            value="Yes" if bool(settings["ignore_nsfw"]) else "No",
            inline=True,
        )
        embed.add_field(name="Live Starboard Posts", value=str(live_posts), inline=True)

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(
        name="starboard_refresh",
        description="Refresh one source message on the starboard.",
    )
    @app_commands.describe(message_id="The source message ID to refresh.")
    async def starboard_refresh(
        self, interaction: discord.Interaction, message_id: str
    ) -> None:
        actor = interaction.user
        guild = interaction.guild

        if not isinstance(actor, discord.Member) or guild is None:
            await interaction.response.send_message(
                embed=self._embed(
                    "Server only",
                    "This command must be used in a server.",
                    self.error_colour,
                ),
                ephemeral=True,
            )
            return

        if not self._is_manager(actor):
            await interaction.response.send_message(
                embed=self._embed(
                    "No permission",
                    "You do not have permission to refresh starboard posts.",
                    self.error_colour,
                ),
                ephemeral=True,
            )
            return

        try:
            source_message_id = int(message_id)
        except Exception:
            await interaction.response.send_message(
                embed=self._embed(
                    "Invalid message ID",
                    "Please provide a valid numeric source message ID.",
                    self.error_colour,
                ),
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        settings = self._get_settings(guild.id)
        if not bool(settings["is_enabled"]):
            await interaction.followup.send(
                embed=self._embed(
                    "Starboard disabled",
                    "Enable the starboard first.",
                    self.error_colour,
                ),
                ephemeral=True,
            )
            return

        post_row = self._fetch_post(guild.id, source_message_id)
        if post_row is None:
            await interaction.followup.send(
                embed=self._embed(
                    "Not tracked",
                    "That source message is not currently tracked in the starboard database.",
                    self.error_colour,
                ),
                ephemeral=True,
            )
            return

        message = await self._fetch_message_safe(
            guild,
            int(post_row["source_channel_id"]),
            int(post_row["source_message_id"]),
        )
        if message is None:
            try:
                await self._delete_starboard_message_if_exists(guild, post_row)
            except Exception:
                pass
            self._delete_post_record(guild.id, source_message_id)
            await interaction.followup.send(
                embed=self._embed(
                    "Source message missing",
                    "The source message could not be fetched, so its starboard record was removed.",
                    self.info_colour,
                ),
                ephemeral=True,
            )
            return

        try:
            await self._reconcile_message(guild, message, settings)
            await interaction.followup.send(
                embed=self._embed(
                    "Refreshed",
                    "That source message has been re-synchronised.",
                    self.success_colour,
                ),
                ephemeral=True,
            )
        except Exception as e:
            await interaction.followup.send(
                embed=self._embed(
                    "Refresh failed",
                    f"Failed to refresh that message: {e}",
                    self.error_colour,
                ),
                ephemeral=True,
            )

    @app_commands.command(
        name="starboard_rebuild",
        description="Rebuild the starboard from recent tracked messages.",
    )
    @app_commands.describe(
        limit="How many tracked source messages to check. Default 100, max 1000."
    )
    async def starboard_rebuild(
        self, interaction: discord.Interaction, limit: Optional[int] = 100
    ) -> None:
        actor = interaction.user
        guild = interaction.guild

        if not isinstance(actor, discord.Member) or guild is None:
            await interaction.response.send_message(
                embed=self._embed(
                    "Server only",
                    "This command must be used in a server.",
                    self.error_colour,
                ),
                ephemeral=True,
            )
            return

        if not self._is_manager(actor):
            await interaction.response.send_message(
                embed=self._embed(
                    "No permission",
                    "You do not have permission to rebuild the starboard.",
                    self.error_colour,
                ),
                ephemeral=True,
            )
            return

        limit = max(1, min(int(limit or 100), 1000))

        await interaction.response.defer(ephemeral=True, thinking=True)

        settings = self._get_settings(guild.id)
        if not bool(settings["is_enabled"]):
            await interaction.followup.send(
                embed=self._embed(
                    "Starboard disabled",
                    "Enable the starboard first.",
                    self.error_colour,
                ),
                ephemeral=True,
            )
            return

        cursor.execute(
            """
            SELECT * FROM starboard_posts
            WHERE guild_id = ?
            ORDER BY last_updated_at DESC, source_message_id DESC
            LIMIT ?
            """,
            (guild.id, limit),
        )
        rows = cursor.fetchall()

        checked = 0
        updated = 0
        removed = 0
        failed = 0

        for row in rows:
            checked += 1
            message = await self._fetch_message_safe(
                guild,
                int(row["source_channel_id"]),
                int(row["source_message_id"]),
            )
            if message is None:
                try:
                    await self._delete_starboard_message_if_exists(guild, row)
                except Exception:
                    pass
                self._delete_post_record(guild.id, int(row["source_message_id"]))
                removed += 1
                continue

            try:
                await self._reconcile_message(guild, message, settings)
                updated += 1
            except Exception:
                failed += 1

        embed = discord.Embed(
            title="Starboard rebuild complete",
            color=self.info_colour,
        )
        embed.add_field(name="Checked", value=str(checked), inline=True)
        embed.add_field(name="Updated", value=str(updated), inline=True)
        embed.add_field(name="Removed", value=str(removed), inline=True)
        embed.add_field(name="Failed", value=str(failed), inline=True)

        await interaction.followup.send(embed=embed, ephemeral=True)
        audit_log(
            f"{actor} rebuilt starboard in guild {guild.id}. checked={checked}, updated={updated}, removed={removed}, failed={failed}"
        )

    @app_commands.command(
        name="starboard_leaderboard",
        description="Show the most starred source messages.",
    )
    @app_commands.describe(limit="How many entries to show. Default 10, max 25.")
    async def starboard_leaderboard(
        self, interaction: discord.Interaction, limit: Optional[int] = 10
    ) -> None:
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                embed=self._embed(
                    "Server only",
                    "This command must be used in a server.",
                    self.error_colour,
                ),
                ephemeral=True,
            )
            return

        limit = max(1, min(int(limit or 10), 25))

        cursor.execute(
            """
            SELECT * FROM starboard_posts
            WHERE guild_id = ? AND star_count > 0
            ORDER BY star_count DESC, source_message_id DESC
            LIMIT ?
            """,
            (guild.id, limit),
        )
        rows = cursor.fetchall()

        if not rows:
            await interaction.response.send_message(
                embed=self._embed(
                    "No starboard data",
                    "There are no starred messages recorded yet.",
                    self.info_colour,
                ),
                ephemeral=True,
            )
            return

        lines = []
        for idx, row in enumerate(rows, start=1):
            source_channel_id = int(row["source_channel_id"])
            source_message_id = int(row["source_message_id"])
            lines.append(
                f"**{idx}.** ⭐ **{row['star_count']}** - <#{source_channel_id}> - source message `{source_message_id}`"
            )

        embed = discord.Embed(
            title="Starboard Leaderboard",
            description="\n".join(lines),
            color=self.info_colour,
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    # --------------------------------------------------------
    # Ready
    # --------------------------------------------------------
    @commands.Cog.listener()
    async def on_ready(self) -> None:
        try:
            cursor.execute(
                """
                SELECT guild_id, channel_id, is_enabled
                FROM starboard_settings
                WHERE is_enabled = 1
                """
            )
            settings_rows = cursor.fetchall()

            active = 0
            for row in settings_rows:
                guild = self.bot.get_guild(int(row["guild_id"]))
                if guild is None:
                    continue

                if not row["channel_id"]:
                    self._update_settings(guild.id, is_enabled=False)
                    continue

                channel = guild.get_channel(int(row["channel_id"]))
                if channel is None:
                    self._update_settings(guild.id, is_enabled=False)
                    continue

                active += 1

            logging.info(
                "\033[96mStarboard\033[0m cog synced. Active starboards: %d.",
                active,
            )
            audit_log(f"Starboard cog ready. Active starboards: {active}.")
        except Exception as e:
            logging.error(f"Error initialising starboard cog on_ready: {e}")
            audit_log(f"Error initialising starboard cog: {e}")


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Starboard(bot))
