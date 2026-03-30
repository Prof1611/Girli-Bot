import re
import asyncio
import discord
import logging
from typing import List, Optional, Sequence
from discord import app_commands
from discord.ext import commands
from datetime import datetime


def audit_log(message: str):
    """Append a timestamped message to the audit log file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open("audit.log", "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] {message}\n")
    except Exception as e:
        logging.error(f"Failed to write to audit.log: {e}")


def make_embed(title: str, description: str, color: discord.Color) -> discord.Embed:
    return discord.Embed(title=title, description=description, color=color)


def collect_attachments(
    attachments: Sequence[Optional[discord.Attachment]],
) -> List[discord.Attachment]:
    return [attachment for attachment in attachments if attachment is not None]


async def attachments_to_files(
    attachments: Sequence[discord.Attachment],
) -> List[discord.File]:
    if not attachments:
        return []
    return list(await asyncio.gather(*(attachment.to_file() for attachment in attachments)))


def first_image_attachment(
    attachments: Sequence[discord.Attachment],
) -> Optional[discord.Attachment]:
    for attachment in attachments:
        if attachment.content_type and attachment.content_type.startswith("image/"):
            return attachment
    return None

# — Dropdown (Select) to choose embed colour first (24 options + custom) —
class ColourSelect(discord.ui.Select):
    def __init__(self, parent_view: "ColourPickView"):
        options = [
            discord.SelectOption(
                label="Default", value="default", description="Black (#000000)"
            ),
            discord.SelectOption(
                label="Custom Hex",
                value="custom_hex",
                description="Enter your own hex code…",
            ),
            discord.SelectOption(
                label="Random", value="random", description="Pick a random colour"
            ),
            discord.SelectOption(
                label="Teal", value="teal", description="Aloha (#1ABC9C)"
            ),
            discord.SelectOption(
                label="Dark Teal", value="dark_teal", description="Blue Green (#11806A)"
            ),
            discord.SelectOption(
                label="Green", value="green", description="UFO Green (#2ECC71)"
            ),
            discord.SelectOption(
                label="Blurple", value="blurple", description="Blue Genie (#5865F2)"
            ),
            discord.SelectOption(
                label="OG Blurple",
                value="og_blurple",
                description="Zeus' Temple (#7289DA)",
            ),
            discord.SelectOption(
                label="Blue", value="blue", description="Dayflower (#3498DB)"
            ),
            discord.SelectOption(
                label="Dark Blue", value="dark_blue", description="Deep Water (#206694)"
            ),
            discord.SelectOption(
                label="Purple", value="purple", description="Deep Lilac (#9B59B6)"
            ),
            discord.SelectOption(
                label="Dark Purple",
                value="dark_purple",
                description="Maximum Purple (#71368A)",
            ),
            discord.SelectOption(
                label="Magenta", value="magenta", description="Mellow Melon (#E91E63)"
            ),
            discord.SelectOption(
                label="Dark Magenta",
                value="dark_magenta",
                description="Plum Perfect (#AD1457)",
            ),
            discord.SelectOption(
                label="Gold", value="gold", description="Tanned Leather (#F1C40F)"
            ),
            discord.SelectOption(
                label="Dark Gold", value="dark_gold", description="Tree Sap (#C27C0E)"
            ),
            discord.SelectOption(
                label="Orange", value="orange", description="Dark Cheddar (#E67E22)"
            ),
            discord.SelectOption(
                label="Dark Orange",
                value="dark_orange",
                description="Pepperoni (#A84300)",
            ),
            discord.SelectOption(
                label="Red", value="red", description="Carmine Pink (#E74C3C)"
            ),
            discord.SelectOption(
                label="Dark Red", value="dark_red", description="Red Birch (#992D22)"
            ),
            discord.SelectOption(
                label="Greyple", value="greyple", description="Irogon Blue (#99AAB5)"
            ),
            discord.SelectOption(
                label="Light Grey",
                value="light_grey",
                description="Harrison Grey (#979C9F)",
            ),
            discord.SelectOption(
                label="Darker Grey",
                value="darker_grey",
                description="Morro Bay (#546E7A)",
            ),
            discord.SelectOption(
                label="Dark Theme",
                value="dark_theme",
                description="Antarctic Deep (transparent)",
            ),
            discord.SelectOption(
                label="Yellow", value="yellow", description="Corn (#FEE75C)"
            ),
        ]
        super().__init__(
            placeholder="Choose an embed colour…",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.parent_view = parent_view

    async def callback(self, interaction: discord.Interaction):
        choice = self.values[0]
        try:
            if choice == "custom_hex":
                await interaction.response.send_modal(
                    HexContentModal(
                        self.parent_view.channel, self.parent_view.attachments
                    )
                )
            else:
                colour_method = getattr(discord.Color, choice)
                self.parent_view.chosen_colour = colour_method()
                await interaction.response.send_modal(
                    ContentModal(
                        self.parent_view.channel,
                        self.parent_view.chosen_colour,
                        self.parent_view.attachments,
                    )
                )
            audit_log(
                f"{interaction.user} chose '{choice}' for #{self.parent_view.channel.name}."
            )
        except Exception as e:
            logging.warning(f"ColourSelect.callback failed on choice '{choice}': {e}")
            audit_log(f"Error processing colour choice '{choice}': {e}")
            # fallback to default colour
            self.parent_view.chosen_colour = discord.Color.default()
            await interaction.response.send_modal(
                ContentModal(
                    self.parent_view.channel,
                    discord.Color.default(),
                    self.parent_view.attachments,
                )
            )


# — View that holds the ColourSelect dropdown —
class ColourPickView(discord.ui.View):
    def __init__(
        self,
        channel: discord.TextChannel,
        attachments: Sequence[discord.Attachment],
    ):
        super().__init__(timeout=60)
        self.channel = channel
        self.attachments = list(attachments)
        self.chosen_colour = discord.Color.default()
        self.add_item(ColourSelect(self))

    async def on_timeout(self):
        logging.info(f"ColourPickView timed out in #{self.channel.name}")
        audit_log(f"Colour pick dropdown timed out in #{self.channel.name}.")
        for child in self.children:
            child.disabled = True


# — Modal for built-in colours: collect title + description —
class ContentModal(discord.ui.Modal, title="Write your embed"):
    embed_title = discord.ui.TextInput(
        label="Embed Title",
        style=discord.TextStyle.short,
        required=True,
        placeholder="Title…",
    )
    embed_message = discord.ui.TextInput(
        label="Description",
        style=discord.TextStyle.long,
        required=True,
        placeholder="Your message…",
    )

    def __init__(
        self,
        channel: discord.TextChannel,
        colour: discord.Color,
        attachments: Sequence[discord.Attachment],
    ):
        super().__init__()
        self.channel = channel
        self.colour = colour
        self.attachments = list(attachments)

    async def on_submit(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title=self.embed_title.value,
            description=self.embed_message.value,
            color=self.colour,
        )
        try:
            files = await attachments_to_files(self.attachments)
            image_attachment = first_image_attachment(self.attachments)
            if image_attachment:
                embed.set_image(url=f"attachment://{image_attachment.filename}")
            await self.channel.send(embed=embed, files=files or None)
            audit_log(
                f"{interaction.user} sent embed '{self.embed_title.value}' in #{self.channel.name} with colour {self.colour}."
            )
            success = make_embed(
                "Embed sent!", "Custom embed sent successfully.", discord.Color.green()
            )
            await interaction.response.send_message(embed=success, ephemeral=True)
        except discord.Forbidden:
            logging.error(f"No permission to send embed in #{self.channel.name}")
            audit_log(f"{interaction.user} lacked permissions in #{self.channel.name}.")
            error = make_embed(
                "Error",
                f"I don't have permission to send embeds in {self.channel.mention}.",
                discord.Color.red(),
            )
            await interaction.response.send_message(embed=error, ephemeral=True)
        except Exception as e:
            logging.error(f"ContentModal.on_submit error: {e}")
            audit_log(f"Error sending embed: {e}")
            error = make_embed(
                "Error",
                "Something went wrong while sending your embed. Please try again later.",
                discord.Color.red(),
            )
            await interaction.response.send_message(embed=error, ephemeral=True)


# — Modal for Custom HEX: collect hex + title + description —
class HexContentModal(discord.ui.Modal, title="Custom HEX Embed"):
    hex_code = discord.ui.TextInput(
        label="HEX Code",
        style=discord.TextStyle.short,
        required=True,
        placeholder="#RRGGBB or RRGGBB",
        max_length=7,
    )
    embed_title = discord.ui.TextInput(
        label="Embed Title",
        style=discord.TextStyle.short,
        required=True,
        placeholder="Title…",
    )
    embed_message = discord.ui.TextInput(
        label="Description",
        style=discord.TextStyle.long,
        required=True,
        placeholder="Your message…",
    )

    def __init__(
        self, channel: discord.TextChannel, attachments: Sequence[discord.Attachment]
    ):
        super().__init__()
        self.channel = channel
        self.attachments = list(attachments)

    async def on_submit(self, interaction: discord.Interaction):
        hex_str = self.hex_code.value.strip().lstrip("#")
        if not re.fullmatch(r"[0-9A-Fa-f]{6}", hex_str):
            audit_log(
                f"{interaction.user} provided invalid hex '{self.hex_code.value}'."
            )
            error = make_embed(
                "Error",
                "Invalid hex code! Must be exactly 6 hex digits.",
                discord.Color.red(),
            )
            return await interaction.response.send_message(embed=error, ephemeral=True)

        colour = discord.Color(int(hex_str, 16))
        embed = discord.Embed(
            title=self.embed_title.value,
            description=self.embed_message.value,
            color=colour,
        )
        try:
            files = await attachments_to_files(self.attachments)
            image_attachment = first_image_attachment(self.attachments)
            if image_attachment:
                embed.set_image(url=f"attachment://{image_attachment.filename}")
            await self.channel.send(embed=embed, files=files or None)
            audit_log(
                f"{interaction.user} sent custom embed '{self.embed_title.value}' in #{self.channel.name}."
            )
            success = make_embed(
                "Embed sent!", "Custom embed sent successfully.", discord.Color.green()
            )
            await interaction.response.send_message(embed=success, ephemeral=True)
        except discord.Forbidden:
            logging.error(f"No permission to send custom embed in #{self.channel.name}")
            audit_log(f"{interaction.user} lacked permissions in #{self.channel.name}.")
            error = make_embed(
                "Error",
                f"I don't have permission to send embeds in {self.channel.mention}.",
                discord.Color.red(),
            )
            await interaction.response.send_message(embed=error, ephemeral=True)
        except Exception as e:
            logging.error(f"HexContentModal.on_submit error: {e}")
            audit_log(f"Error sending custom embed: {e}")
            error = make_embed(
                "Error",
                "Something went wrong while sending your embed. Please try again later.",
                discord.Color.red(),
            )
            await interaction.response.send_message(embed=error, ephemeral=True)


# — Cog to tie it all together —
class CustomEmbed(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        audit_log("CustomEmbed cog initialised.")

    @commands.Cog.listener()
    async def on_ready(self):
        logging.info("\033[96mCustomEmbed\033[0m cog synced successfully.")
        audit_log("CustomEmbed cog synced successfully.")

    @app_commands.command(
        name="sendembed",
        description="Send a custom embed: choose a dropdown colour (or custom hex).",
    )
    @app_commands.describe(
        channel="Channel to post the embed in",
        attachment1="Optional image or attachment to include.",
        attachment2="Optional image or attachment to include.",
        attachment3="Optional image or attachment to include.",
    )
    async def sendembed(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel,
        attachment1: Optional[discord.Attachment] = None,
        attachment2: Optional[discord.Attachment] = None,
        attachment3: Optional[discord.Attachment] = None,
    ):
        try:
            attachments = collect_attachments(
                (attachment1, attachment2, attachment3)
            )
            perms = channel.permissions_for(interaction.guild.me)
            if not (perms.send_messages and perms.embed_links):
                error = make_embed(
                    "Error",
                    f"I need send_messages & embed_links in {channel.mention}.",
                    discord.Color.red(),
                )
                await interaction.response.send_message(embed=error, ephemeral=True)
                audit_log(
                    f"{interaction.user} invoked /sendembed but lacked perms in #{channel.name}."
                )
                return

            view = ColourPickView(channel, attachments)
            # *** Send prompt as an embed ***
            await interaction.response.send_message(
                embed=discord.Embed(description="Choose a colour for your embed:"),
                view=view,
                ephemeral=True,
            )
            audit_log(f"{interaction.user} invoked /sendembed in #{channel.name}.")
        except Exception as e:
            logging.error(f"Error in /sendembed: {e}")
            audit_log(f"Unexpected error in /sendembed: {e}")
            error = make_embed(
                "Error",
                "Something went wrong while starting the embed maker. Please try again later.",
                discord.Color.red(),
            )
            await interaction.response.send_message(embed=error, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(CustomEmbed(bot))
