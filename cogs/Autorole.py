import discord
import logging
import yaml
import datetime
from discord.ext import commands


def audit_log(message: str):
    """Append a timestamped message to the audit log file."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open("audit.log", "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] {message}\n")
    except Exception as e:
        logging.error(f"Failed to write to audit.log: {e}")


class AutoRole(commands.Cog):
    """
    AutoRole
    Assigns ONE configured role to every new member who joins.

    Config keys (config.yaml):
      - autorole_enabled: true/false
      - autorole_role_id: int (required when enabled)
      - autorole_include_bots: true/false (default: false)
    """

    CONFIG_PATH = "config.yaml"

    def __init__(self, bot: commands.Bot):
        self.bot = bot

        # Load config safely
        full_config = self._load_config()
        autorole_cfg = (
            full_config.get("features", {})
            .get("autorole", {})
        )

        self.enabled: bool = bool(autorole_cfg.get("enabled", True))
        self.role_id: int | None = autorole_cfg.get("role_id")
        self.include_bots: bool = bool(autorole_cfg.get("include_bots", False))

        if self.enabled and not self.role_id:
            logging.warning(
                "AutoRole is enabled but no role ID is set in config.yaml."
            )
            audit_log(
                "AutoRole: Enabled but no role configured. No roles will be assigned."
            )

    # ---------------------------
    # Lifecycle
    # ---------------------------

    @commands.Cog.listener()
    async def on_ready(self):
        logging.info("\033[96mAutoRole\033[0m cog synced successfully.")
        audit_log("AutoRole cog synced successfully.")

        if not getattr(self.bot, "intents", None) or not self.bot.intents.members:
            msg = (
                "AutoRole notice: Server Members Intent is disabled. "
                "Member join events may not fire. Enable it in the Developer Portal and "
                "pass intents when creating the bot client."
            )
            logging.warning(msg)
            audit_log(msg)

    # ---------------------------
    # Events
    # ---------------------------

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """Assign the configured role when a member joins."""
        if not self.enabled:
            return

        # Skip bots unless explicitly allowed
        if member.bot and not self.include_bots:
            return

        if not self.role_id:
            return

        role = member.guild.get_role(int(self.role_id))
        if role is None:
            logging.error(
                f"Role with ID {self.role_id} not found in guild '{member.guild.name}' ({member.guild.id})."
            )
            audit_log(
                f"AutoRole error: role {self.role_id} not found in guild '{member.guild.name}' ({member.guild.id})."
            )
            return

        try:
            await member.add_roles(role, reason="AutoRole: assign on join")
            logging.info(
                f"Assigned '{role.name}' to {member} in '{member.guild.name}'."
            )
            audit_log(
                f"Assigned role '{role.name}' ({role.id}) to {member} in guild '{member.guild.name}' ({member.guild.id})."
            )
        except discord.Forbidden:
            logging.error(
                f"Forbidden when assigning '{role.name}' to {member}. Check permissions/role position."
            )
            audit_log(
                f"AutoRole forbidden: could not assign '{role.name}' to {member} in '{member.guild.name}'."
            )
        except discord.HTTPException as e:
            logging.error(f"HTTP error when assigning '{role.name}' to {member}: {e}")
            audit_log(
                f"AutoRole HTTP error: could not assign '{role.name}' to {member} in '{member.guild.name}': {e}"
            )
        except Exception as e:
            logging.error(
                f"Unexpected error when assigning '{role.name}' to {member}: {e}",
                exc_info=True,
            )
            audit_log(
                f"AutoRole unexpected error: could not assign '{role.name}' to {member} in '{member.guild.name}': {e}"
            )

    # ---------------------------
    # Helper
    # ---------------------------

    def _load_config(self) -> dict:
        try:
            with open(self.CONFIG_PATH, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
                if not isinstance(data, dict):
                    logging.warning("config.yaml is not a dict; using defaults.")
                    return {}
                return data
        except FileNotFoundError:
            logging.error("config.yaml not found. Using defaults in memory.")
            audit_log("AutoRole: config.yaml not found. Using defaults in memory.")
            return {}
        except Exception as e:
            logging.error(f"Error loading config.yaml: {e}", exc_info=True)
            audit_log(f"AutoRole: error loading config.yaml: {e}")
            return {}


async def setup(bot: commands.Bot):
    await bot.add_cog(AutoRole(bot))
