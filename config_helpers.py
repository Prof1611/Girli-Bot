from __future__ import annotations

import logging
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional

import discord
import yaml


CONFIG_PATH = Path("config.yaml")


def _parse_colour(value: Optional[Any], fallback: discord.Color) -> discord.Color:
    if isinstance(value, discord.Color):
        return value
    if isinstance(value, int):
        try:
            return discord.Color(value)
        except ValueError:
            return fallback
    if isinstance(value, str):
        raw = value.strip()
        if raw.startswith("#"):
            raw = raw[1:]
        try:
            return discord.Color(int(raw, 16))
        except ValueError:
            return fallback
    return fallback


@lru_cache(maxsize=1)
def _load_config_cached() -> Dict[str, Any]:
    try:
        with CONFIG_PATH.open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            if not isinstance(data, dict):
                logging.warning("config.yaml did not contain a mapping. Using empty config.")
                return {}
            return data
    except FileNotFoundError:
        logging.warning("config.yaml not found while loading configuration.")
        return {}
    except Exception as exc:  # pragma: no cover - defensive logging only
        logging.error("Failed to load config.yaml: %s", exc)
        return {}


def load_config() -> Dict[str, Any]:
    """Return a deepcopy of the cached configuration mapping."""
    return deepcopy(_load_config_cached())


def get_embed_colours() -> Dict[str, discord.Color]:
    """Fetch success/info/error embed colours from the configuration."""
    config = _load_config_cached()
    appearance = config.get("appearance", {}) if isinstance(config, dict) else {}
    colours_cfg = appearance.get("colours", {}) if isinstance(appearance, dict) else {}
    return {
        "success": _parse_colour(colours_cfg.get("success"), discord.Color.green()),
        "info": _parse_colour(colours_cfg.get("info"), discord.Color.blurple()),
        "error": _parse_colour(colours_cfg.get("error"), discord.Color.red()),
    }


def colour_from_value(value: Optional[Any], fallback: discord.Color) -> discord.Color:
    """Expose colour parsing so cogs can translate custom hex values."""
    return _parse_colour(value, fallback)
