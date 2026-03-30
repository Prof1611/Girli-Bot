import discord
import logging
import yaml
from discord.ext import commands
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests
import unicodedata
import string
import re
from bs4 import BeautifulSoup


def audit_log(message: str):
    """Append a timestamped message to the audit log file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("audit.log", "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")


def normalize_string(s: str) -> str:
    """
    Normalize a string by removing diacritics, punctuation, extra whitespace,
    and converting to lowercase.
    """
    s = s or ""
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("utf-8")
    s = s.translate(str.maketrans("", "", string.punctuation))
    return " ".join(s.split()).lower()


def clean_display(s: str) -> str:
    """Clean text for display without changing intended casing."""
    if not s:
        return ""
    return " ".join(s.split()).strip()


class Scrape(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

        # Load the config file with UTF-8 encoding.
        with open("config.yaml", "r", encoding="utf-8") as config_file:
            self.config = yaml.safe_load(config_file)

        # HTTP session with headers and safer defaults.
        self.http = requests.Session()
        self.http.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (compatible; DiscordBot/1.0; +https://discord.com)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
        )

        audit_log("Scrape cog initialised and configuration loaded successfully.")

    @commands.Cog.listener()
    async def on_ready(self):
        logging.info("\033[96mScrape\033[0m cog synced successfully.")
        audit_log("Scrape cog synced successfully.")

    @discord.app_commands.command(
        name="scrape",
        description="Checks Holly Humberstone's website for new shows and updates #concert-chats and server events.",
    )
    async def scrape(self, interaction: discord.Interaction):
        await interaction.response.defer()

        audit_log(
            f"{interaction.user.name} (ID: {interaction.user.id}) invoked /scrape command in guild '{interaction.guild.name}' (ID: {interaction.guild.id})."
        )

        try:
            audit_log("Starting scraping process via /scrape command.")

            # Run the scraper asynchronously in a separate thread.
            new_entries = await asyncio.to_thread(self.run_scraper)

            audit_log(
                f"{interaction.user.name} (ID: {interaction.user.id}) retrieved {len(new_entries)} entries from the website (after dedupe/filter)."
            )

            # Create forum threads and get count.
            threads_created = await self.check_forum_threads(
                interaction.guild, interaction, new_entries
            )

            # Create scheduled events and get count.
            events_created = await self.check_server_events(
                interaction.guild, interaction, new_entries
            )

            # Send a combined summary.
            await self.send_combined_summary(
                interaction, threads_created, events_created
            )

            logging.info(
                f"Full scrape and creation process done: {threads_created} threads, {events_created} events created."
            )
            audit_log("Scrape process completed successfully.")

        except Exception as e:
            logging.error(f"An error occurred in the scrape command: {e}")
            audit_log(
                f"{interaction.user.name} (ID: {interaction.user.id}) encountered an error in /scrape command: {e}"
            )
            error_embed = discord.Embed(
                title="Error",
                description=f"An error occurred during scraping:\n`{e}`",
                color=discord.Color.red(),
            )
            await interaction.followup.send(embed=error_embed)

    def _extract_date_text(self, date_el) -> str:
        """
        Extract date text from <p class="date"> which contains <sup>th</sup> etc.

        Example HTML:
          <p class="date">9<sup>th</sup> Feb 2026</p>

        Output:
          "9th Feb 2026"
        """
        if not date_el:
            return ""

        # Keep suffixes, but avoid spacing like "9 th"
        # get_text(" ", ...) tends to output "9 th Feb 2026"
        txt = date_el.get_text(" ", strip=True)
        txt = re.sub(
            r"\s+(st|nd|rd|th)\b", r"\1", txt, flags=re.IGNORECASE
        )  # "9 th" -> "9th"
        txt = re.sub(r"\s*-\s*", "-", txt)  # normalise range hyphens
        return clean_display(txt)

    def _month_to_number(self, month_str: str) -> int | None:
        """
        Accepts "Feb" or "February", etc.
        Returns month number 1-12 or None.
        """
        if not month_str:
            return None
        m = month_str.strip()
        for fmt in ("%b", "%B"):
            try:
                return datetime.strptime(m, fmt).month
            except Exception:
                continue
        return None

    def _parse_tour_date_to_formatted(self, date_text: str, fallback_year: int) -> str:
        """
        Convert a site date string like:
          - "9th Feb 2026" -> "09 February 2026"
          - "5th-7th Jun" -> "05 June YYYY - 07 June YYYY" (uses fallback_year)
          - "5th-7th Jun 2026" -> "05 June 2026 - 07 June 2026"
        If parsing fails, returns the original cleaned date_text.
        """
        if not date_text:
            return ""

        s = clean_display(date_text)

        # Range: 5th-7th Jun [2026]
        m_range = re.match(
            r"^\s*(\d{1,2})(?:st|nd|rd|th)?\s*-\s*(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)\s*(\d{4})?\s*$",
            s,
            flags=re.IGNORECASE,
        )
        if m_range:
            d1 = int(m_range.group(1))
            d2 = int(m_range.group(2))
            mon = self._month_to_number(m_range.group(3))
            yr = int(m_range.group(4)) if m_range.group(4) else int(fallback_year)
            if mon:
                start_dt = datetime(yr, mon, d1)
                end_dt = datetime(yr, mon, d2)
                start_str = start_dt.strftime("%d %B %Y")
                end_str = end_dt.strftime("%d %B %Y")
                return f"{start_str} - {end_str}"
            return s

        # Single: 9th Feb 2026
        m_single = re.match(
            r"^\s*(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)\s+(\d{4})\s*$",
            s,
            flags=re.IGNORECASE,
        )
        if m_single:
            day = int(m_single.group(1))
            mon = self._month_to_number(m_single.group(2))
            yr = int(m_single.group(3))
            if mon:
                dt = datetime(yr, mon, day)
                return dt.strftime("%d %B %Y")
            return s

        # Single without year: 9th Feb
        m_single_no_year = re.match(
            r"^\s*(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)\s*$",
            s,
            flags=re.IGNORECASE,
        )
        if m_single_no_year:
            day = int(m_single_no_year.group(1))
            mon = self._month_to_number(m_single_no_year.group(2))
            yr = int(fallback_year)
            if mon:
                dt = datetime(yr, mon, day)
                return dt.strftime("%d %B %Y")
            return s

        return s

    def run_scraper(self):
        logging.info("Running scraper using Holly Humberstone tour page HTML...")
        audit_log(
            "Starting scraper: Requesting event data from Holly Humberstone tour page HTML."
        )

        entries_raw: list[tuple[str, str, str]] = []

        try:
            url = "https://www.hollyhumberstone.com/tour/"
            response = self.http.get(url, timeout=20)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # New format: <div class="tour-dates"><div class="tour-date">...</div></div>
            cards = soup.select(".tour-dates .tour-date")
            logging.info(f"Retrieved {len(cards)} tour-date blocks from HTML.")
            audit_log(
                f"Scraped tour page HTML: Retrieved {len(cards)} tour-date blocks."
            )

            # Used for entries without a year, if they appear (eg festival ranges)
            current_year = datetime.now(ZoneInfo("Europe/London")).year
            last_seen_year = current_year

            for card in cards:
                date_el = card.select_one("p.date")
                venue_el = card.select_one("p.venue")
                city_el = card.select_one("p.city")

                raw_date = self._extract_date_text(date_el)
                venue = clean_display(venue_el.get_text(strip=True) if venue_el else "")
                location = clean_display(
                    city_el.get_text(strip=True) if city_el else ""
                )

                # Skip empty blocks
                if not raw_date and not venue and not location:
                    continue

                # Update last seen year when present in the raw date text
                year_match = re.search(r"\b(\d{4})\b", raw_date or "")
                if year_match:
                    try:
                        last_seen_year = int(year_match.group(1))
                    except Exception:
                        last_seen_year = last_seen_year

                formatted_date = self._parse_tour_date_to_formatted(
                    raw_date, last_seen_year
                )

                entries_raw.append((formatted_date, venue, location))

            audit_log(
                f"Finished processing tour page events. Total parsed entries: {len(entries_raw)}."
            )

        except Exception as e:
            logging.error(f"An error occurred during website scraping: {e}")
            audit_log(f"Error during website scraping: {e}")

        # Dedupe entries (date + venue + location) using normalised keys.
        seen: set[tuple[str, str, str]] = set()
        new_entries: list[tuple[str, str, str]] = []
        for event_date, venue, location in entries_raw:
            key = (
                normalize_string(event_date),
                normalize_string(venue),
                normalize_string(location),
            )
            if key in seen:
                continue
            seen.add(key)
            new_entries.append((event_date, venue, location))

        logging.info(f"Returning {len(new_entries)} unique entries after dedupe.")
        audit_log(f"Returning {len(new_entries)} unique entries after dedupe/filter.")

        return new_entries

    def format_api_date(self, iso_date_str):
        """
        Convert an ISO date string (YYYY-MM-DD) into the format "DD Month YYYY".
        For example, "2025-04-15" becomes "15 April 2025".
        """
        try:
            dt = datetime.strptime(iso_date_str, "%Y-%m-%d")
            formatted_date = dt.strftime("%d %B %Y")
            audit_log(f"Formatted API date '{iso_date_str}' to '{formatted_date}'.")
            return formatted_date
        except Exception as e:
            logging.error(f"Error formatting API date '{iso_date_str}': {e}")
            audit_log(f"Error formatting API date '{iso_date_str}': {e}")
            return iso_date_str

    def format_date(self, date_str):
        # Original method for page-based dates remains unchanged.
        if "-" in date_str:
            start_date_str, end_date_str = map(str.strip, date_str.split("-"))
            start_date = datetime.strptime(start_date_str, "%b %d, %Y").strftime(
                "%d %B %Y"
            )
            end_date = datetime.strptime(end_date_str, "%b %d, %Y").strftime("%d %B %Y")
            return f"{start_date} - {end_date}"
        else:
            return datetime.strptime(date_str, "%b %d, %Y").strftime("%d %B %Y")

    def parse_event_dates(self, formatted_date: str):
        """
        Parse the formatted date string (e.g. "01 January 2025" or "01 January 2025 - 02 January 2025")
        into start and end timezone-aware datetime objects.

        - If it's a single date, set the event from 7:00 PM to 11:00 PM.
        - If it's a range, set the start time to 8:00 AM on the first day and the end time to 11:00 PM on the last day.
        """
        try:
            tz = ZoneInfo("Europe/London")
            if "-" in formatted_date:
                start_date_str, end_date_str = map(str.strip, formatted_date.split("-"))
                dt_start = datetime.strptime(start_date_str, "%d %B %Y")
                dt_end = datetime.strptime(end_date_str, "%d %B %Y")
                start_dt = datetime(
                    dt_start.year, dt_start.month, dt_start.day, 8, 0, 0, tzinfo=tz
                )
                end_dt = datetime(
                    dt_end.year, dt_end.month, dt_end.day, 23, 0, 0, tzinfo=tz
                )
            else:
                dt = datetime.strptime(formatted_date, "%d %B %Y")
                start_dt = datetime(dt.year, dt.month, dt.day, 19, 0, 0, tzinfo=tz)
                end_dt = datetime(dt.year, dt.month, dt.day, 23, 0, 0, tzinfo=tz)

            logging.debug(
                f"Parsed event dates from '{formatted_date}' -> start: {start_dt}, end: {end_dt}"
            )
            audit_log(f"Successfully parsed event dates for '{formatted_date}'.")
            return start_dt, end_dt

        except Exception as e:
            logging.error(f"Error parsing event dates from '{formatted_date}': {e}")
            audit_log(f"Error parsing event dates from '{formatted_date}': {e}")
            now = datetime.now(ZoneInfo("Europe/London"))
            return now, now + timedelta(hours=4)

    async def check_forum_threads(self, guild, interaction, new_entries):
        audit_log("Starting check for forum threads for new entries.")

        gigchats_id = self.config.get("channels", {}).get("liveshows_forum_id")
        if gigchats_id is None:
            logging.error("Missing 'channels.liveshows_forum_id' in config.yaml.")
            error_embed = discord.Embed(
                title="Error",
                description=(
                    "Threads channel ID is not configured. "
                    "Please set `channels.liveshows_forum_id` in config.yaml."
                ),
                color=discord.Color.red(),
            )
            await interaction.followup.send(embed=error_embed)
            audit_log(
                f"{interaction.user.name} (ID: {interaction.user.id}): "
                "Failed to update threads because 'channels.liveshows_forum_id' "
                "is missing from config.yaml."
            )
            return 0

        gigchats_channel = guild.get_channel(gigchats_id)
        if gigchats_channel is None:
            logging.error(f"Channel with ID {gigchats_id} not found.")
            error_embed = discord.Embed(
                title="Error",
                description="Threads channel was not found. Please double-check the config.",
                color=discord.Color.red(),
            )
            await interaction.followup.send(embed=error_embed)
            audit_log(
                f"{interaction.user.name} (ID: {interaction.user.id}): Failed to update threads because channel with ID {gigchats_id} was not found in guild '{guild.name}' (ID: {guild.id})."
            )
            return 0

        # Build a set of existing thread keys including archived threads.
        existing_thread_keys = await self.get_existing_forum_thread_keys(
            gigchats_channel
        )

        new_threads_created = 0

        for event_date, venue, location in new_entries:
            event_date = clean_display(event_date)
            venue = clean_display(venue)
            location = clean_display(location)

            if venue:
                thread_title = f"{event_date} - {venue}".strip()
            else:
                thread_title = f"{event_date}".strip()

            key = (normalize_string(thread_title), normalize_string(location))

            logging.debug(f"Checking thread: title='{thread_title}', key={key}")

            if key in existing_thread_keys:
                audit_log(
                    f"Skipping thread creation for '{thread_title}' as it already exists (active or archived)."
                )
                continue

            title_only_key = (normalize_string(thread_title), "")
            if title_only_key in existing_thread_keys:
                audit_log(
                    f"Skipping thread creation for '{thread_title}' as a title-only match already exists (active or archived)."
                )
                continue

            try:
                content_parts = ["Holly Humberstone"]
                if venue:
                    content_parts.append(f"at {venue}")
                if location:
                    content_parts.append(location)
                content = " ".join(content_parts).replace("  ", " ").strip()

                logging.info(f"Creating thread for: {thread_title}")

                await gigchats_channel.create_thread(
                    name=thread_title,
                    content=content,
                    auto_archive_duration=60,
                )

                new_threads_created += 1
                existing_thread_keys.add(key)

                logging.info(f"Successfully created thread: {thread_title}")
                audit_log(
                    f"{interaction.user.name} (ID: {interaction.user.id}) created thread '{thread_title}' in channel #{gigchats_channel.name} (ID: {gigchats_channel.id}) in guild '{guild.name}' (ID: {guild.id})."
                )
                await asyncio.sleep(2)

            except discord.Forbidden:
                logging.error(
                    f"Permission denied when trying to create thread '{thread_title}'"
                )
                error_embed = discord.Embed(
                    title="Error",
                    description=f"Permission denied when trying to create thread '{thread_title}'.",
                    color=discord.Color.red(),
                )
                await interaction.followup.send(embed=error_embed)
                audit_log(
                    f"{interaction.user.name} (ID: {interaction.user.id}) encountered permission error creating thread '{thread_title}' in channel #{gigchats_channel.name} (ID: {gigchats_channel.id})."
                )

            except discord.HTTPException as e:
                logging.error(f"Failed to create thread '{thread_title}': {e}")
                error_embed = discord.Embed(
                    title="Error",
                    description=f"Failed to create thread '{thread_title}': `{e}`",
                    color=discord.Color.red(),
                )
                await interaction.followup.send(embed=error_embed)
                audit_log(
                    f"{interaction.user.name} (ID: {interaction.user.id}) failed to create thread '{thread_title}' in channel #{gigchats_channel.name} (ID: {gigchats_channel.id}) due to HTTP error: {e}"
                )

        audit_log(
            f"Forum threads check complete. New threads created: {new_threads_created}."
        )
        return new_threads_created

    async def get_existing_forum_thread_keys(
        self, forum_channel
    ) -> set[tuple[str, str]]:
        """
        Return a set of keys (normalised_thread_title, normalised_location) for:
        - active threads
        - archived threads
        Location is derived from the starter post content when available.
        """
        keys: set[tuple[str, str]] = set()

        try:
            active_threads = list(forum_channel.threads)
        except Exception as e:
            logging.error(f"Error accessing active threads: {e}")
            active_threads = []

        archived_threads = []
        try:
            async for t in forum_channel.archived_threads(limit=100):
                archived_threads.append(t)
        except Exception as e:
            logging.warning(f"Could not fetch archived threads (public): {e}")

        try:
            async for t in forum_channel.archived_threads(private=True, limit=100):
                archived_threads.append(t)
        except Exception as e:
            logging.debug(f"Could not fetch archived threads (private): {e}")

        all_threads = active_threads + archived_threads

        for thread in all_threads:
            title_norm = normalize_string(thread.name)

            loc_norm = ""
            try:
                starter_message = await thread.fetch_message(thread.id)
                content_norm = normalize_string(starter_message.content or "")
                loc_norm = content_norm
            except Exception:
                loc_norm = ""

            keys.add((title_norm, ""))
            keys.add((title_norm, loc_norm))

        return keys

    async def check_server_events(self, guild, interaction, new_entries):
        audit_log("Starting check for scheduled events for new entries.")
        new_events_created = 0

        try:
            with open("event-image.jpg", "rb") as img_file:
                event_image = img_file.read()
        except Exception as e:
            logging.error(f"Failed to load event image: {e}")
            audit_log(f"Failed to load event image: {e}")
            event_image = None

        scheduled_events = await guild.fetch_scheduled_events()
        logging.debug(
            f"Guild '{guild.name}' has {len(scheduled_events)} scheduled events."
        )

        existing_event_names = {normalize_string(e.name): e for e in scheduled_events}

        for event_date, venue, location in new_entries:
            event_date = clean_display(event_date)
            venue = clean_display(venue)
            location = clean_display(location)

            if venue:
                event_name = f"{event_date} - {venue}".strip()
            else:
                event_name = f"{event_date}".strip()

            norm_event_name = normalize_string(event_name)
            logging.debug(f"Normalised scheduled event name: '{norm_event_name}'")

            exists = norm_event_name in existing_event_names

            logging.info(
                f"Does scheduled event '{event_name}' exist in guild '{guild.name}'? {exists}"
            )

            if exists:
                audit_log(
                    f"Skipping creation of scheduled event '{event_name}' as it already exists."
                )
                continue

            start_time, end_time = self.parse_event_dates(event_date)

            try:
                description_parts = ["Holly Humberstone"]
                if venue:
                    description_parts.append(f"at {venue}")
                if location:
                    description_parts.append(location)
                description = " ".join(description_parts).replace("  ", " ").strip()

                loc_bits = [b for b in [venue, location] if b]
                loc_display = ", ".join(loc_bits)

                await guild.create_scheduled_event(
                    name=event_name,
                    description=description,
                    start_time=start_time,
                    end_time=end_time,
                    location=loc_display if loc_display else "TBA",
                    entity_type=discord.EntityType.external,
                    image=event_image,
                    privacy_level=discord.PrivacyLevel.guild_only,
                )

                new_events_created += 1
                existing_event_names[norm_event_name] = True

                logging.info(f"Successfully created scheduled event: {event_name}")
                audit_log(
                    f"{interaction.user.name} (ID: {interaction.user.id}) created scheduled event '{event_name}' in guild '{guild.name}' (ID: {guild.id})."
                )
                await asyncio.sleep(2)

            except discord.Forbidden:
                logging.error(
                    f"Permission denied when trying to create scheduled event '{event_name}'"
                )
                error_embed = discord.Embed(
                    title="Error",
                    description=f"Permission denied when trying to create scheduled event '{event_name}'.",
                    color=discord.Color.red(),
                )
                await interaction.followup.send(embed=error_embed)
                audit_log(
                    f"{interaction.user.name} (ID: {interaction.user.id}) encountered permission error creating scheduled event '{event_name}' in guild '{guild.name}' (ID: {guild.id})."
                )

            except discord.HTTPException as e:
                logging.error(f"Failed to create scheduled event '{event_name}': {e}")
                error_embed = discord.Embed(
                    title="Error",
                    description=f"Failed to create scheduled event '{event_name}': `{e}`",
                    color=discord.Color.red(),
                )
                await interaction.followup.send(embed=error_embed)
                audit_log(
                    f"{interaction.user.name} (ID: {interaction.user.id}) failed to create scheduled event '{event_name}' in guild '{guild.name}' (ID: {guild.id}) due to HTTP error: {e}"
                )

        audit_log(
            f"Scheduled events check complete. New events created: {new_events_created}."
        )
        return new_events_created

    async def send_combined_summary(
        self, interaction, threads_created: int, events_created: int
    ):
        if threads_created == 0 and events_created == 0:
            description = "All up to date! No new threads or scheduled events created."
        else:
            description = (
                f"**Forum Threads:** {threads_created} new thread{'s' if threads_created != 1 else ''} created.\n"
                f"**Scheduled Events:** {events_created} new scheduled event{'s' if events_created != 1 else ''} created."
            )

        embed = discord.Embed(
            title="Scrape Completed",
            description=description,
            color=(
                discord.Color.green()
                if (threads_created or events_created)
                else discord.Color.blurple()
            ),
        )

        logging.debug(f"Sending summary embed with description: {description}")
        await interaction.followup.send(embed=embed)
        audit_log("Combined summary sent to user with details: " + description)

    async def setup_audit(self, interaction):
        audit_log(
            f"{interaction.user.name} (ID: {interaction.user.id}) initiated a scrape command in guild '{interaction.guild.name}' (ID: {interaction.guild.id})."
        )


async def setup(bot):
    await bot.add_cog(Scrape(bot))
