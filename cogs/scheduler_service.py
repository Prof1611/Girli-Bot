import asyncio
import datetime
import json
import logging
import os
import uuid
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional

import discord
from discord import AllowedMentions

SCHEDULE_FILE = "scheduled_dispatches.json"
LONDON_TZ = ZoneInfo("Europe/London")


def utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def parse_schedule_input(raw_value: str) -> datetime.datetime:
    value = raw_value.strip()
    if not value:
        raise ValueError("Schedule value cannot be empty.")

    normalised = value.replace("Z", "+00:00")
    try:
        parsed = datetime.datetime.fromisoformat(normalised)
    except ValueError:
        parsed = None

    if parsed is None:
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                parsed = datetime.datetime.strptime(value, fmt)
                break
            except ValueError:
                continue

    if parsed is None:
        raise ValueError(
            "Use `YYYY-MM-DD HH:MM` (London time) or ISO 8601 like `YYYY-MM-DDTHH:MM:SS+01:00`."
        )

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=LONDON_TZ).astimezone(datetime.timezone.utc)
    else:
        parsed = parsed.astimezone(datetime.timezone.utc)

    return parsed


class ScheduledDispatcher:
    def __init__(self, bot: discord.Client):
        self.bot = bot
        self.lock = asyncio.Lock()
        self.jobs: List[Dict[str, Any]] = []
        self.task: Optional[asyncio.Task] = None
        self._loaded = False

    async def start(self):
        if not self._loaded:
            await self._load_jobs()
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self._run_loop(), name="scheduled-dispatcher")

    async def _load_jobs(self):
        async with self.lock:
            if not os.path.exists(SCHEDULE_FILE):
                self.jobs = []
                self._loaded = True
                return
            try:
                with open(SCHEDULE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self.jobs = data if isinstance(data, list) else []
            except Exception as e:
                logging.error(f"Failed loading schedule file: {e}")
                self.jobs = []
            self._loaded = True

    async def _save_jobs(self):
        with open(SCHEDULE_FILE, "w", encoding="utf-8") as f:
            json.dump(self.jobs, f, ensure_ascii=False, indent=2)

    async def enqueue(
        self,
        guild_id: int,
        channel_id: int,
        author_id: int,
        execute_at: datetime.datetime,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        if execute_at <= utc_now():
            raise ValueError("Schedule time must be in the future.")

        job = {
            "id": uuid.uuid4().hex[:12],
            "guild_id": guild_id,
            "channel_id": channel_id,
            "author_id": author_id,
            "execute_at": execute_at.isoformat(),
            "created_at": utc_now().isoformat(),
            "attempts": 0,
            "next_attempt_at": execute_at.isoformat(),
            "state": "pending",
            "payload": payload,
        }
        async with self.lock:
            self.jobs.append(job)
            self.jobs.sort(key=lambda j: j["execute_at"])
            await self._save_jobs()
        return job

    async def list_jobs_for_guild(self, guild_id: int) -> List[Dict[str, Any]]:
        async with self.lock:
            jobs = [job.copy() for job in self.jobs if job.get("guild_id") == guild_id]
        jobs.sort(key=lambda j: j["execute_at"])
        return jobs

    async def cancel_job(self, guild_id: int, job_id: str) -> bool:
        async with self.lock:
            before = len(self.jobs)
            self.jobs = [
                job
                for job in self.jobs
                if not (
                    job.get("guild_id") == guild_id
                    and str(job.get("id", "")).lower() == job_id.lower()
                )
            ]
            changed = len(self.jobs) != before
            if changed:
                await self._save_jobs()
            return changed

    async def _claim_due_jobs(self) -> List[Dict[str, Any]]:
        now_iso = utc_now().isoformat()
        async with self.lock:
            due_jobs: List[Dict[str, Any]] = []
            claim_until = (utc_now() + datetime.timedelta(seconds=90)).isoformat()
            for job in self.jobs:
                next_attempt_at = job.get("next_attempt_at", job.get("execute_at", ""))
                state = job.get("state", "pending")
                claim_expired = job.get("claim_until", "") <= now_iso
                if next_attempt_at <= now_iso and (state != "running" or claim_expired):
                    job["state"] = "running"
                    job["claim_until"] = claim_until
                    due_jobs.append(job.copy())
            if due_jobs:
                await self._save_jobs()
            return due_jobs

    async def _run_loop(self):
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                due_jobs = await self._claim_due_jobs()
                for job in due_jobs:
                    await self._dispatch_due_job(job)
            except Exception as e:
                logging.error(f"Scheduler loop error: {e}")
            await asyncio.sleep(2)

    async def _mark_job_success(self, job_id: str):
        async with self.lock:
            self.jobs = [job for job in self.jobs if job.get("id") != job_id]
            await self._save_jobs()

    async def _mark_job_retry(self, job_id: str, error_message: str):
        max_attempts = 20
        async with self.lock:
            for job in self.jobs:
                if job.get("id") != job_id:
                    continue
                attempts = int(job.get("attempts", 0)) + 1
                if attempts > max_attempts:
                    logging.error(
                        f"Scheduled job {job_id} dropped after {max_attempts} retries. Last error: {error_message[:250]}"
                    )
                    self.jobs = [entry for entry in self.jobs if entry.get("id") != job_id]
                    break
                job["attempts"] = attempts
                job["last_error"] = error_message[:500]
                backoff_seconds = min(300, 30 * attempts)
                next_attempt = utc_now() + datetime.timedelta(seconds=backoff_seconds)
                job["next_attempt_at"] = next_attempt.isoformat()
                job["state"] = "pending"
                job.pop("claim_until", None)
                break
            await self._save_jobs()

    async def _dispatch_due_job(self, job: Dict[str, Any]):
        send_result = await self._dispatch(job)
        job_id = job.get("id")
        if send_result:
            await self._mark_job_success(job_id)
        else:
            await self._mark_job_retry(
                job_id, "Dispatch failed (transient or recoverable)."
            )

    async def _dispatch(self, job: Dict[str, Any]) -> bool:
        channel_id = job.get("channel_id")
        channel = self.bot.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(channel_id)
            except Exception as e:
                logging.error(f"Scheduled job {job.get('id')} failed: channel missing ({e})")
                return False

        if not isinstance(channel, discord.TextChannel):
            logging.error(f"Scheduled job {job.get('id')} failed: target channel is not text")
            return False

        payload = job.get("payload", {})
        payload_type = payload.get("type")
        allowed = AllowedMentions(users=True, roles=True)

        try:
            if payload_type == "message":
                await channel.send(
                    payload.get("content", ""),
                    allowed_mentions=allowed,
                )
                return True
            elif payload_type == "embed":
                embed = discord.Embed(
                    title=payload.get("title", "Untitled Embed"),
                    description=payload.get("description", ""),
                    color=discord.Color(payload.get("color", discord.Color.default().value)),
                )
                await channel.send(embed=embed, allowed_mentions=allowed)
                return True
            else:
                logging.error(f"Scheduled job {job.get('id')} failed: unknown payload type {payload_type}")
                return False
        except Exception as e:
            logging.error(f"Scheduled job {job.get('id')} send failed: {e}")
            return False


_dispatcher: Optional[ScheduledDispatcher] = None


def get_dispatcher(bot: discord.Client) -> ScheduledDispatcher:
    global _dispatcher
    if _dispatcher is None:
        _dispatcher = ScheduledDispatcher(bot)
    return _dispatcher


async def setup(bot: discord.Client):
    dispatcher = get_dispatcher(bot)
    await dispatcher.start()
