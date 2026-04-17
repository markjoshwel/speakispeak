"""
speakispeak: Discord bot entrypoint
  with all my heart, 2026, mark joshwel <mark@joshwel.co>
  SPDX-License-Identifier: Unlicense OR 0BSD
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import multiprocessing
import os
import re
import time
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, NamedTuple

import discord

from elias.opus import ensure_opus_loaded
from elias.vendor_bootstrap import bootstrap_voice_recv_vendor
bootstrap_voice_recv_vendor()

from elias.dashboard import DashboardServer
from elias.session import SpeakiSession
from elias.sounds import describe_sound
from elias.state import (
    DASHBOARD_PORT,
    DEFAULT_VC_TIMEOUT_SECONDS,
    DEFAULT_WAIT_UNTIL_VOICE_FINISHED_SECONDS,
    JANITOR_INTERVAL_SECONDS,
    STOP_COMMAND_TEXT,
    STOP_VOTE_EXPIRE_SECONDS,
    STOP_VOTE_THRESHOLD,
    TRIGGER_TEXT,
    WHISPER_MODEL_NAME,
    WORKER_POOL_SIZE,
)

log = logging.getLogger(__name__)
CONFIG_COMMAND = "speaki config"
PUMPKIN_REACTION = "\N{JACK-O-LANTERN}"
PLEADING_REACTION = "\U0001F97A"
QUESTION_REACTION = "\N{BLACK QUESTION MARK ORNAMENT}"
SENSITIVE_CONFIG_KEYS = frozenset({"admin_user_id", "app_token"})
CONFIG_KEY_ORDER = (
    "app_token",
    "admin_user_id",
    "debug",
    "dump-worker-audio",
    "vc-worker",
    "vc-worker-finish-wait",
    "wait_until_voice_finished",
    "vc-timeout",
    "vc-worker-use-grammar",
    "vc-worker-strict-final-only",
    "vc-worker-strict-double-hit",
    "vc-worker-pool-size",
    "vc-worker-use-whisper",
    "vc-worker-whisper-model",
    "vc-worker-load-en",
    "vc-worker-load-ko",
    "vc-worker-load-kr",
    "vc-worker-load-ja",
    "vc-worker-load-jp",
    "dashboard-port",
)
MUTABLE_CONFIG_KEYS = frozenset(set(CONFIG_KEY_ORDER) - SENSITIVE_CONFIG_KEYS)

# ── Stop-vote voicelines ──────────────────────────────────────────────────────
# Written in speaki's canon style: third-person, whiny, defensive, tearful.
# Speaki says "hueng" when distressed and "joayo" when (reluctantly) accepting.

def _stop_vote_registered(voter: str, remaining: int) -> str:
    if remaining == 1:
        count_phrase = "one more person asks~!"
    else:
        count_phrase = f"{remaining} more people ask~!"
    return (
        f"hueng... {voter} wants speaki to leave!! "
        f"but speaki didn't do anything wrong!! speaki will go if {count_phrase} 🥺"
    )


def _stop_vote_already_voted(voter: str, remaining: int) -> str:
    if remaining == 1:
        count_phrase = "one more person"
    else:
        count_phrase = f"{remaining} more people"
    return (
        f"heueeng!! speaki already heard {voter}!! "
        f"{count_phrase} still needs to agree before speaki goes... 😭"
    )


def _stop_vote_passed() -> str:
    return (
        "jo... joayo... speaki will leave then... "
        "but speaki really didn't do anything wrong!! 😭"
    )


def _stop_admin_forced() -> str:
    return "jo... joayo... speaki is leaving... (master has spoken) 🥺"


# ── Data types ────────────────────────────────────────────────────────────────

class Config(NamedTuple):
    token: str
    admin_user_id: int | None
    worker_enabled: bool
    enabled_languages: tuple[str, ...]
    use_grammar: bool
    strict_final_only: bool
    strict_double_hit: bool
    debug: bool
    dump_worker_audio: bool
    worker_pool_size: int
    use_whisper: bool
    whisper_model: str
    worker_finish_wait_seconds: float
    vc_timeout_seconds: float
    dashboard_port: int


@dataclass
class StopVote:
    channel_id: int
    voters: set[int] = field(default_factory=set)
    started_at: float = field(default_factory=time.monotonic)

    def is_expired(self) -> bool:
        return time.monotonic() - self.started_at > STOP_VOTE_EXPIRE_SECONDS


# ── Runtime config ────────────────────────────────────────────────────────────

class RuntimeConfig:
    def __init__(self, config_path: Path):
        self.config_path = config_path
        self._cached_data: dict[str, Any] | None = None
        self._cached_config: Config | None = None
        self._cached_mtime_ns: int | None = None
        self._last_load_warning: str | None = None

    def get(self) -> Config:
        data = self._load_data()
        if self._cached_config is not None and data is self._cached_data:
            return self._cached_config

        config = _build_config(data)
        self._cached_config = config
        return config

    def is_admin(self, user_id: int) -> bool:
        admin_user_id = self.get().admin_user_id
        return admin_user_id is not None and admin_user_id == user_id

    def render_public_config(self) -> str:
        data = self._load_data()
        lines: list[str] = []
        for key in _ordered_config_keys(data):
            if key in SENSITIVE_CONFIG_KEYS:
                continue
            lines.append(f"{key} = {_format_toml_value(data[key])}")

        return "\n".join(lines)

    def apply_update(self, toml_text: str) -> tuple[Config, dict[str, Any]]:
        updates = _load_config_update_data(toml_text)
        if not isinstance(updates, dict) or not updates:
            raise RuntimeError("speaki: error: config update must contain at least one key")

        unknown_keys = sorted(key for key in updates if key not in MUTABLE_CONFIG_KEYS)
        if unknown_keys:
            raise RuntimeError(
                f"speaki: error: unrecognized or immutable config keys: {', '.join(unknown_keys)}"
            )

        _validate_config_updates(updates)
        current_data = self._load_data()
        merged_data = dict(current_data)
        merged_data.update(updates)
        config = _build_config(merged_data)
        self._write_data(merged_data)
        return config, updates

    def _load_data(self) -> dict[str, Any]:
        stat = self.config_path.stat()
        if self._cached_data is not None and self._cached_mtime_ns == stat.st_mtime_ns:
            return self._cached_data

        try:
            data = _load_config_data(self.config_path)
            config = _build_config(data)
        except Exception as exc:
            if self._cached_data is not None and self._cached_config is not None:
                warning = f"{type(exc).__name__}: {exc}"
                if self._last_load_warning != warning:
                    log.warning(
                        "speaki: warning: failed reloading config.toml, using last known good config (%s)",
                        warning,
                    )
                    self._last_load_warning = warning
                return self._cached_data
            raise

        self._cached_data = data
        self._cached_config = config
        self._cached_mtime_ns = stat.st_mtime_ns
        self._last_load_warning = None
        return data

    def _write_data(self, data: dict[str, Any]) -> None:
        lines = [f"{key} = {_format_toml_value(data[key])}" for key in _ordered_config_keys(data)]
        tmp_path = self.config_path.parent.joinpath(f".{self.config_path.name}.tmp")
        tmp_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        tmp_path.replace(self.config_path)


# ── Discord client ────────────────────────────────────────────────────────────

class SpeakiClient(discord.Client):
    def __init__(self, runtime_config: RuntimeConfig, dashboard: DashboardServer | None):
        intents = discord.Intents.none()
        intents.guilds = True
        intents.messages = True
        intents.message_content = True
        intents.voice_states = True

        super().__init__(intents=intents)
        self.runtime_config = runtime_config
        self.dashboard = dashboard
        self.sessions: dict[int, SpeakiSession] = {}
        self._stop_votes: dict[int, StopVote] = {}
        self._janitor_task: asyncio.Task[None] | None = None

    async def setup_hook(self) -> None:
        self._janitor_task = asyncio.create_task(self._run_session_janitor())

    async def close(self) -> None:
        if self._janitor_task is not None:
            self._janitor_task.cancel()
            try:
                await self._janitor_task
            except asyncio.CancelledError:
                pass
            self._janitor_task = None

        sessions = list(self.sessions.values())
        self.sessions.clear()
        for session in sessions:
            await session.close(reason="client shutdown")

        if self.dashboard is not None:
            await self.dashboard.stop()

        await super().close()

    async def on_ready(self) -> None:
        if self.user is None:
            return

        log.info("speaki: info: logged in as %s (%s)", self.user, self.user.id)

    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot or message.guild is None:
            return

        content = message.content.strip()
        if self._is_config_command(content):
            await self._handle_config_command(message, content)
            return

        if content.casefold() == STOP_COMMAND_TEXT:
            await self._handle_stop_command(message)
            return

        if content.casefold() != TRIGGER_TEXT:
            return

        member = message.author if isinstance(message.author, discord.Member) else None
        user_label = str(message.author)
        if member is None or member.voice is None or member.voice.channel is None:
            log.info("speaki: info: master %s typed 'speaki', they are not in a vc", user_label)
            return

        log.info(
            "speaki: info: master %s typed 'speaki', they are in a vc %s#%s, joining it",
            user_label,
            member.voice.channel.name,
            member.voice.channel.id,
        )

        session = self._get_or_create_session(message.guild)

        try:
            await session.activate_for_channel(member.voice.channel, requested_by=user_label)
            session.touch()
            session._emit_session_state()
            sound_path = await session.play_random_sound(force=True)
            if sound_path is not None:
                await message.reply(describe_sound(sound_path), mention_author=False)
        except TimeoutError:
            log.warning(
                "speaki: warning: timed out connecting to voice channel %s in guild %s",
                member.voice.channel.id,
                message.guild.id,
            )
        except Exception:
            log.exception(
                "speaki: error: failed to activate session for guild %s",
                message.guild.id,
            )

    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ) -> None:
        if self.user is not None and member.id == self.user.id:
            session = self.sessions.get(member.guild.id)
            if session is not None:
                await session.handle_self_voice_state_update(before, after)
            return

        if member.bot:
            return

        session = self.sessions.get(member.guild.id)
        if session is None or session.current_channel_id is None:
            return

        before_channel = before.channel
        after_channel = after.channel
        session_channel_id = session.current_channel_id

        member_joined_our_channel = (
            after_channel is not None
            and after_channel.id == session_channel_id
            and (before_channel is None or before_channel.id != session_channel_id)
        )
        member_left_our_channel = (
            before_channel is not None
            and before_channel.id == session_channel_id
            and (after_channel is None or after_channel.id != session_channel_id)
        )

        if member_joined_our_channel:
            log.info(
                "speaki: info: %s joined vc %s#%s",
                member,
                after_channel.name,
                after_channel.id,
            )
            # Invalidate their stop-vote if they had one
            vote = self._stop_votes.get(member.guild.id)
            if vote is not None:
                vote.voters.discard(member.id)
            session.on_member_count_changed(joined=True)
            if self.dashboard is not None:
                session._emit({
                    "type": "member_join",
                    "user_id": str(member.id),
                    "user_label": str(member),
                    "avatar_url": str(member.display_avatar.url) if hasattr(member, "display_avatar") else None,
                })

        elif member_left_our_channel:
            log.info(
                "speaki: info: %s left vc %s#%s",
                member,
                before_channel.name,
                before_channel.id,
            )
            if self.dashboard is not None:
                session._emit({"type": "member_leave", "user_id": str(member.id)})

            remaining_humans = [
                m for m in before_channel.members
                if not m.bot and m.id != member.id
            ]
            if not remaining_humans:
                log.info(
                    "speaki: info: no humans left in vc %s#%s, leaving and shutting down worker",
                    before_channel.name,
                    before_channel.id,
                )
                self._stop_votes.pop(member.guild.id, None)
                closing_session = self.sessions.pop(member.guild.id, None)
                if closing_session is not None:
                    await closing_session.close(reason="last human left voice")
            else:
                session.on_member_count_changed(joined=False)

    # ── Stop-vote handler ─────────────────────────────────────────────────

    async def _handle_stop_command(self, message: discord.Message) -> None:
        assert message.guild is not None

        session = self.sessions.get(message.guild.id)
        if session is None or session.current_channel_id is None:
            return  # speaki isn't even here

        member = message.author if isinstance(message.author, discord.Member) else None
        if member is None:
            return

        # Admins bypass the vote and force an immediate exit.
        if self.runtime_config.is_admin(member.id):
            log.info(
                "speaki: info: admin %s force-stopped speaki in guild %s",
                member,
                message.guild.id,
            )
            await message.reply(_stop_admin_forced(), mention_author=False)
            self._stop_votes.pop(message.guild.id, None)
            closing = self.sessions.pop(message.guild.id, None)
            if closing is not None:
                await closing.close(reason="admin force-stop")
            return

        # Must be in the bot's voice channel to vote.
        if member.voice is None or member.voice.channel is None:
            return
        if member.voice.channel.id != session.current_channel_id:
            return

        channel = message.guild.get_channel(session.current_channel_id)
        if channel is None:
            return

        humans = [m for m in getattr(channel, "members", []) if not m.bot]
        human_count = len(humans)
        votes_needed = max(1, math.ceil(human_count * STOP_VOTE_THRESHOLD))

        # Retrieve or create vote, resetting if expired.
        vote = self._stop_votes.get(message.guild.id)
        if vote is None or vote.is_expired() or vote.channel_id != session.current_channel_id:
            vote = StopVote(channel_id=session.current_channel_id)
            self._stop_votes[message.guild.id] = vote

        already_voted = member.id in vote.voters
        vote.voters.add(member.id)

        current_votes = len(vote.voters)
        remaining = votes_needed - current_votes

        # Emit vote update to dashboard.
        session._emit({
            "type": "vote_update",
            "voter_label": str(member),
            "votes": current_votes,
            "needed": votes_needed,
        })

        if current_votes >= votes_needed:
            log.info(
                "speaki: info: stop vote passed in guild %s (%s/%s votes)",
                message.guild.id,
                current_votes,
                votes_needed,
            )
            await message.reply(_stop_vote_passed(), mention_author=False)
            self._stop_votes.pop(message.guild.id, None)
            closing = self.sessions.pop(message.guild.id, None)
            if closing is not None:
                await closing.close(reason="stop vote passed")
        elif already_voted:
            await message.reply(
                _stop_vote_already_voted(str(member), remaining),
                mention_author=False,
            )
        else:
            await message.reply(
                _stop_vote_registered(str(member), remaining),
                mention_author=False,
            )

    # ── Janitor ───────────────────────────────────────────────────────────

    async def _run_session_janitor(self) -> None:
        while True:
            await asyncio.sleep(JANITOR_INTERVAL_SECONDS)

            to_close: list[tuple[int, str]] = []
            for guild_id, session in self.sessions.items():
                if session.close_requested_reason is not None:
                    to_close.append((guild_id, session.close_requested_reason))
                elif session.is_idle():
                    to_close.append((guild_id, "leaving voice due to inactivity"))

            for guild_id, reason in to_close:
                self._stop_votes.pop(guild_id, None)
                session = self.sessions.pop(guild_id, None)
                if session is not None:
                    await session.close(reason=reason)

            # Expire stale votes.
            stale = [gid for gid, v in self._stop_votes.items() if v.is_expired()]
            for gid in stale:
                self._stop_votes.pop(gid, None)

    # ── Helpers ───────────────────────────────────────────────────────────

    def _get_or_create_session(self, guild: discord.Guild) -> SpeakiSession:
        session = self.sessions.get(guild.id)
        if session is not None:
            return session

        emit = None
        if self.dashboard is not None:
            emit = self.dashboard.make_emitter(asyncio.get_running_loop())

        session = SpeakiSession(
            self,
            guild,
            self.runtime_config.get,
            dashboard_emit=emit,
        )
        self.sessions[guild.id] = session
        return session

    def _is_config_command(self, content: str) -> bool:
        lowered = content.casefold()
        return lowered == CONFIG_COMMAND or (
            lowered.startswith(CONFIG_COMMAND)
            and len(content) > len(CONFIG_COMMAND)
            and content[len(CONFIG_COMMAND)] in {" ", "\t", "\n"}
        )

    async def _handle_config_command(self, message: discord.Message, content: str) -> None:
        if not self.runtime_config.is_admin(message.author.id):
            return

        config_text = _strip_code_fence(content[len(CONFIG_COMMAND):].lstrip())
        if not config_text:
            await message.reply(
                f"```toml\n{self.runtime_config.render_public_config()}\n```",
                mention_author=False,
            )
            return

        try:
            config, _updates = self.runtime_config.apply_update(config_text)
            configure_logging(debug=config.debug)
            await self._refresh_sessions_for_live_config()
        except RuntimeError as exc:
            log.exception(
                "speaki: error: failed to apply live config update from user %s",
                message.author.id,
            )
            reaction = (
                QUESTION_REACTION
                if "unrecognized or immutable config keys" in str(exc)
                else PLEADING_REACTION
            )
            await self._safe_add_reaction(message, reaction)
            return
        except tomllib.TOMLDecodeError:
            log.exception(
                "speaki: error: failed to parse live config update from user %s",
                message.author.id,
            )
            await self._safe_add_reaction(message, PLEADING_REACTION)
            return
        except Exception:
            log.exception(
                "speaki: error: failed to apply live config update from user %s",
                message.author.id,
            )
            await self._safe_add_reaction(message, PLEADING_REACTION)
            return

        log.info(
            "speaki: info: applied live config update from admin user %s",
            message.author.id,
        )
        await self._safe_add_reaction(message, PUMPKIN_REACTION)

    async def _refresh_sessions_for_live_config(self) -> None:
        for guild_id, session in self.sessions.items():
            result = await session.refresh_runtime_config()
            if result.action == "closed":
                continue

            guild = self.get_guild(guild_id)
            guild_label = guild.name if guild is not None else str(guild_id)
            channel_suffix = f" in <#{result.channel_id}>" if result.channel_id is not None else ""
            action_label = {
                "disabled": "workers disabled",
                "enabled": "workers enabled",
                "restarted": "workers redeployed",
                "updated": "runtime updated",
                "unchanged": "already matched new config",
                "disconnected": "session not connected",
            }.get(result.action, result.action)
            log.info(
                "speaki: info: live config refresh for guild %s (%s%s): %s",
                guild_id,
                guild_label,
                channel_suffix,
                action_label,
            )

    async def _safe_add_reaction(self, message: discord.Message, reaction: str) -> None:
        try:
            await message.add_reaction(reaction)
        except Exception:
            log.warning(
                "speaki: warning: failed to add reaction %s to message %s",
                reaction,
                message.id,
            )


# ── Config helpers ────────────────────────────────────────────────────────────

def _read_bool(data: dict[str, object], *keys: str, default: bool) -> bool:
    for key in keys:
        value = data.get(key)
        if isinstance(value, bool):
            return value
    return default


def _load_enabled_languages(data: dict[str, object]) -> tuple[str, ...]:
    enabled: list[str] = []

    if _read_bool(data, "vc-worker-load-en", default=True):
        enabled.append("en")
    if _read_bool(data, "vc-worker-load-ko", "vc-worker-load-kr", default=True):
        enabled.append("ko")
    if _read_bool(data, "vc-worker-load-ja", "vc-worker-load-jp", default=True):
        enabled.append("ja")

    if not enabled:
        raise RuntimeError("speaki: error: at least one vc-worker-load-* language must be enabled")

    return tuple(enabled)


def _read_str(data: dict[str, object], *keys: str, default: str) -> str:
    for key in keys:
        value = data.get(key)
        if isinstance(value, str) and value:
            return value
    return default


def _read_positive_int(data: dict[str, object], *keys: str, default: int) -> int:
    for key in keys:
        value = data.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and value >= 1:
            return value
    return default


def _read_nonnegative_int(data: dict[str, object], *keys: str, default: int) -> int:
    for key in keys:
        value = data.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            return value
    return default


def _read_nonnegative_float(data: dict[str, object], *keys: str, default: float) -> float:
    for key in keys:
        value = data.get(key)
        if isinstance(value, (int, float)):
            result = float(value)
            if result < 0:
                raise RuntimeError(f"speaki: error: {key} must be >= 0")
            return result
    return default


def _read_optional_user_id(data: dict[str, object], key: str) -> int | None:
    value = data.get(key)
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    raise RuntimeError(f"speaki: error: {key} must be an integer Discord user id")


def _load_config_data(config_path: Path) -> dict[str, Any]:
    data = tomllib.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError("speaki: error: config.toml must contain a top-level table")
    return data


_CODE_FENCE_RE = re.compile(r"^```(?:toml)?\s*\n(.*?)\n?```\s*$", re.DOTALL)


def _strip_code_fence(text: str) -> str:
    m = _CODE_FENCE_RE.match(text.strip())
    return m.group(1).strip() if m else text


def _load_config_update_data(toml_text: str) -> dict[str, Any]:
    normalized_text = toml_text.strip()
    if not normalized_text:
        raise RuntimeError("speaki: error: config update must contain at least one key")

    updates = tomllib.loads(normalized_text)
    if not isinstance(updates, dict):
        raise RuntimeError("speaki: error: config update must contain top-level keys")

    return _normalise_config_update_keys(updates)


def _build_config(data: dict[str, object]) -> Config:
    env_token = os.environ.get("SPEAKI_TOKEN")

    token = env_token if env_token else data.get("app_token")
    if not isinstance(token, str) or not token:
        raise RuntimeError("speaki: error: missing app_token in config.toml")

    worker_enabled = _read_bool(data, "vc-worker", default=True)
    enabled_languages = _load_enabled_languages(data) if worker_enabled else ()
    worker_finish_wait = _read_nonnegative_float(
        data,
        "vc-worker-finish-wait",
        "wait_until_voice_finished",
        default=DEFAULT_WAIT_UNTIL_VOICE_FINISHED_SECONDS,
    )
    vc_timeout = _read_nonnegative_float(
        data,
        "vc-timeout",
        default=DEFAULT_VC_TIMEOUT_SECONDS,
    )
    worker_pool_size = _read_positive_int(
        data,
        "vc-worker-pool-size",
        default=WORKER_POOL_SIZE,
    )
    dashboard_port = _read_nonnegative_int(
        data,
        "dashboard-port",
        default=DASHBOARD_PORT,
    )

    return Config(
        token=token,
        admin_user_id=_read_optional_user_id(data, "admin_user_id"),
        worker_enabled=worker_enabled,
        enabled_languages=enabled_languages,
        use_grammar=_read_bool(data, "vc-worker-use-grammar", default=True),
        strict_final_only=_read_bool(data, "vc-worker-strict-final-only", default=True),
        strict_double_hit=_read_bool(data, "vc-worker-strict-double-hit", default=True),
        debug=bool(data.get("debug", False)),
        dump_worker_audio=bool(data.get("dump-worker-audio", False)),
        worker_pool_size=worker_pool_size,
        use_whisper=_read_bool(data, "vc-worker-use-whisper", default=False),
        whisper_model=_read_str(data, "vc-worker-whisper-model", default=WHISPER_MODEL_NAME),
        worker_finish_wait_seconds=worker_finish_wait,
        vc_timeout_seconds=vc_timeout,
        dashboard_port=dashboard_port,
    )


def load_config(config_path: Path) -> Config:
    return _build_config(_load_config_data(config_path))


def _ordered_config_keys(data: dict[str, Any]) -> list[str]:
    ordered = [key for key in CONFIG_KEY_ORDER if key in data]
    extras = sorted(key for key in data if key not in CONFIG_KEY_ORDER)
    return [*ordered, *extras]


def _normalise_config_update_keys(data: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    seen_keys: dict[str, str] = {}
    for key, value in data.items():
        normalized_key = _normalise_config_key(key)
        existing_key = seen_keys.get(normalized_key)
        if existing_key is not None and existing_key != key:
            raise RuntimeError(
                f"speaki: error: duplicate config update for {normalized_key}"
            )
        normalized[normalized_key] = value
        seen_keys[normalized_key] = key
    return normalized


def _normalise_config_key(key: str) -> str:
    if "_" not in key:
        return key

    candidate = key.replace("_", "-")
    if candidate in CONFIG_KEY_ORDER:
        return candidate
    return key


def _format_toml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, str):
        return json.dumps(value)
    raise RuntimeError(f"speaki: error: unsupported config value type: {type(value).__name__}")


def _validate_config_updates(updates: dict[str, Any]) -> None:
    bool_keys = {
        "debug",
        "dump-worker-audio",
        "vc-worker",
        "vc-worker-use-grammar",
        "vc-worker-strict-final-only",
        "vc-worker-strict-double-hit",
        "vc-worker-use-whisper",
        "vc-worker-load-en",
        "vc-worker-load-ko",
        "vc-worker-load-kr",
        "vc-worker-load-ja",
        "vc-worker-load-jp",
    }
    float_keys = {
        "vc-worker-finish-wait",
        "wait_until_voice_finished",
        "vc-timeout",
    }
    str_keys = {
        "vc-worker-whisper-model",
    }
    int_keys = {
        "vc-worker-pool-size",
        "dashboard-port",
    }

    for key, value in updates.items():
        if key in bool_keys:
            if not isinstance(value, bool):
                raise RuntimeError(f"speaki: error: {key} must be true or false")
            continue
        if key in float_keys:
            if not isinstance(value, (int, float)):
                raise RuntimeError(f"speaki: error: {key} must be a non-negative number")
            if float(value) < 0:
                raise RuntimeError(f"speaki: error: {key} must be >= 0")
            continue
        if key in str_keys:
            if not isinstance(value, str) or not value:
                raise RuntimeError(f"speaki: error: {key} must be a non-empty string")
            continue
        if key in int_keys:
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise RuntimeError(f"speaki: error: {key} must be a non-negative integer")
            continue


def configure_logging(*, debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    logging.getLogger("discord.player").setLevel(logging.WARNING)
    logging.getLogger("discord.voice_state").setLevel(logging.WARNING)
    logging.getLogger("discord.client").setLevel(logging.WARNING)
    logging.getLogger("discord.gateway").setLevel(logging.WARNING)
    logging.getLogger("discord.ext.voice_recv.gateway").setLevel(logging.WARNING)
    logging.getLogger("discord.ext.voice_recv.reader").setLevel(logging.WARNING)
    logging.getLogger("discord.ext.voice_recv.router").setLevel(logging.WARNING)
    logging.getLogger("discord.opus").setLevel(logging.WARNING)
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)


def main() -> None:
    config_path = Path("config.toml")
    runtime_config = RuntimeConfig(config_path)
    config = runtime_config.get()
    configure_logging(debug=config.debug)
    log.info(
        "speaki: info: vc-worker=%s; worker languages: %s; grammar=%s; dump-worker-audio=%s",
        config.worker_enabled,
        ", ".join(config.enabled_languages) if config.enabled_languages else "(disabled)",
        config.use_grammar,
        config.dump_worker_audio,
    )
    log.info(
        "speaki: info: vc-worker-finish-wait=%ss; vc-timeout=%ss; strict-final-only=%s; strict-double-hit=%s",
        config.worker_finish_wait_seconds,
        config.vc_timeout_seconds,
        config.strict_final_only,
        config.strict_double_hit,
    )
    log.info("speaki: info: using vendored discord-ext-voice-recv fork")
    if not ensure_opus_loaded():
        log.warning(
            "speaki: warning: failed to load libopus; voice playback and receive decoding will not work. "
            "On Apple Silicon with Homebrew, install opus and/or set SPEAKI_OPUS_LIB=/opt/homebrew/lib/libopus.dylib."
        )

    dashboard: DashboardServer | None = None
    if config.dashboard_port > 0:
        dashboard = DashboardServer(port=config.dashboard_port)

    client = SpeakiClient(runtime_config, dashboard)

    async def runner() -> None:
        if dashboard is not None:
            await dashboard.start()
        await client.start(config.token)

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        log.info("speaki: info: Ctrl+C received, shutting down")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
