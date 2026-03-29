"""
speakispeak: Discord bot entrypoint
  with all my heart, 2026, mark joshwel <mark@joshwel.co>
  SPDX-License-Identifier: Unlicense OR 0BSD
"""

from __future__ import annotations

import asyncio
import json
import logging
import multiprocessing
import os
import tomllib
from pathlib import Path
from typing import Any, NamedTuple

import discord

from elias.opus import ensure_opus_loaded
from elias.vendor_bootstrap import bootstrap_voice_recv_vendor
bootstrap_voice_recv_vendor()

from elias.session import SpeakiSession
from elias.sounds import describe_sound
from elias.state import (
    DEFAULT_VC_TIMEOUT_SECONDS,
    DEFAULT_WAIT_UNTIL_VOICE_FINISHED_SECONDS,
    JANITOR_INTERVAL_SECONDS,
    TRIGGER_TEXT,
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
    "vc-worker-load-en",
    "vc-worker-load-ko",
    "vc-worker-load-kr",
    "vc-worker-load-ja",
    "vc-worker-load-jp",
)
MUTABLE_CONFIG_KEYS = frozenset(set(CONFIG_KEY_ORDER) - SENSITIVE_CONFIG_KEYS)


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
    worker_finish_wait_seconds: float
    vc_timeout_seconds: float


class RuntimeConfig:
    def __init__(self, config_path: Path):
        self.config_path = config_path

    def get(self) -> Config:
        return _build_config(self._load_data())

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
        return _load_config_data(self.config_path)

    def _write_data(self, data: dict[str, Any]) -> None:
        lines = [f"{key} = {_format_toml_value(data[key])}" for key in _ordered_config_keys(data)]
        tmp_path = self.config_path.parent.joinpath(f".{self.config_path.name}.tmp")
        tmp_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        tmp_path.replace(self.config_path)


class SpeakiClient(discord.Client):
    def __init__(self, runtime_config: RuntimeConfig):
        intents = discord.Intents.none()
        intents.guilds = True
        intents.messages = True
        intents.message_content = True
        intents.voice_states = True

        super().__init__(intents=intents)
        self.runtime_config = runtime_config
        self.sessions: dict[int, SpeakiSession] = {}
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

        session = self.sessions.get(message.guild.id)
        if session is None:
            session = SpeakiSession(
                self,
                message.guild,
                self.runtime_config.get,
            )
            self.sessions[message.guild.id] = session

        try:
            await session.activate_for_channel(member.voice.channel, requested_by=user_label)
            session.touch()
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
        if member.bot:
            return

        session = self.sessions.get(member.guild.id)
        if session is None or session.current_channel_id is None:
            return

        before_channel = before.channel
        after_channel = after.channel
        session_channel_id = session.current_channel_id

        if before_channel is not None and before_channel.id == session_channel_id and (
            after_channel is None or after_channel.id != session_channel_id
        ):
            log.info(
                "speaki: info: %s left vc %s#%s",
                member,
                before_channel.name,
                before_channel.id,
            )

            remaining_humans = [
                channel_member
                for channel_member in before_channel.members
                if not channel_member.bot and channel_member.id != member.id
            ]
            if not remaining_humans:
                log.info(
                    "speaki: info: no humans left in vc %s#%s, leaving and shutting down worker",
                    before_channel.name,
                    before_channel.id,
                )
                closing_session = self.sessions.pop(member.guild.id, None)
                if closing_session is not None:
                    await closing_session.close(reason="last human left voice")

    async def _run_session_janitor(self) -> None:
        while True:
            await asyncio.sleep(JANITOR_INTERVAL_SECONDS)

            idle_guild_ids = [
                guild_id
                for guild_id, session in self.sessions.items()
                if session.is_idle()
            ]
            for guild_id in idle_guild_ids:
                session = self.sessions.pop(guild_id, None)
                if session is not None:
                    await session.close(reason="leaving voice due to inactivity")

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

        config_text = content[len(CONFIG_COMMAND):].lstrip()
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
        worker_finish_wait_seconds=worker_finish_wait,
        vc_timeout_seconds=vc_timeout,
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

    client = SpeakiClient(runtime_config)
    try:
        client.run(config.token, log_handler=None)
    except KeyboardInterrupt:
        log.info("speaki: info: Ctrl+C received, shutting down")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
