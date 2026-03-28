"""
speakispeak: Discord bot entrypoint
  with all my heart, 2026, mark joshwel <mark@joshwel.co>
  SPDX-License-Identifier: Unlicense OR 0BSD
"""

from __future__ import annotations

import asyncio
import logging
import multiprocessing
import os
import tomllib
from pathlib import Path
from typing import NamedTuple

import discord

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


class Config(NamedTuple):
    token: str
    worker_enabled: bool
    enabled_languages: tuple[str, ...]
    use_grammar: bool
    strict_final_only: bool
    strict_double_hit: bool
    debug: bool
    dump_worker_audio: bool
    worker_finish_wait_seconds: float
    vc_timeout_seconds: float


class SpeakiClient(discord.Client):
    def __init__(self, config: Config):
        intents = discord.Intents.none()
        intents.guilds = True
        intents.messages = True
        intents.message_content = True
        intents.voice_states = True

        super().__init__(intents=intents)
        self.config = config
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

        if message.content.strip().casefold() != TRIGGER_TEXT:
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
                self.config.enabled_languages if self.config.worker_enabled else (),
                worker_enabled=self.config.worker_enabled,
                use_grammar=self.config.use_grammar,
                strict_final_only=self.config.strict_final_only,
                strict_double_hit=self.config.strict_double_hit,
                debug=self.config.debug,
                dump_worker_audio=self.config.dump_worker_audio,
                worker_finish_wait_seconds=self.config.worker_finish_wait_seconds,
                vc_timeout_seconds=self.config.vc_timeout_seconds,
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


def load_config(config_path: Path) -> Config:
    data = tomllib.loads(config_path.read_text(encoding="utf-8"))

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


def configure_logging(*, debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
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
    config = load_config(Path("config.toml"))
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
    if not discord.opus.is_loaded():
        discord.opus._load_default()

    client = SpeakiClient(config)
    try:
        client.run(config.token, log_handler=None)
    except KeyboardInterrupt:
        log.info("speaki: info: Ctrl+C received, shutting down")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
