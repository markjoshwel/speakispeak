"""
speakispeak: guild voice session management
  with all my heart, 2026, mark joshwel <mark@joshwel.co>
  SPDX-License-Identifier: Unlicense OR 0BSD
"""

from __future__ import annotations

import asyncio
import logging
import multiprocessing
import time
from pathlib import Path
from queue import Empty, Full
from typing import TYPE_CHECKING, Any, Callable

import discord
from discord.ext import voice_recv

from .sink import SpeakiAudioSink
from .sounds import describe_sound, pick_random_sound
from .detection import format_recognised_log_window
from .state import (
    PLAYBACK_COOLDOWN_SECONDS,
    WORKER_POLL_TIMEOUT_SECONDS,
    WORKER_QUEUE_MAXSIZE,
    WORKER_STARTUP_TIMEOUT_SECONDS,
    Shutdown,
    TriggerEvent,
)
from .stt_worker import worker_main

if TYPE_CHECKING:
    from discord.abc import Connectable

log = logging.getLogger(__name__)


class SessionRefreshResult:
    def __init__(
        self,
        *,
        worker_enabled: bool,
        changed: bool,
        action: str,
        channel_id: int | None,
    ):
        self.worker_enabled = worker_enabled
        self.changed = changed
        self.action = action
        self.channel_id = channel_id


class SpeakiSession:
    def __init__(
        self,
        client: discord.Client,
        guild: discord.Guild,
        config_provider: Callable[[], Any],
    ):
        self.client = client
        self.guild = guild
        self.config_provider = config_provider
        self.worker_enabled = False
        self.enabled_languages: tuple[str, ...] = ()
        self.use_grammar = True
        self.strict_final_only = True
        self.strict_double_hit = True
        self.debug = False
        self.dump_worker_audio = False
        self.worker_finish_wait_seconds = 0.0
        self.vc_timeout_seconds = 0.0
        self.voice_client: voice_recv.VoiceRecvClient | None = None
        self.current_channel_id: int | None = None
        self.activation_lock = asyncio.Lock()
        self.playback_lock = asyncio.Lock()
        self.worker_context = multiprocessing.get_context("spawn")
        self.worker_processes: dict[str, multiprocessing.Process] = {}
        self.worker_input_queues: dict[str, Any] = {}
        self.worker_output_queue: Any | None = None
        self.worker_ready_events: dict[str, Any] = {}
        self.worker_shutdown_events: dict[str, Any] = {}
        self.worker_consumer_task: asyncio.Task[None] | None = None
        self.worker_signature: tuple[Any, ...] | None = None
        self.receive_sink: SpeakiAudioSink | None = None
        self.last_activity_monotonic = time.monotonic()
        self.last_playback_monotonic = 0.0
        self._closed = False
        self._load_runtime_config()

    async def activate_for_channel(self, channel: Connectable, *, requested_by: str) -> None:
        if self._closed:
            raise RuntimeError("Session is already closed")

        async with self.activation_lock:
            self._load_runtime_config()
            await self._ensure_connected(channel)
            await self._sync_worker_state(reason=f"activation requested by {requested_by}")
            self.touch()
            if self.worker_enabled:
                log.info(
                    "speaki: info: joining vc: session worker active after request from %s in %s#%s",
                    requested_by,
                    channel.name,
                    channel.id,
                )
            else:
                log.info(
                    "speaki: info: joining vc: worker disabled for session request from %s in %s#%s",
                    requested_by,
                    channel.name,
                    channel.id,
                )

    def touch(self) -> None:
        self.last_activity_monotonic = time.monotonic()

    def is_idle(self, now: float | None = None) -> bool:
        self._load_runtime_config()
        current = now if now is not None else time.monotonic()
        return current - self.last_activity_monotonic >= self.vc_timeout_seconds

    async def play_random_sound(self, *, force: bool, voice_trigger_text: str | None = None) -> Path | None:
        if self.voice_client is None or not self.voice_client.is_connected():
            return None

        sound_path = pick_random_sound()

        async with self.playback_lock:
            if self.voice_client is None:
                return None

            now = time.monotonic()
            if not force and now - self.last_playback_monotonic < PLAYBACK_COOLDOWN_SECONDS:
                return None

            if self.voice_client.is_playing():
                if not force:
                    return None
                self.voice_client.stop()

            source = discord.FFmpegPCMAudio(str(sound_path))
            self.voice_client.play(source)
            self.last_playback_monotonic = now
            if voice_trigger_text:
                log.info(
                    "speaki: info: playing sfx %s (voice trigger: %s)",
                    describe_sound(sound_path),
                    voice_trigger_text,
                )
            else:
                log.info("speaki: info: playing sfx %s", describe_sound(sound_path))
            return sound_path

    async def refresh_runtime_config(self) -> SessionRefreshResult:
        if self._closed:
            return SessionRefreshResult(
                worker_enabled=self.worker_enabled,
                changed=False,
                action="closed",
                channel_id=self.current_channel_id,
            )

        async with self.activation_lock:
            previous_runtime_snapshot = self._current_runtime_snapshot()
            previous_signature = self.worker_signature
            previous_worker_enabled = self.worker_enabled
            had_workers = bool(self.worker_processes)
            channel_id = self.current_channel_id
            self._load_runtime_config()
            action = await self._sync_worker_state(reason="live config update")
            current_runtime_snapshot = self._current_runtime_snapshot()
            if action == "unchanged" and previous_runtime_snapshot != current_runtime_snapshot:
                action = "updated"
            changed = (
                action not in {"unchanged", "disconnected"}
                or previous_worker_enabled != self.worker_enabled
                or previous_signature != self.worker_signature
                or had_workers != bool(self.worker_processes)
                or previous_runtime_snapshot != current_runtime_snapshot
            )
            return SessionRefreshResult(
                worker_enabled=self.worker_enabled,
                changed=changed,
                action=action,
                channel_id=channel_id,
            )

    async def close(self, *, reason: str = "session closed") -> None:
        if self._closed:
            return

        self._closed = True
        log.info("speaki: info: shutting down session for guild %s (%s)", self.guild.id, reason)

        self._stop_listener()
        await self._stop_worker_consumer_task()

        if self.voice_client is not None:
            self.voice_client.stop()

        self._request_worker_shutdown(reason=reason)
        await self._stop_worker_processes()

        if self.voice_client is not None:
            if self.voice_client.is_connected():
                try:
                    await asyncio.wait_for(self.voice_client.disconnect(force=True), timeout=3.0)
                except TimeoutError:
                    log.warning(
                        "speaki: warning: timed out disconnecting voice client for guild %s",
                        self.guild.id,
                    )
            self.voice_client = None

        self.receive_sink = None
        self.current_channel_id = None

    async def _ensure_connected(self, channel: Connectable) -> None:
        current = self.voice_client
        if current is not None and current.is_connected():
            if current.channel.id != channel.id:
                await current.move_to(channel)
                current.stop_listening()
            self.voice_client = current
            self.current_channel_id = channel.id
            return

        try:
            connected = await channel.connect(cls=voice_recv.VoiceRecvClient)
        except TimeoutError:
            await self._cleanup_failed_voice_client()
            raise

        self.voice_client = connected
        self.current_channel_id = channel.id

    def _ensure_worker(self) -> None:
        desired_signature = self._current_worker_signature()
        active_languages = [
            language
            for language, process in self.worker_processes.items()
            if process.is_alive()
        ]
        if (
            active_languages
            and set(active_languages) == set(self.enabled_languages)
            and self.worker_signature == desired_signature
        ):
            if self.worker_consumer_task is None:
                self.worker_consumer_task = asyncio.create_task(self._consume_worker_events())
            return

        self.worker_output_queue = self.worker_context.Queue(maxsize=WORKER_QUEUE_MAXSIZE)

        for language in self.enabled_languages:
            input_queue = self.worker_context.Queue(maxsize=WORKER_QUEUE_MAXSIZE)
            ready_event = self.worker_context.Event()
            shutdown_event = self.worker_context.Event()
            process = self.worker_context.Process(
                target=worker_main,
                args=(
                    input_queue,
                    self.worker_output_queue,
                    ready_event,
                    shutdown_event,
                    (language,),
                    self.use_grammar,
                    self.strict_final_only,
                    self.strict_double_hit,
                    self.debug,
                    self.dump_worker_audio,
                    f"guild_{self.guild.id}_{language}",
                    self.worker_finish_wait_seconds,
                ),
                daemon=True,
                name=f"speaki-worker-{self.guild.id}-{language}",
            )
            process.start()
            self.worker_processes[language] = process
            self.worker_input_queues[language] = input_queue
            self.worker_ready_events[language] = ready_event
            self.worker_shutdown_events[language] = shutdown_event

        self.worker_signature = desired_signature
        self.worker_consumer_task = asyncio.create_task(self._consume_worker_events())
        log.info(
            "speaki: info: spawned workers for guild %s with languages: %s; grammar=%s; strict-final-only=%s; strict-double-hit=%s",
            self.guild.id,
            ", ".join(self.enabled_languages),
            self.use_grammar,
            self.strict_final_only,
            self.strict_double_hit,
        )

    async def wait_until_worker_ready(self) -> None:
        if not self.worker_ready_events:
            return

        wait_results = await asyncio.gather(
            *[
                asyncio.to_thread(ready_event.wait, WORKER_STARTUP_TIMEOUT_SECONDS)
                for ready_event in self.worker_ready_events.values()
            ]
        )
        if not all(wait_results):
            raise TimeoutError("speaki: error: worker startup timed out")

    def _ensure_listener(self) -> None:
        if self.voice_client is None or not self.worker_input_queues or self.current_channel_id is None:
            return

        if self.voice_client.is_listening():
            return

        self.receive_sink = SpeakiAudioSink(
            guild_id=self.guild.id,
            channel_id=self.current_channel_id,
            input_queues=tuple(self.worker_input_queues.values()),
        )
        self.voice_client.listen(self.receive_sink, after=self._after_listening)

    def _stop_listener(self) -> None:
        if self.receive_sink is not None:
            self.receive_sink.shutdown()
            self.receive_sink = None

        if self.voice_client is not None and self.voice_client.is_listening():
            self.voice_client.stop_listening()

    def _after_listening(self, error: Exception | None) -> None:
        if error is not None:
            log.exception("speaki: error: voice receive stopped in guild %s", self.guild.id, exc_info=error)

    async def _consume_worker_events(self) -> None:
        while not self._closed and self.worker_output_queue is not None:
            event = await asyncio.to_thread(self._poll_worker_output)
            if event is None:
                continue

            if isinstance(event, TriggerEvent):
                self.touch()
                sound_path = await self.play_random_sound(force=False, voice_trigger_text=event.text)
                if sound_path is not None:
                    log.info(
                        "speaki: info: [worker: %s] recognised %s from audio stream: trigger=%s recognised=%s (playing sfx %s)",
                        event.user_label,
                        event.trigger_kind,
                        event.text,
                        format_recognised_log_window(event.recognised_text, event.text),
                        describe_sound(sound_path),
                    )

    def _poll_worker_output(self) -> TriggerEvent | None:
        if self.worker_output_queue is None:
            return None

        try:
            return self.worker_output_queue.get(timeout=WORKER_POLL_TIMEOUT_SECONDS)
        except Empty:
            return None

    def _request_worker_shutdown(self, *, reason: str) -> None:
        if not self.worker_shutdown_events:
            return

        log.info("speaki: info: signalling worker shutdown for guild %s (%s)", self.guild.id, reason)
        for shutdown_event in self.worker_shutdown_events.values():
            shutdown_event.set()

        for input_queue in self.worker_input_queues.values():
            try:
                input_queue.put_nowait(Shutdown(reason=reason))
            except Full:
                pass

    async def _stop_worker_processes(self) -> None:
        await self._stop_worker_consumer_task()

        if not self.worker_processes:
            self.worker_processes.clear()
            self.worker_input_queues.clear()
            self.worker_output_queue = None
            self.worker_ready_events.clear()
            self.worker_shutdown_events.clear()
            self.worker_signature = None
            return

        input_queues = list(self.worker_input_queues.values())
        output_queue = self.worker_output_queue

        for process in self.worker_processes.values():
            await asyncio.to_thread(process.join, 3.0)

        for process in self.worker_processes.values():
            if process.is_alive():
                process.terminate()
                await asyncio.to_thread(process.join, 1.0)

        for queue in input_queues:
            await self._close_worker_queue(queue)

        if output_queue is not None:
            await self._close_worker_queue(output_queue)

        self.worker_processes.clear()
        self.worker_input_queues.clear()
        self.worker_output_queue = None
        self.worker_ready_events.clear()
        self.worker_shutdown_events.clear()
        self.worker_signature = None

    async def _cleanup_failed_voice_client(self) -> None:
        voice_client = self.voice_client
        self._stop_listener()
        self.voice_client = None
        self.current_channel_id = None

        if voice_client is None:
            return

        try:
            voice_client.stop()
        except Exception:
            pass

        try:
            if voice_client.is_connected():
                await voice_client.disconnect(force=True)
        except Exception:
            pass

    async def _close_worker_queue(self, queue: Any) -> None:
        try:
            queue.cancel_join_thread()
        except Exception:
            pass

        try:
            await asyncio.to_thread(queue.close)
        except Exception:
            return

    async def _stop_worker_consumer_task(self) -> None:
        if self.worker_consumer_task is None:
            return

        self.worker_consumer_task.cancel()
        try:
            await self.worker_consumer_task
        except asyncio.CancelledError:
            pass
        self.worker_consumer_task = None

    def _load_runtime_config(self) -> None:
        config = self.config_provider()
        self.worker_enabled = config.worker_enabled
        self.enabled_languages = config.enabled_languages
        self.use_grammar = config.use_grammar
        self.strict_final_only = config.strict_final_only
        self.strict_double_hit = config.strict_double_hit
        self.debug = config.debug
        self.dump_worker_audio = config.dump_worker_audio
        self.worker_finish_wait_seconds = config.worker_finish_wait_seconds
        self.vc_timeout_seconds = config.vc_timeout_seconds

    def _current_worker_signature(self) -> tuple[Any, ...]:
        return (
            self.enabled_languages,
            self.use_grammar,
            self.strict_final_only,
            self.strict_double_hit,
            self.debug,
            self.dump_worker_audio,
            self.worker_finish_wait_seconds,
        )

    def _current_runtime_snapshot(self) -> tuple[Any, ...]:
        return (
            self.worker_enabled,
            self.enabled_languages,
            self.use_grammar,
            self.strict_final_only,
            self.strict_double_hit,
            self.debug,
            self.dump_worker_audio,
            self.worker_finish_wait_seconds,
            self.vc_timeout_seconds,
        )

    async def _sync_worker_state(self, *, reason: str) -> str:
        if self.voice_client is None or not self.voice_client.is_connected():
            return "disconnected"

        if not self.worker_enabled:
            if self.worker_processes or self.worker_input_queues:
                log.info(
                    "speaki: info: disabling workers for guild %s (%s)",
                    self.guild.id,
                    reason,
                )
            self._stop_listener()
            self._request_worker_shutdown(reason=reason)
            await self._stop_worker_processes()
            return "disabled"

        restarted = False
        active_languages = [
            language
            for language, process in self.worker_processes.items()
            if process.is_alive()
        ]
        if (
            self.worker_processes
            and (
                set(active_languages) != set(self.enabled_languages)
                or self.worker_signature != self._current_worker_signature()
            )
        ):
            log.info(
                "speaki: info: restarting workers for guild %s (%s)",
                self.guild.id,
                reason,
            )
            restarted = True
            self._stop_listener()
            self._request_worker_shutdown(reason=reason)
            await self._stop_worker_processes()

        had_workers = bool(self.worker_processes)
        self._ensure_worker()
        await self.wait_until_worker_ready()
        self._ensure_listener()
        if restarted:
            return "restarted"
        if not had_workers and self.worker_processes:
            return "enabled"
        return "unchanged"
