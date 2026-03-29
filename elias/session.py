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
    VOICE_CONNECT_TIMEOUT_SECONDS,
    VOICE_DISCONNECT_TIMEOUT_SECONDS,
    VOICE_ERROR_BURST_RECOVERY_GRACE_SECONDS,
    VOICE_HEALTH_POLL_INTERVAL_SECONDS,
    VOICE_HEALTH_RECOVERY_GRACE_SECONDS,
    VOICE_LISTENER_RECOVERY_GRACE_SECONDS,
    VOICE_RECOVERY_MIN_INTERVAL_SECONDS,
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
        self._health_monitor_task: asyncio.Task[None] | None = asyncio.create_task(
            self._run_voice_health_monitor()
        )
        self._recovery_task: asyncio.Task[None] | None = None
        self._voice_unhealthy_since_monotonic = 0.0
        self._listener_unhealthy_since_monotonic = 0.0
        self._receive_error_unhealthy_since_monotonic = 0.0
        self._last_recovery_monotonic = 0.0
        self._last_recv_diag: dict[str, int] = {}
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
        sound_path = pick_random_sound()

        async with self.playback_lock:
            now = time.monotonic()
            if not force and now - self.last_playback_monotonic < PLAYBACK_COOLDOWN_SECONDS:
                return None

            for attempt in range(2):
                voice_client = await self._ensure_voice_ready_for_playback(
                    reason=voice_trigger_text or "typed trigger"
                )
                if voice_client is None:
                    return None

                if voice_client.is_playing():
                    if not force:
                        return None
                    voice_client.stop_playing()

                source = discord.FFmpegPCMAudio(str(sound_path))
                try:
                    voice_client.play(
                        source,
                        after=self._build_playback_after_callback(
                            sound_path=sound_path,
                            voice_trigger_text=voice_trigger_text,
                        ),
                    )
                except Exception as exc:
                    source.cleanup()
                    log.warning(
                        "speaki: warning: playback start failed in guild %s on attempt %s (%s)",
                        self.guild.id,
                        attempt + 1,
                        exc,
                    )
                    if attempt == 0:
                        await self._recover_voice_transport(reason=f"playback start failed: {type(exc).__name__}")
                        continue
                    raise

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

        return None

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

        await self._stop_background_tasks()
        self._stop_listener()
        await self._stop_worker_consumer_task()

        if self.voice_client is not None:
            self.voice_client.stop()

        self._request_worker_shutdown(reason=reason)
        await self._stop_worker_processes()

        if self.voice_client is not None:
            await self._disconnect_voice_client(self.voice_client, reason=reason, clear_state=True)

        self.receive_sink = None
        self.current_channel_id = None

    async def _ensure_connected(self, channel: Connectable) -> None:
        current = self.voice_client
        if current is not None and current.is_connected():
            if current.channel.id != channel.id:
                try:
                    await current.move_to(channel, timeout=VOICE_CONNECT_TIMEOUT_SECONDS)
                except Exception:
                    await self._disconnect_voice_client(
                        current,
                        reason="failed moving voice client to requested channel",
                        clear_state=True,
                    )
                    raise
                current.stop_listening()
            self.voice_client = current
            self.current_channel_id = channel.id
            return

        if current is not None:
            await self._disconnect_voice_client(
                current,
                reason="discarding stale voice client before connect",
                clear_state=True,
            )

        try:
            connected = await channel.connect(
                cls=voice_recv.VoiceRecvClient,
                timeout=VOICE_CONNECT_TIMEOUT_SECONDS,
                reconnect=True,
            )
        except TimeoutError:
            await self._cleanup_failed_voice_client()
            raise
        except Exception:
            await self._cleanup_failed_voice_client()
            raise

        self.voice_client = connected
        self.current_channel_id = channel.id
        self._reset_health_state()

    def _get_current_channel(self) -> Connectable | None:
        if self.current_channel_id is None:
            return None

        channel = self.guild.get_channel(self.current_channel_id)
        if channel is None:
            return None
        return channel  # type: ignore[return-value]

    def _voice_connection_state_name(self, voice_client: voice_recv.VoiceRecvClient | None = None) -> str:
        client = voice_client if voice_client is not None else self.voice_client
        if client is None:
            return "missing"

        connection = getattr(client, "_connection", None)
        state = getattr(connection, "state", None)
        return getattr(state, "name", "unknown")

    def _recv_diag_int(self, diagnostics: dict[str, Any], key: str) -> int:
        value = diagnostics.get(key, 0)
        return value if isinstance(value, int) else 0

    def _recv_diag_snapshot(self, voice_client: voice_recv.VoiceRecvClient | None = None) -> dict[str, int]:
        client = voice_client if voice_client is not None else self.voice_client
        if client is None:
            return {}

        try:
            diagnostics = client.get_recv_diagnostics()
        except Exception:
            return {}

        if not isinstance(diagnostics, dict):
            return {}

        return {
            "decrypt_error": self._recv_diag_int(diagnostics, "decrypt_error"),
            "opus_decode_err": self._recv_diag_int(diagnostics, "opus_decode_err"),
            "pcm_frames": self._recv_diag_int(diagnostics, "pcm_frames"),
            "rtp_packets_total": self._recv_diag_int(diagnostics, "rtp_packets_total"),
        }

    def _voice_health_summary(self, voice_client: voice_recv.VoiceRecvClient | None = None) -> str:
        client = voice_client if voice_client is not None else self.voice_client
        if client is None:
            return "voice-client=missing"

        diagnostics = self._recv_diag_snapshot(client)
        return (
            f"state={self._voice_connection_state_name(client)} "
            f"connected={client.is_connected()} "
            f"listening={client.is_listening()} "
            f"workers={sorted(self.worker_processes)} "
            f"diag={diagnostics}"
        )

    def _reset_health_state(self) -> None:
        self._voice_unhealthy_since_monotonic = 0.0
        self._listener_unhealthy_since_monotonic = 0.0
        self._receive_error_unhealthy_since_monotonic = 0.0
        self._last_recv_diag = self._recv_diag_snapshot()

    async def _ensure_voice_ready_for_playback(
        self,
        *,
        reason: str,
    ) -> voice_recv.VoiceRecvClient | None:
        voice_client = self.voice_client
        if voice_client is not None and voice_client.is_connected():
            return voice_client

        if self.current_channel_id is None:
            return None

        await self._recover_voice_transport(reason=f"voice not connected for playback ({reason})")
        voice_client = self.voice_client
        if voice_client is None or not voice_client.is_connected():
            return None
        return voice_client

    def _build_playback_after_callback(
        self,
        *,
        sound_path: Path,
        voice_trigger_text: str | None,
    ) -> Callable[[Exception | None], None]:
        def after(error: Exception | None) -> None:
            if error is None or self._closed:
                return

            log.exception(
                "speaki: error: playback failed in guild %s while playing %s",
                self.guild.id,
                describe_sound(sound_path),
                exc_info=error,
            )
            trigger_reason = voice_trigger_text or "typed trigger"
            self._schedule_recovery(
                reason=f"playback failed during {trigger_reason}: {type(error).__name__}"
            )

        return after

    def _schedule_recovery(self, *, reason: str) -> None:
        if self._closed:
            return

        loop = self.client.loop
        if loop.is_closed():
            return

        def create_task() -> None:
            if self._closed:
                return

            if self._recovery_task is not None and not self._recovery_task.done():
                return

            task = asyncio.create_task(self._recover_voice_transport(reason=reason))
            self._recovery_task = task
            task.add_done_callback(self._on_recovery_task_done)

        loop.call_soon_threadsafe(create_task)

    def _on_recovery_task_done(self, task: asyncio.Task[None]) -> None:
        if self._recovery_task is task:
            self._recovery_task = None

        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("speaki: error: background recovery failed for guild %s", self.guild.id)

    async def _run_voice_health_monitor(self) -> None:
        while not self._closed:
            await asyncio.sleep(VOICE_HEALTH_POLL_INTERVAL_SECONDS)
            try:
                await self._check_voice_health()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception(
                    "speaki: error: voice health monitor failed for guild %s",
                    self.guild.id,
                )

    async def _check_voice_health(self) -> None:
        if self._closed or self.activation_lock.locked():
            return

        voice_client = self.voice_client
        if self.current_channel_id is None or voice_client is None:
            self._reset_health_state()
            return

        now = time.monotonic()
        diagnostics = self._recv_diag_snapshot(voice_client)
        decrypt_error_delta = self._recv_diag_int(diagnostics, "decrypt_error") - self._last_recv_diag.get(
            "decrypt_error",
            0,
        )
        opus_decode_err_delta = self._recv_diag_int(diagnostics, "opus_decode_err") - self._last_recv_diag.get(
            "opus_decode_err",
            0,
        )
        pcm_frames_delta = self._recv_diag_int(diagnostics, "pcm_frames") - self._last_recv_diag.get(
            "pcm_frames",
            0,
        )
        self._last_recv_diag = diagnostics

        if not voice_client.is_connected():
            if self._voice_unhealthy_since_monotonic == 0.0:
                self._voice_unhealthy_since_monotonic = now
            elif now - self._voice_unhealthy_since_monotonic >= VOICE_HEALTH_RECOVERY_GRACE_SECONDS:
                await self._recover_voice_transport(
                    reason=f"voice transport unhealthy ({self._voice_connection_state_name(voice_client)})"
                )
            return

        self._voice_unhealthy_since_monotonic = 0.0

        listener_healthy = True
        if self.worker_enabled:
            listener_healthy = (
                voice_client.is_listening()
                and (self.worker_consumer_task is not None and not self.worker_consumer_task.done())
                and all(process.is_alive() for process in self.worker_processes.values())
            )

        if not listener_healthy:
            if self._listener_unhealthy_since_monotonic == 0.0:
                self._listener_unhealthy_since_monotonic = now
            elif now - self._listener_unhealthy_since_monotonic >= VOICE_LISTENER_RECOVERY_GRACE_SECONDS:
                await self._recover_listener(reason="listener or worker path unhealthy")
            return

        self._listener_unhealthy_since_monotonic = 0.0

        receive_error_burst = decrypt_error_delta >= 3 or (
            opus_decode_err_delta >= 24 and pcm_frames_delta <= 0
        )
        if receive_error_burst:
            if self._receive_error_unhealthy_since_monotonic == 0.0:
                self._receive_error_unhealthy_since_monotonic = now
            elif (
                now - self._receive_error_unhealthy_since_monotonic
                >= VOICE_ERROR_BURST_RECOVERY_GRACE_SECONDS
            ):
                await self._recover_voice_transport(
                    reason=(
                        "receive errors spiking "
                        f"(decrypt+={decrypt_error_delta}, opus+={opus_decode_err_delta}, pcm+={pcm_frames_delta})"
                    )
                )
            return

        self._receive_error_unhealthy_since_monotonic = 0.0

    async def _recover_listener(self, *, reason: str) -> None:
        if self._closed:
            return

        voice_client = self.voice_client
        if voice_client is None or not voice_client.is_connected():
            await self._recover_voice_transport(reason=reason)
            return

        async with self.activation_lock:
            if self._closed:
                return

            log.warning(
                "speaki: warning: recovering listener for guild %s (%s) [%s]",
                self.guild.id,
                reason,
                self._voice_health_summary(voice_client),
            )
            self._stop_listener()
            await self._sync_worker_state(reason=f"listener recovery: {reason}")
            self._reset_health_state()

    async def _recover_voice_transport(self, *, reason: str) -> None:
        if self._closed or self.current_channel_id is None:
            return

        now = time.monotonic()
        if now - self._last_recovery_monotonic < VOICE_RECOVERY_MIN_INTERVAL_SECONDS:
            return

        async with self.activation_lock:
            if self._closed or self.current_channel_id is None:
                return

            voice_client = self.voice_client
            channel = self._get_current_channel()
            if channel is None:
                log.warning(
                    "speaki: warning: current tracked voice channel disappeared for guild %s during recovery",
                    self.guild.id,
                )
                self.current_channel_id = None
                self.voice_client = None
                return

            self._last_recovery_monotonic = time.monotonic()
            log.warning(
                "speaki: warning: recovering voice transport for guild %s (%s) [%s]",
                self.guild.id,
                reason,
                self._voice_health_summary(voice_client),
            )

            self._stop_listener()
            if voice_client is not None:
                await self._disconnect_voice_client(
                    voice_client,
                    reason=f"voice recovery: {reason}",
                    clear_state=True,
                )

            try:
                connected = await channel.connect(
                    cls=voice_recv.VoiceRecvClient,
                    timeout=VOICE_CONNECT_TIMEOUT_SECONDS,
                    reconnect=True,
                )
            except TimeoutError:
                log.warning(
                    "speaki: warning: timed out reconnecting voice transport for guild %s (%s)",
                    self.guild.id,
                    reason,
                )
                return
            except Exception:
                log.exception(
                    "speaki: error: failed reconnecting voice transport for guild %s (%s)",
                    self.guild.id,
                    reason,
                )
                return

            self.voice_client = connected
            self.current_channel_id = channel.id
            await self._sync_worker_state(reason=f"voice recovery: {reason}")
            self._reset_health_state()

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
            self._schedule_recovery(reason=f"voice receive stopped: {type(error).__name__}")

    async def _consume_worker_events(self) -> None:
        try:
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
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not self._closed:
                log.exception(
                    "speaki: error: worker event consumer crashed for guild %s",
                    self.guild.id,
                )
                self._schedule_recovery(reason=f"worker consumer crashed: {type(exc).__name__}")

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

        await self._disconnect_voice_client(
            voice_client,
            reason="cleaning up failed voice client",
            clear_state=False,
        )

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

    async def _stop_background_tasks(self) -> None:
        for task in (self._recovery_task, self._health_monitor_task):
            if task is None:
                continue

            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self._recovery_task = None
        self._health_monitor_task = None

    async def _disconnect_voice_client(
        self,
        voice_client: voice_recv.VoiceRecvClient,
        *,
        reason: str,
        clear_state: bool,
    ) -> None:
        try:
            voice_client.stop()
        except Exception:
            pass

        connection = getattr(voice_client, "_connection", None)
        try:
            if connection is not None:
                await asyncio.wait_for(
                    connection.disconnect(force=True, cleanup=True, wait=False),
                    timeout=VOICE_DISCONNECT_TIMEOUT_SECONDS,
                )
            else:
                await asyncio.wait_for(
                    voice_client.disconnect(force=True),
                    timeout=VOICE_DISCONNECT_TIMEOUT_SECONDS,
                )
        except TimeoutError:
            log.warning(
                "speaki: warning: timed out disconnecting voice client for guild %s (%s)",
                self.guild.id,
                reason,
            )
        except Exception:
            log.warning(
                "speaki: warning: failed disconnecting voice client for guild %s (%s)",
                self.guild.id,
                reason,
                exc_info=True,
            )
        finally:
            try:
                voice_client.cleanup()
            except Exception:
                pass

            if clear_state and self.voice_client is voice_client:
                self.voice_client = None
                self.current_channel_id = None
                self.receive_sink = None

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
            self._reset_health_state()
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
        self._reset_health_state()
        if restarted:
            return "restarted"
        if not had_workers and self.worker_processes:
            return "enabled"
        return "unchanged"
