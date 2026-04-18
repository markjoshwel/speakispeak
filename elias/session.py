"""
speakispeak: guild voice session management
  with all my heart, 2026, mark joshwel <mark@joshwel.co>
  SPDX-License-Identifier: Unlicense OR 0BSD
"""

from __future__ import annotations

import asyncio
import logging
import multiprocessing
import threading
import time
from collections import deque
from pathlib import Path
from queue import Empty, Full
from typing import TYPE_CHECKING, Any, Callable

import discord
from discord.ext import voice_recv

from .audio import convert_discord_pcm_to_vosk_pcm
from .sink import SpeakiAudioSink
from .sounds import describe_sound, pick_random_sound
from .detection import format_recognised_log_window
from .state import (
    AudioChunk,
    DISCORD_CHANNELS,
    DISCORD_SAMPLE_RATE,
    EMPTY_VC_GRACE_SECONDS,
    PCM_SAMPLE_WIDTH_BYTES,
    PLAYBACK_COOLDOWN_SECONDS,
    SPEAKER_TRIGGER_COOLDOWN_SECONDS,
    STRICT_DOUBLE_HIT_WINDOW_SECONDS,
    VOICE_CONNECT_TIMEOUT_SECONDS,
    VOICE_DISCONNECT_TIMEOUT_SECONDS,
    VOICE_ERROR_BURST_RECOVERY_GRACE_SECONDS,
    VOICE_HEALTH_POLL_INTERVAL_SECONDS,
    VOICE_HEALTH_RECOVERY_GRACE_SECONDS,
    VOICE_HARD_RESET_RETRY_DELAY_SECONDS,
    VOICE_LISTENER_RECOVERY_GRACE_SECONDS,
    VOICE_POST_RECONNECT_DECRYPT_RESET_THRESHOLD,
    VOICE_POST_RECONNECT_OPUS_RESET_THRESHOLD,
    VOICE_POST_RECONNECT_UNSTABLE_SECONDS,
    VOICE_RECONNECT_WINDOW_SECONDS,
    VOICE_RECOVERY_MIN_INTERVAL_SECONDS,
    VOICE_SELF_DISCONNECT_CONFIRMATION_SECONDS,
    VOICE_SOFT_RECONNECT_LIMIT,
    WHISPER_CHUNK_OVERLAP_BYTES,
    WHISPER_CHUNK_TARGET_BYTES,
    WHISPER_MODEL_NAME,
    WhisperJob,
    WORKER_POOL_SIZE,
    WORKER_POLL_TIMEOUT_SECONDS,
    WORKER_QUEUE_MAXSIZE,
    WORKER_SCALE_DOWN_GRACE_SECONDS,
    WORKER_STARTUP_TIMEOUT_SECONDS,
    Shutdown,
    SpeakerIdle,
    TranscriptionRecord,
    TriggerEvent,
)
from .stt_worker import worker_main as _vosk_worker_main
from .whisper_worker import worker_main as _whisper_worker_main

if TYPE_CHECKING:
    from discord.abc import Connectable

log = logging.getLogger(__name__)

_TRANSCRIPTION_HISTORY_MAX = 15


class SpeakerRouter:
    """Routes audio chunks from the sink to a fixed pool of worker processes.

    Each active speaker is stickily assigned to one pool slot. When a speaker goes
    idle (signalled by the sink's on_speaker_idle callback), their slot is freed and
    a SpeakerIdle message is sent to the worker so it can clear per-user state.

    If all slots are occupied when a new speaker becomes active, the least-recently-
    active speaker's slot is evicted (LRU).
    """

    def __init__(
        self,
        pool_size: int,
        queues: list[Any],
        on_routing: Callable[[int, int], None] | None = None,
    ) -> None:
        self._pool_size = pool_size
        self._queues = queues
        self._on_routing = on_routing
        self._assignments: dict[int, int] = {}
        self._last_active: dict[int, float] = {}
        self._lock = threading.Lock()

    def route(self, chunk: AudioChunk) -> None:
        slot = self._assign(chunk.user_id)
        try:
            self._queues[slot].put_nowait(chunk)
        except Full:
            pass
        if self._on_routing is not None:
            self._on_routing(chunk.user_id, slot)

    def release(self, user_id: int) -> None:
        with self._lock:
            slot = self._assignments.pop(user_id, None)
            self._last_active.pop(user_id, None)
        if slot is not None:
            try:
                self._queues[slot].put_nowait(SpeakerIdle(user_id=user_id))
            except Full:
                pass

    def confirm_trigger(self, user_id: int, text: str, now: float) -> bool:
        """Vosk workers handle their own deduplication; always pass through here."""
        return True

    def _assign(self, user_id: int) -> int:
        with self._lock:
            now = time.monotonic()
            if user_id in self._assignments:
                self._last_active[user_id] = now
                return self._assignments[user_id]
            used = set(self._assignments.values())
            for slot in range(self._pool_size):
                if slot not in used:
                    self._assignments[user_id] = slot
                    self._last_active[user_id] = now
                    return slot
            # All slots busy — evict the least-recently-active speaker.
            lru_uid = min(self._last_active, key=self._last_active.__getitem__)
            slot = self._assignments.pop(lru_uid)
            del self._last_active[lru_uid]
            try:
                self._queues[slot].put_nowait(SpeakerIdle(user_id=lru_uid))
            except Full:
                pass
            self._assignments[user_id] = slot
            self._last_active[user_id] = now
            return slot


class WhisperSpeakerRouter:
    """Per-speaker audio buffer that dispatches WhisperJob chunks to a shared work queue.

    Unlike SpeakerRouter (which stickily assigns a speaker to one worker), all Whisper
    workers share a single queue and are stateless.  This router handles:
      - Accumulating 48 kHz stereo PCM per speaker until WHISPER_CHUNK_TARGET_BYTES
      - Keeping WHISPER_CHUNK_OVERLAP_BYTES as a tail for the next chunk so wakewords
        that straddle a boundary appear in both the outgoing job and the next one
      - Flushing remaining audio when a speaker goes idle (SpeakerIdle path)
      - Trigger cooldown / strict-double-hit deduplication (workers are stateless)

    Called from the discord.py voice receive thread via the sink callbacks, so all
    mutable state is protected by a threading.Lock.
    """

    def __init__(
        self,
        shared_queue: Any,
        *,
        guild_id: int,
        strict_double_hit: bool,
        worker_count: int = 1,
        on_routing: Callable[[int, int], None] | None = None,
    ) -> None:
        self._queue = shared_queue
        self._guild_id = guild_id
        self._strict_double_hit = strict_double_hit
        self._worker_count = max(1, worker_count)
        self._on_routing = on_routing
        self._next_worker_idx = 0
        self._lock = threading.Lock()
        self._buffers: dict[int, bytearray] = {}
        self._labels: dict[int, str] = {}
        self._buffer_last_update: dict[int, float] = {}
        self._user_worker_idx: dict[int, int] = {}  # sticky per-user dashboard slot
        self._last_trigger_text: dict[int, str] = {}
        self._last_trigger_monotonic: dict[int, float] = {}
        self._last_candidate_text: dict[int, str] = {}
        self._last_candidate_monotonic: dict[int, float] = {}
        self._candidate_hit_count: dict[int, int] = {}

    def route(self, chunk: AudioChunk) -> None:
        with self._lock:
            buf = self._buffers.setdefault(chunk.user_id, bytearray())
            self._labels[chunk.user_id] = chunk.user_label
            self._buffer_last_update[chunk.user_id] = time.monotonic()
            buf.extend(chunk.pcm)
            if len(buf) >= WHISPER_CHUNK_TARGET_BYTES:
                self._dispatch_and_trim(chunk.user_id, chunk.user_label)

    def release(self, user_id: int) -> None:
        """Flush any remaining buffer for a speaker that just went idle."""
        with self._lock:
            buf = self._buffers.pop(user_id, None)
            self._buffer_last_update.pop(user_id, None)
            self._user_worker_idx.pop(user_id, None)
            label = self._labels.pop(user_id, str(user_id))
            if buf:
                self._dispatch_pcm(user_id, label, bytes(buf))

    def flush_stale(self, max_age_seconds: float) -> None:
        """Dispatch buffers that haven't received new audio in max_age_seconds.

        Discord stops sending RTP packets when a user is silent, so on_speaker_idle
        never fires for the sink — the router buffer accumulates until the next
        utterance.  This method is called periodically to break that stall.
        """
        now = time.monotonic()
        with self._lock:
            stale = [
                uid for uid, last in self._buffer_last_update.items()
                if now - last >= max_age_seconds and self._buffers.get(uid)
            ]
            for uid in stale:
                label = self._labels.get(uid, str(uid))
                buf = self._buffers.pop(uid, None)
                self._buffer_last_update.pop(uid, None)
                self._user_worker_idx.pop(uid, None)
                if buf:
                    self._dispatch_pcm(uid, label, bytes(buf))

    def confirm_trigger(self, user_id: int, text: str, now: float) -> bool:
        """Return True if this trigger should be emitted (cooldown + optional double-hit)."""
        with self._lock:
            last_text = self._last_trigger_text.get(user_id, "")
            last_mono = self._last_trigger_monotonic.get(user_id, 0.0)
            if last_text == text and now - last_mono < SPEAKER_TRIGGER_COOLDOWN_SECONDS:
                return False

            if self._strict_double_hit:
                prev_candidate = self._last_candidate_text.get(user_id, "")
                prev_candidate_mono = self._last_candidate_monotonic.get(user_id, 0.0)
                if (
                    text == prev_candidate
                    and now - prev_candidate_mono <= STRICT_DOUBLE_HIT_WINDOW_SECONDS
                ):
                    self._candidate_hit_count[user_id] = self._candidate_hit_count.get(user_id, 0) + 1
                else:
                    self._last_candidate_text[user_id] = text
                    self._last_candidate_monotonic[user_id] = now
                    self._candidate_hit_count[user_id] = 1
                if self._candidate_hit_count[user_id] < 2:
                    return False

            self._last_trigger_text[user_id] = text
            self._last_trigger_monotonic[user_id] = now
            self._last_candidate_text.pop(user_id, None)
            self._last_candidate_monotonic.pop(user_id, None)
            self._candidate_hit_count.pop(user_id, None)
            return True

    def _dispatch_and_trim(self, user_id: int, user_label: str) -> None:
        buf = self._buffers[user_id]
        self._dispatch_pcm(user_id, user_label, bytes(buf))
        if len(buf) > WHISPER_CHUNK_OVERLAP_BYTES:
            del buf[:-WHISPER_CHUNK_OVERLAP_BYTES]
        else:
            buf.clear()

    def _dispatch_pcm(self, user_id: int, user_label: str, pcm_48k: bytes) -> None:
        pcm_16k = convert_discord_pcm_to_vosk_pcm(pcm_48k)
        if not pcm_16k:
            return
        job = WhisperJob(
            guild_id=self._guild_id,
            user_id=user_id,
            user_label=user_label,
            pcm_16k_mono=pcm_16k,
            enqueued_at=time.monotonic(),
        )
        try:
            self._queue.put_nowait(job)
        except Full:
            pass
        if self._on_routing is not None:
            if user_id not in self._user_worker_idx:
                self._user_worker_idx[user_id] = self._next_worker_idx % self._worker_count
                self._next_worker_idx += 1
            self._on_routing(user_id, self._user_worker_idx[user_id])


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
        dashboard_emit: Callable[[dict[str, Any]], None] | None = None,
    ):
        self.client = client
        self.guild = guild
        self.config_provider = config_provider
        self.dashboard_emit = dashboard_emit
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
        self.use_whisper: bool = False
        self.whisper_model: str = WHISPER_MODEL_NAME
        self.worker_pool_size: int = WORKER_POOL_SIZE
        self.worker_context = multiprocessing.get_context("spawn")
        self.worker_processes: list[multiprocessing.Process] = []
        self.worker_input_queues: list[Any] = []
        self.worker_output_queue: Any | None = None
        self.worker_ready_events: list[Any] = []
        self.worker_shutdown_events: list[Any] = []
        self.worker_consumer_task: asyncio.Task[None] | None = None
        self.worker_signature: tuple[Any, ...] | None = None
        self._speaker_router: SpeakerRouter | None = None
        self.receive_sink: SpeakiAudioSink | None = None
        self.last_activity_monotonic = time.monotonic()
        self.last_playback_monotonic = 0.0
        self._health_monitor_task: asyncio.Task[None] | None = asyncio.create_task(
            self._run_voice_health_monitor()
        )
        self._recovery_task: asyncio.Task[None] | None = None
        self._queued_recovery_reason: str | None = None
        self._queued_recovery_hard = False
        self._voice_unhealthy_since_monotonic = 0.0
        self._listener_unhealthy_since_monotonic = 0.0
        self._receive_error_unhealthy_since_monotonic = 0.0
        self._last_recovery_monotonic = 0.0
        self._last_recv_diag: dict[str, int] = {}
        self._recent_transport_reconnects: deque[float] = deque()
        self._post_reconnect_unstable_until_monotonic = 0.0
        self._self_session_id: str | None = None
        self._transport_epoch = 0
        self._last_dave_opcode_at = 0.0
        self._last_dave_opcode = -1
        self._last_transport_epoch_seen_by_dave = -1
        self._self_disconnect_task: asyncio.Task[None] | None = None
        self._scale_down_task: asyncio.Task[None] | None = None
        self._empty_vc_since_monotonic: float = 0.0
        self.close_requested_reason: str | None = None
        self._closed = False
        self._transcription_history: dict[str, list[dict[str, Any]]] = {}
        self._bot_status: str = "loading"
        self._bot_status_detail: str = "starting"
        self.master_user_ids: frozenset[int] = frozenset()
        self._load_runtime_config()

    async def activate_for_channel(self, channel: Connectable, *, requested_by: str) -> None:
        if self._closed:
            raise RuntimeError("Session is already closed")

        await self._wait_for_active_recovery_task()

        async with self.activation_lock:
            self._load_runtime_config()
            if self.worker_enabled and not self.worker_processes:
                self._ensure_worker()
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

    def request_close(self, reason: str) -> None:
        """Signal to the owning client that this session should be closed."""
        if not self._closed and self.close_requested_reason is None:
            self.close_requested_reason = reason

    def on_member_count_changed(self, *, joined: bool) -> None:
        """Called from main.py when a non-bot member joins or leaves the VC."""
        if self._closed:
            return
        if joined:
            asyncio.create_task(self._scale_pool_immediate())
        else:
            if self._scale_down_task is None or self._scale_down_task.done():
                self._scale_down_task = asyncio.create_task(self._scale_pool_deferred())
        self._emit_session_state()

    async def _scale_pool_immediate(self) -> None:
        if self._closed:
            return
        async with self.activation_lock:
            if self._closed:
                return
            await self._sync_worker_state(reason="member joined, scaling pool up")

    async def _scale_pool_deferred(self) -> None:
        await asyncio.sleep(WORKER_SCALE_DOWN_GRACE_SECONDS)
        if self._closed:
            return
        async with self.activation_lock:
            if self._closed:
                return
            await self._sync_worker_state(reason="deferred pool scale-down after member left")

    def _desired_pool_size(self) -> int:
        """Worker pool size capped to the number of humans currently in the VC."""
        channel = self._get_current_channel()
        if channel is None:
            return max(1, self.worker_pool_size)
        humans = sum(1 for m in getattr(channel, "members", []) if not m.bot and m.id not in self.master_user_ids)
        return max(1, min(humans if humans > 0 else self.worker_pool_size, self.worker_pool_size))

    # ── Dashboard event helpers ───────────────────────────────────────────

    def _emit(self, event: dict[str, Any]) -> None:
        if self.dashboard_emit is not None:
            self.dashboard_emit(event)

    def _emit_session_state(self) -> None:
        channel = self._get_current_channel()
        if channel is None:
            return
        members = [
            {
                "user_id": str(m.id),
                "user_label": str(m),
                "avatar_url": str(m.display_avatar.url) if hasattr(m, "display_avatar") else None,
            }
            for m in getattr(channel, "members", [])
            if not m.bot and m.id not in self.master_user_ids
        ]
        desired = self._desired_pool_size()
        self._emit({
            "type": "session_state",
            "guild_name": self.guild.name,
            "channel_name": channel.name,
            "worker_count": desired,
            "max_workers": self.worker_pool_size,
            "members": members,
            "transcription_history": dict(self._transcription_history),
            "bot_status": self._bot_status,
            "bot_status_detail": self._bot_status_detail,
        })

    def _emit_bot_status(self, status: str, detail: str = "") -> None:
        if self._bot_status == status and self._bot_status_detail == detail:
            return
        self._bot_status = status
        self._bot_status_detail = detail
        self._emit({"type": "bot_status", "status": status, "detail": detail})

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
                        await self._recover_voice_transport(
                            reason=f"playback start failed: {type(exc).__name__}",
                            hard=True,
                        )
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
        self._emit({"type": "session_close", "reason": reason})
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
                self._reset_transport_supervision_state()
            self._bind_voice_client_listeners(current)
            self.voice_client = current
            self.current_channel_id = channel.id
            self._reset_health_state()
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
            if await self._try_adopt_connected_voice_client(channel):
                return
            await self._cleanup_failed_voice_client()
            raise
        except Exception:
            await self._cleanup_failed_voice_client()
            raise

        self.voice_client = connected
        self.current_channel_id = channel.id
        self._bind_voice_client_listeners(connected)
        self._reset_transport_supervision_state()
        self._reset_health_state()

    def _get_current_channel(self) -> Connectable | None:
        if self.current_channel_id is None:
            return None

        channel = self.guild.get_channel(self.current_channel_id)
        if channel is None:
            return None
        return channel  # type: ignore[return-value]

    def _bind_voice_client_listeners(self, voice_client: voice_recv.VoiceRecvClient) -> None:
        if getattr(voice_client, "_speaki_transport_listeners_bound", False):
            return

        voice_client.add_listener(
            self._on_voice_transport_reconnect_scheduled,
            name="on_voice_transport_reconnect_scheduled",
        )
        voice_client.add_listener(
            self._on_voice_transport_reconnected,
            name="on_voice_transport_reconnected",
        )
        voice_client.add_listener(
            self._on_voice_transport_closed,
            name="on_voice_transport_closed",
        )
        voice_client.add_listener(
            self._on_voice_dave_opcode,
            name="on_voice_dave_opcode",
        )
        setattr(voice_client, "_speaki_transport_listeners_bound", True)

    async def _wait_for_active_recovery_task(self) -> None:
        task = self._recovery_task
        if task is None or task.done():
            return

        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

    async def _try_adopt_connected_voice_client(self, channel: Connectable, *, grace_seconds: float = 2.5) -> bool:
        deadline = time.monotonic() + grace_seconds
        while time.monotonic() < deadline:
            candidates = [self.voice_client, self.guild.voice_client]
            for candidate in candidates:
                if not isinstance(candidate, voice_recv.VoiceRecvClient):
                    continue
                if candidate.guild.id != self.guild.id:
                    continue
                candidate_channel = getattr(candidate, "channel", None)
                if candidate_channel is None or candidate_channel.id != channel.id:
                    continue
                if not candidate.is_connected():
                    continue

                self.voice_client = candidate
                self.current_channel_id = channel.id
                self._bind_voice_client_listeners(candidate)
                self._reset_transport_supervision_state()
                self._reset_health_state()
                return True
            await asyncio.sleep(0.2)
        return False

    def _cancel_self_disconnect_task(self) -> None:
        if self._self_disconnect_task is None:
            return
        self._self_disconnect_task.cancel()
        self._self_disconnect_task = None

    def _schedule_self_disconnect_check(self) -> None:
        if self._closed:
            return
        if self._self_disconnect_task is not None and not self._self_disconnect_task.done():
            return

        self._self_disconnect_task = asyncio.create_task(self._confirm_self_disconnect())

    async def _confirm_self_disconnect(self) -> None:
        current_task = asyncio.current_task()
        try:
            await asyncio.sleep(VOICE_SELF_DISCONNECT_CONFIRMATION_SECONDS)
            if self._closed or self.current_channel_id is None:
                return

            current_member = self.guild.get_member(self.client.user.id) if self.client.user is not None else None
            current_channel = current_member.voice.channel if current_member and current_member.voice else None
            if current_channel is not None:
                self.current_channel_id = current_channel.id
                return

            voice_client = self.voice_client
            if voice_client is not None and voice_client.is_connected():
                return

            self._schedule_recovery(reason="bot remained detached from voice", hard=True)
        except asyncio.CancelledError:
            raise
        finally:
            if self._self_disconnect_task is current_task:
                self._self_disconnect_task = None

    async def handle_self_voice_state_update(
        self,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ) -> None:
        if self._closed:
            return

        before_channel_id = before.channel.id if before.channel is not None else None
        after_channel_id = after.channel.id if after.channel is not None else None
        if after_channel_id is not None:
            self.current_channel_id = after_channel_id
            self._cancel_self_disconnect_task()

        session_id = after.session_id
        if isinstance(session_id, str):
            if (
                self._self_session_id is not None
                and self._self_session_id != session_id
                and after_channel_id is not None
                and after_channel_id == before_channel_id
            ):
                self._transport_epoch += 1
                self._mark_post_reconnect_unstable()
            self._self_session_id = session_id

        if after_channel_id is None and self.current_channel_id is not None:
            self._schedule_self_disconnect_check()

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

    def _transport_diag_float(self, diagnostics: dict[str, Any], key: str) -> float | None:
        value = diagnostics.get(key)
        if isinstance(value, (int, float)):
            return float(value)
        return None

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
            "voice_ws_last_op": self._recv_diag_int(diagnostics, "voice_ws_last_op"),
            "dave_ws_last_op": self._recv_diag_int(diagnostics, "dave_ws_last_op"),
            "dave_ws_total": self._recv_diag_int(diagnostics, "dave_ws_total"),
        }

    def _transport_diag_snapshot(
        self,
        voice_client: voice_recv.VoiceRecvClient | None = None,
    ) -> dict[str, Any]:
        client = voice_client if voice_client is not None else self.voice_client
        if client is None:
            return {}

        try:
            diagnostics = client.get_transport_diagnostics()
        except Exception:
            return {}

        if not isinstance(diagnostics, dict):
            return {}

        recent_reconnects = diagnostics.get("recent_reconnects")
        reconnect_count = len(recent_reconnects) if isinstance(recent_reconnects, list) else 0
        return {
            "recent_reconnect_count": reconnect_count,
            "last_close_code": diagnostics.get("last_close_code"),
            "last_retry_delay": self._transport_diag_float(diagnostics, "last_retry_delay"),
            "last_reconnect_at": self._transport_diag_float(diagnostics, "last_reconnect_at"),
            "consecutive_reconnect_failures": self._recv_diag_int(
                diagnostics,
                "consecutive_reconnect_failures",
            ),
            "last_session_id": diagnostics.get("last_session_id"),
            "last_transport_epoch": self._recv_diag_int(diagnostics, "last_transport_epoch"),
        }

    def _voice_health_summary(self, voice_client: voice_recv.VoiceRecvClient | None = None) -> str:
        client = voice_client if voice_client is not None else self.voice_client
        if client is None:
            return "voice-client=missing"

        diagnostics = self._recv_diag_snapshot(client)
        transport = self._transport_diag_snapshot(client)
        return (
            f"state={self._voice_connection_state_name(client)} "
            f"connected={client.is_connected()} "
            f"listening={client.is_listening()} "
            f"workers={sum(1 for p in self.worker_processes if p.is_alive())}/{len(self.worker_processes)} "
            f"recv={diagnostics} "
            f"transport={transport} "
            f"epoch={self._transport_epoch}"
        )

    def _reset_health_state(self) -> None:
        self._voice_unhealthy_since_monotonic = 0.0
        self._listener_unhealthy_since_monotonic = 0.0
        self._receive_error_unhealthy_since_monotonic = 0.0
        self._last_recv_diag = self._recv_diag_snapshot()

    def _reset_transport_supervision_state(self) -> None:
        self._recent_transport_reconnects.clear()
        self._post_reconnect_unstable_until_monotonic = 0.0
        self._last_dave_opcode_at = 0.0
        self._last_dave_opcode = -1
        self._last_transport_epoch_seen_by_dave = -1

    def _mark_post_reconnect_unstable(self) -> None:
        self._post_reconnect_unstable_until_monotonic = (
            time.monotonic() + VOICE_POST_RECONNECT_UNSTABLE_SECONDS
        )
        self._last_recv_diag = self._recv_diag_snapshot()

    def _prune_recent_transport_reconnects(self, *, now: float) -> None:
        while (
            self._recent_transport_reconnects
            and now - self._recent_transport_reconnects[0] > VOICE_RECONNECT_WINDOW_SECONDS
        ):
            self._recent_transport_reconnects.popleft()

    def _record_transport_reconnect_attempt(self, *, now: float, retry_delay: float) -> tuple[int, bool]:
        self._prune_recent_transport_reconnects(now=now)
        self._recent_transport_reconnects.append(now)
        attempts = len(self._recent_transport_reconnects)
        hard_reset = (
            attempts > VOICE_SOFT_RECONNECT_LIMIT
            or retry_delay > VOICE_HARD_RESET_RETRY_DELAY_SECONDS
        )
        return attempts, hard_reset

    def _had_recent_dave_transition(self, *, now: float) -> bool:
        return (
            self._last_dave_opcode in {21, 22, 23, 24}
            and now - self._last_dave_opcode_at <= VOICE_POST_RECONNECT_UNSTABLE_SECONDS
            and self._last_transport_epoch_seen_by_dave >= max(0, self._transport_epoch - 1)
        )

    def _poisoned_receive_reason(
        self,
        *,
        now: float,
        decrypt_error_delta: int,
        opus_decode_err_delta: int,
        pcm_frames_delta: int,
    ) -> str | None:
        if now > self._post_reconnect_unstable_until_monotonic:
            return None

        reason_prefix = "post-reconnect transport poison"
        if self._had_recent_dave_transition(now=now):
            reason_prefix = "post-reconnect DAVE transport poison"

        if decrypt_error_delta >= VOICE_POST_RECONNECT_DECRYPT_RESET_THRESHOLD:
            if pcm_frames_delta <= 0 or decrypt_error_delta >= VOICE_POST_RECONNECT_DECRYPT_RESET_THRESHOLD * 2:
                return (
                    f"{reason_prefix}: decrypt+={decrypt_error_delta}, "
                    f"pcm+={pcm_frames_delta}"
                )
            return None

        if (
            opus_decode_err_delta >= VOICE_POST_RECONNECT_OPUS_RESET_THRESHOLD
            and pcm_frames_delta <= 0
        ):
            return (
                f"{reason_prefix}: opus+={opus_decode_err_delta}, "
                f"pcm+={pcm_frames_delta}"
            )

        return None

    async def _ensure_voice_ready_for_playback(
        self,
        *,
        reason: str,
    ) -> voice_recv.VoiceRecvClient | None:
        await self._wait_for_active_recovery_task()

        voice_client = self.voice_client
        if voice_client is not None and voice_client.is_connected():
            return voice_client

        if self.current_channel_id is None:
            return None

        await self._recover_voice_transport(
            reason=f"voice not connected for playback ({reason})",
            hard=True,
        )
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
                reason=f"playback failed during {trigger_reason}: {type(error).__name__}",
                hard=True,
            )

        return after

    def _schedule_recovery(self, *, reason: str, hard: bool) -> None:
        if self._closed:
            return

        loop = self.client.loop
        if loop.is_closed():
            return

        def create_task() -> None:
            if self._closed:
                return

            if self._recovery_task is not None and not self._recovery_task.done():
                if hard:
                    self._queued_recovery_hard = True
                    self._queued_recovery_reason = reason
                elif self._queued_recovery_reason is None:
                    self._queued_recovery_reason = reason
                return

            self._queued_recovery_reason = None
            self._queued_recovery_hard = False
            task = asyncio.create_task(self._run_recovery_pipeline(reason=reason, hard=hard))
            self._recovery_task = task
            task.add_done_callback(self._on_recovery_task_done)

        loop.call_soon_threadsafe(create_task)

    async def _run_recovery_pipeline(self, *, reason: str, hard: bool) -> None:
        next_reason = reason
        next_hard = hard
        while not self._closed:
            if next_hard:
                await self._recover_voice_transport(reason=next_reason, hard=True)
            else:
                await self._recover_listener(reason=next_reason)

            if self._queued_recovery_reason is None:
                break

            next_reason = self._queued_recovery_reason
            next_hard = self._queued_recovery_hard
            self._queued_recovery_reason = None
            self._queued_recovery_hard = False

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

        if self._recovery_task is not None and not self._recovery_task.done():
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

        poisoned_reason = self._poisoned_receive_reason(
            now=now,
            decrypt_error_delta=decrypt_error_delta,
            opus_decode_err_delta=opus_decode_err_delta,
            pcm_frames_delta=pcm_frames_delta,
        )
        if poisoned_reason is not None:
            await self._recover_voice_transport(reason=poisoned_reason, hard=True)
            return

        if not voice_client.is_connected():
            if self._voice_unhealthy_since_monotonic == 0.0:
                self._voice_unhealthy_since_monotonic = now
            elif now - self._voice_unhealthy_since_monotonic >= VOICE_HEALTH_RECOVERY_GRACE_SECONDS:
                await self._recover_voice_transport(
                    reason=f"voice transport unhealthy ({self._voice_connection_state_name(voice_client)})",
                    hard=True,
                )
            return

        self._voice_unhealthy_since_monotonic = 0.0

        # Ghost-VC guard: if we've been alone for long enough, request self-close.
        channel = self._get_current_channel()
        if channel is not None:
            humans = [m for m in getattr(channel, "members", []) if not m.bot and m.id not in self.master_user_ids]
            if not humans:
                if self._empty_vc_since_monotonic == 0.0:
                    self._empty_vc_since_monotonic = now
                elif now - self._empty_vc_since_monotonic >= EMPTY_VC_GRACE_SECONDS:
                    log.info(
                        "speaki: info: alone in vc %s#%s for %.0fs, requesting close",
                        channel.name,
                        channel.id,
                        EMPTY_VC_GRACE_SECONDS,
                    )
                    self.request_close("left empty voice channel")
                return
            else:
                self._empty_vc_since_monotonic = 0.0

        listener_healthy = True
        if self.worker_enabled:
            listener_healthy = (
                voice_client.is_listening()
                and (self.worker_consumer_task is not None and not self.worker_consumer_task.done())
                and all(process.is_alive() for process in self.worker_processes)
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
                    ),
                    hard=True,
                )
            return

        self._receive_error_unhealthy_since_monotonic = 0.0

        # Flush any router buffers that Discord stopped sending packets for.
        # Threshold 0.8 s: hangover is 0.35 s, so anything older is genuinely idle.
        if isinstance(self._speaker_router, WhisperSpeakerRouter):
            self._speaker_router.flush_stale(0.8)

    async def _on_voice_transport_reconnect_scheduled(self, payload: dict[str, Any]) -> None:
        if self._closed:
            return

        now = time.monotonic()
        retry_delay = payload.get("retry_delay", 0.0)
        retry_delay_value = float(retry_delay) if isinstance(retry_delay, (int, float)) else 0.0
        attempts, hard_reset = self._record_transport_reconnect_attempt(
            now=now,
            retry_delay=retry_delay_value,
        )
        if not hard_reset:
            return

        trigger = (
            f"retry delay {retry_delay_value:.2f}s exceeds "
            f"{VOICE_HARD_RESET_RETRY_DELAY_SECONDS:.2f}s"
            if retry_delay_value > VOICE_HARD_RESET_RETRY_DELAY_SECONDS
            else f"voice reconnect attempt {attempts} within {VOICE_RECONNECT_WINDOW_SECONDS:.0f}s"
        )
        self._schedule_recovery(reason=trigger, hard=True)

    async def _on_voice_transport_reconnected(self, payload: dict[str, Any]) -> None:
        if self._closed:
            return

        session_id = payload.get("session_id")
        if isinstance(session_id, str):
            self._self_session_id = session_id

        transport_epoch = payload.get("transport_epoch")
        if isinstance(transport_epoch, int):
            self._transport_epoch = max(self._transport_epoch, transport_epoch)

        self._mark_post_reconnect_unstable()

    async def _on_voice_transport_closed(self, payload: dict[str, Any]) -> None:
        if self._closed or self.current_channel_id is None:
            return

        reconnect_enabled = payload.get("reconnect_enabled")
        if reconnect_enabled is False:
            self._schedule_recovery(reason="voice transport closed without reconnect", hard=True)

    async def _on_voice_dave_opcode(self, opcode: int, _payload: dict[str, Any]) -> None:
        if not isinstance(opcode, int):
            return

        self._last_dave_opcode_at = time.monotonic()
        self._last_dave_opcode = opcode
        self._last_transport_epoch_seen_by_dave = self._transport_epoch

    async def _recover_listener(self, *, reason: str) -> None:
        if self._closed:
            return

        now = time.monotonic()
        if now - self._last_recovery_monotonic < VOICE_RECOVERY_MIN_INTERVAL_SECONDS:
            return

        voice_client = self.voice_client
        if voice_client is None or not voice_client.is_connected():
            await self._recover_voice_transport(reason=reason, hard=True)
            return

        async with self.activation_lock:
            if self._closed:
                return

            self._emit_bot_status("reconnecting", reason[:60])
            self._last_recovery_monotonic = time.monotonic()
            log.warning(
                "speaki: warning: recovering listener for guild %s (%s) [%s]",
                self.guild.id,
                reason,
                self._voice_health_summary(voice_client),
            )
            self._stop_listener()
            await self._sync_worker_state(
                reason=f"listener recovery: {reason}",
                force_restart=not all(process.is_alive() for process in self.worker_processes),
            )
            self._reset_health_state()

    async def _recover_voice_transport(self, *, reason: str, hard: bool) -> None:
        if self._closed or self.current_channel_id is None:
            return

        now = time.monotonic()
        if not hard and now - self._last_recovery_monotonic < VOICE_RECOVERY_MIN_INTERVAL_SECONDS:
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

            # Don't reconnect to a channel that has no humans — prevents the ghost-rejoin bug.
            humans = [m for m in getattr(channel, "members", []) if not m.bot and m.id not in self.master_user_ids]
            if not humans:
                log.info(
                    "speaki: info: not reconnecting to empty vc %s#%s in guild %s",
                    channel.name,
                    channel.id,
                    self.guild.id,
                )
                self.current_channel_id = None
                self.voice_client = None
                self.request_close("refused to reconnect to empty voice channel")
                return

            self._emit_bot_status("reconnecting", reason[:60])
            self._last_recovery_monotonic = time.monotonic()
            log.warning(
                "speaki: warning: %s for guild %s (%s) [%s]",
                "performing hard voice transport reset" if hard else "recovering voice transport",
                self.guild.id,
                reason,
                self._voice_health_summary(voice_client),
            )

            self._stop_listener()
            if hard:
                await self._stop_worker_consumer_task()
                self._request_worker_shutdown(reason=f"hard voice recovery: {reason}")
                await self._stop_worker_processes()
            if voice_client is not None:
                await self._disconnect_voice_client(
                    voice_client,
                    reason=f"voice recovery: {reason}",
                    clear_state=False,
                )
                if self.voice_client is voice_client:
                    self.voice_client = None
                self.receive_sink = None

            try:
                connected = await channel.connect(
                    cls=voice_recv.VoiceRecvClient,
                    timeout=VOICE_CONNECT_TIMEOUT_SECONDS,
                    reconnect=True,
                )
            except TimeoutError:
                if await self._try_adopt_connected_voice_client(channel):
                    await self._sync_worker_state(
                        reason=f"voice recovery adoption: {reason}",
                        force_restart=hard,
                    )
                    return
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
            self._bind_voice_client_listeners(connected)
            self._reset_transport_supervision_state()
            self._mark_post_reconnect_unstable()
            await self._sync_worker_state(
                reason=f"voice recovery: {reason}",
                force_restart=hard,
            )
            self._reset_health_state()

    def _ensure_worker(self) -> None:
        desired_size = self._desired_pool_size()
        desired_signature = self._current_worker_signature()
        active_count = sum(1 for p in self.worker_processes if p.is_alive())
        if (
            active_count == desired_size
            and self.worker_signature == desired_signature
        ):
            if self.worker_consumer_task is None:
                self.worker_consumer_task = asyncio.create_task(self._consume_worker_events())
            return

        emit = self.dashboard_emit

        def on_routing(user_id: int, worker_idx: int) -> None:
            if emit is not None:
                emit({"type": "worker_routing", "user_id": str(user_id), "worker_idx": worker_idx})

        self.worker_output_queue = self.worker_context.Queue(maxsize=WORKER_QUEUE_MAXSIZE)
        processes: list[multiprocessing.Process] = []
        input_queues: list[Any] = []
        ready_events: list[Any] = []
        shutdown_events: list[Any] = []

        if self.use_whisper:
            worker_target = _whisper_worker_main
            worker_kind = f"whisper/{self.whisper_model}"
            # All Whisper workers share one queue — stateless, first-available dispatch.
            shared_input_queue = self.worker_context.Queue(maxsize=WORKER_QUEUE_MAXSIZE)
        else:
            worker_target = _vosk_worker_main
            worker_kind = f"vosk/{'+'.join(self.enabled_languages)}"
            shared_input_queue = None

        self._emit_bot_status("loading", f"loading {worker_kind}")
        for idx in range(desired_size):
            input_queue = shared_input_queue if self.use_whisper else self.worker_context.Queue(maxsize=WORKER_QUEUE_MAXSIZE)
            ready_event = self.worker_context.Event()
            shutdown_event = self.worker_context.Event()
            if self.use_whisper:
                worker_args: tuple[Any, ...] = (
                    input_queue,
                    self.worker_output_queue,
                    ready_event,
                    shutdown_event,
                    self.enabled_languages,
                    self.whisper_model,
                    self.debug,
                )
            else:
                worker_args = (
                    input_queue,
                    self.worker_output_queue,
                    ready_event,
                    shutdown_event,
                    self.enabled_languages,
                    self.use_grammar,
                    self.strict_final_only,
                    self.strict_double_hit,
                    self.debug,
                    self.dump_worker_audio,
                    f"guild_{self.guild.id}_pool{idx}",
                    self.worker_finish_wait_seconds,
                )
            process = self.worker_context.Process(
                target=worker_target,
                args=worker_args,
                daemon=True,
                name=f"speaki-worker-{self.guild.id}-{idx}",
            )
            process.start()
            processes.append(process)
            input_queues.append(input_queue)
            ready_events.append(ready_event)
            shutdown_events.append(shutdown_event)

        self.worker_processes = processes
        self.worker_input_queues = input_queues
        self.worker_ready_events = ready_events
        self.worker_shutdown_events = shutdown_events
        if self.use_whisper:
            self._speaker_router = WhisperSpeakerRouter(
                shared_input_queue,
                guild_id=self.guild.id,
                strict_double_hit=self.strict_double_hit,
                worker_count=desired_size,
                on_routing=on_routing,
            )
        else:
            self._speaker_router = SpeakerRouter(
                pool_size=desired_size,
                queues=self.worker_input_queues,
                on_routing=on_routing,
            )
        self.worker_signature = desired_signature
        self.worker_consumer_task = asyncio.create_task(self._consume_worker_events())
        log.info(
            "speaki: info: spawned %s-worker pool for guild %s (%s); strict-double-hit=%s; max=%s",
            desired_size,
            self.guild.id,
            worker_kind,
            self.strict_double_hit,
            self.worker_pool_size,
        )
        self._emit({
            "type": "worker_pool_resize",
            "count": desired_size,
            "max": self.worker_pool_size,
        })

    async def wait_until_worker_ready(self) -> None:
        if not self.worker_ready_events:
            return

        wait_results = await asyncio.gather(
            *[
                asyncio.to_thread(ready_event.wait, WORKER_STARTUP_TIMEOUT_SECONDS)
                for ready_event in self.worker_ready_events
            ]
        )
        if not all(wait_results):
            raise TimeoutError("speaki: error: worker startup timed out")

    def _ensure_listener(self) -> None:
        if self.voice_client is None or self._speaker_router is None or self.current_channel_id is None:
            return

        if self.voice_client.is_listening():
            return

        emit = self.dashboard_emit

        def on_live(user_id: int, user_label: str, avatar_url: str | None, amplitude: float) -> None:
            if emit is not None:
                emit({
                    "type": "live_audio",
                    "user_id": str(user_id),
                    "user_label": user_label,
                    "avatar_url": avatar_url,
                    "amplitude": amplitude,
                })

        self.receive_sink = SpeakiAudioSink(
            guild_id=self.guild.id,
            channel_id=self.current_channel_id,
            route_chunk=self._speaker_router.route,
            on_speaker_idle=self._speaker_router.release,
            on_live_amplitude=on_live if emit is not None else None,
            ignore_user_ids=self.master_user_ids,
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
            self._schedule_recovery(
                reason=f"voice receive stopped: {type(error).__name__}",
                hard=False,
            )

    async def _consume_worker_events(self) -> None:
        try:
            while not self._closed and self.worker_output_queue is not None:
                event = await asyncio.to_thread(self._poll_worker_output)
                if event is None:
                    continue

                if isinstance(event, TranscriptionRecord):
                    uid = str(event.user_id)
                    history = self._transcription_history.setdefault(uid, [])
                    history.append({"text": event.text, "wakeword": event.wakeword})
                    if len(history) > _TRANSCRIPTION_HISTORY_MAX:
                        del history[:-_TRANSCRIPTION_HISTORY_MAX]
                    self._emit({
                        "type": "transcription",
                        "user_id": uid,
                        "user_label": event.user_label,
                        "text": event.text,
                        "wakeword": event.wakeword,
                    })
                    if event.wakeword is not None:
                        router = self._speaker_router
                        if router is not None and not router.confirm_trigger(
                            event.user_id, event.wakeword, time.monotonic()
                        ):
                            continue
                        self.touch()
                        sound_path = await self.play_random_sound(force=False, voice_trigger_text=event.wakeword)
                        if sound_path is not None:
                            log.info(
                                "speaki: info: [whisper: %s] wakeword trigger=%s recognised=%s (playing sfx %s)",
                                event.user_label,
                                event.wakeword,
                                format_recognised_log_window(event.text, event.wakeword),
                                describe_sound(sound_path),
                            )
                        else:
                            log.info(
                                "speaki: info: [whisper: %s] wakeword trigger=%s recognised=%s (no sound — cooling down or no sounds dir)",
                                event.user_label,
                                event.wakeword,
                                format_recognised_log_window(event.text, event.wakeword),
                            )
                elif isinstance(event, TriggerEvent):
                    router = self._speaker_router
                    if router is not None and not router.confirm_trigger(
                        event.user_id, event.text, time.monotonic()
                    ):
                        continue
                    self.touch()
                    # Find which worker index produced this (approximate for Whisper)
                    worker_idx = 0
                    if isinstance(router, WhisperSpeakerRouter):
                        worker_idx = (router._next_worker_idx - 1) % max(1, len(self.worker_processes))
                    self._emit({
                        "type": "trigger",
                        "user_id": str(event.user_id),
                        "user_label": event.user_label,
                        "text": event.text,
                        "worker_idx": worker_idx,
                    })
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
                    else:
                        log.info(
                            "speaki: info: [worker: %s] recognised %s from audio stream: trigger=%s recognised=%s (no sound — cooling down or no sounds dir)",
                            event.user_label,
                            event.trigger_kind,
                            event.text,
                            format_recognised_log_window(event.recognised_text, event.text),
                        )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not self._closed:
                log.exception(
                    "speaki: error: worker event consumer crashed for guild %s",
                    self.guild.id,
                )
                self._schedule_recovery(
                    reason=f"worker consumer crashed: {type(exc).__name__}",
                    hard=False,
                )

    def _poll_worker_output(self) -> TranscriptionRecord | TriggerEvent | None:
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
        for shutdown_event in self.worker_shutdown_events:
            shutdown_event.set()

        # Send one Shutdown per worker process so each one can exit its blocking get().
        # For Whisper (shared queue), worker_input_queues has the same queue repeated N
        # times, so this correctly enqueues N Shutdown messages into the one shared queue.
        for input_queue in self.worker_input_queues:
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
            self._speaker_router = None
            return

        # dict.fromkeys preserves order and deduplicates (important for Whisper's shared queue).
        input_queues = list(dict.fromkeys(self.worker_input_queues))
        output_queue = self.worker_output_queue

        for process in self.worker_processes:
            await asyncio.to_thread(process.join, 3.0)

        for process in self.worker_processes:
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
        self._speaker_router = None

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
        for task in (self._recovery_task, self._health_monitor_task, self._scale_down_task):
            if task is None:
                continue

            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self._recovery_task = None
        self._health_monitor_task = None
        self._scale_down_task = None
        self._queued_recovery_reason = None
        self._queued_recovery_hard = False
        self._cancel_self_disconnect_task()

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

        # Cancel the WS poll loop before disconnecting. `connection.disconnect()` does not
        # cancel `_runner`, so without this the old _poll_voice_ws task keeps running and
        # races the new connection's gateway handshake — both send VOICE_STATE_UPDATE
        # simultaneously, Discord assigns two session IDs, and both get 4006-kicked in
        # a loop.
        if connection is not None:
            runner = getattr(connection, "_runner", None)
            if runner is not None and not runner.done():
                runner.cancel()
            connection._runner = None

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
        self.use_whisper = config.use_whisper
        self.whisper_model = config.whisper_model
        self.worker_pool_size = config.worker_pool_size
        self.worker_finish_wait_seconds = config.worker_finish_wait_seconds
        self.vc_timeout_seconds = config.vc_timeout_seconds
        self.master_user_ids = config.master_user_ids

    def _current_worker_signature(self) -> tuple[Any, ...]:
        return (
            self.enabled_languages,
            self.use_grammar,
            self.strict_final_only,
            self.strict_double_hit,
            self.debug,
            self.dump_worker_audio,
            self._desired_pool_size(),   # dynamic: changes when VC headcount changes
            self.worker_finish_wait_seconds,
            self.use_whisper,
            self.whisper_model,
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

    async def _sync_worker_state(self, *, reason: str, force_restart: bool = False) -> str:
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
        active_count = sum(1 for p in self.worker_processes if p.is_alive())
        worker_consumer_dead = (
            self.worker_consumer_task is not None and self.worker_consumer_task.done()
        )
        if (
            force_restart
            or (
                self.worker_processes
                and (
                    active_count < len(self.worker_processes)
                    or self.worker_signature != self._current_worker_signature()
                    or worker_consumer_dead
                )
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
        self._emit_bot_status("listening")
        self._reset_health_state()
        if restarted:
            return "restarted"
        if not had_workers and self.worker_processes:
            return "enabled"
        return "unchanged"
