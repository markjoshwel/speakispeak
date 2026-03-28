"""
speakispeak: Discord voice receive sink
  with all my heart, 2026, mark joshwel <mark@joshwel.co>
  SPDX-License-Identifier: Unlicense OR 0BSD
"""

from __future__ import annotations

import logging
import time
from queue import Full
from typing import Any

import discord
from discord.ext import voice_recv

from .state import (
    AudioChunk,
    DISCORD_CHANNELS,
    DISCORD_SAMPLE_RATE,
    PCM_SAMPLE_WIDTH_BYTES,
    SINK_BATCH_WINDOW_SECONDS,
    SINK_MAX_BUFFER_SECONDS,
)

log = logging.getLogger(__name__)

MAX_BUFFER_BYTES = int(DISCORD_SAMPLE_RATE * DISCORD_CHANNELS * PCM_SAMPLE_WIDTH_BYTES * SINK_MAX_BUFFER_SECONDS)


class SpeakiAudioSink(voice_recv.AudioSink):
    def __init__(self, guild_id: int, channel_id: int, input_queues: tuple[Any, ...]):
        super().__init__()
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.input_queues = input_queues
        self.dropped_chunks = 0
        self.trimmed_buffers = 0
        self._last_drop_log_monotonic = 0.0
        self._speaker_buffers: dict[int, bytearray] = {}
        self._speaker_labels: dict[int, str] = {}
        self._speaker_last_flush_monotonic: dict[int, float] = {}
        self._speaker_last_drop_monotonic: dict[int, float] = {}
        self._closed = False

    def wants_opus(self) -> bool:
        return False

    def write(self, user: discord.User | discord.Member | None, data: voice_recv.VoiceData) -> None:
        if self._closed:
            return

        if user is None or getattr(user, "bot", False):
            return

        member = user if isinstance(user, discord.Member) else None
        if member is None or member.voice is None or member.voice.channel is None:
            return

        if member.voice.channel.id != self.channel_id:
            return

        pcm = data.pcm
        if not pcm:
            return

        now = time.monotonic()
        buffer = self._speaker_buffers.setdefault(member.id, bytearray())
        buffer.extend(pcm)
        if len(buffer) > MAX_BUFFER_BYTES:
            del buffer[:-MAX_BUFFER_BYTES]
            self.trimmed_buffers += 1
        self._speaker_labels[member.id] = str(member)

        last_flush = self._speaker_last_flush_monotonic.get(member.id, now)
        if now - last_flush < SINK_BATCH_WINDOW_SECONDS and len(buffer) < 3840 * 10:
            return

        self._speaker_last_flush_monotonic[member.id] = now
        chunk = AudioChunk(
            guild_id=self.guild_id,
            user_id=member.id,
            user_label=str(member),
            pcm=bytes(buffer),
            received_monotonic=now,
        )
        buffer.clear()

        queue_full = False
        for input_queue in self.input_queues:
            try:
                input_queue.put_nowait(chunk)
            except Full:
                queue_full = True

        if queue_full:
            self.dropped_chunks += 1
            self._speaker_last_drop_monotonic[member.id] = now
            if now - self._last_drop_log_monotonic >= 5.0:
                queue_size = "unknown"
                try:
                    queue_size = ",".join(str(input_queue.qsize()) for input_queue in self.input_queues)
                except Exception:
                    pass
                log.warning(
                    "speaki: warning: dropped %s voice chunks in guild %s (trimmed=%s, queue=%s)",
                    self.dropped_chunks,
                    self.guild_id,
                    self.trimmed_buffers,
                    queue_size,
                )
                self._last_drop_log_monotonic = now

    def cleanup(self) -> None:
        self._closed = True
        now = time.monotonic()
        for user_id, buffer in list(self._speaker_buffers.items()):
            if not buffer:
                continue

            chunk = AudioChunk(
                guild_id=self.guild_id,
                user_id=user_id,
                user_label=self._speaker_labels.get(user_id, str(user_id)),
                pcm=bytes(buffer),
                received_monotonic=now,
            )
            try:
                for input_queue in self.input_queues:
                    input_queue.put_nowait(chunk)
            except Full:
                self.dropped_chunks += 1
            buffer.clear()
        return None

    def shutdown(self) -> None:
        self._closed = True
        self._speaker_buffers.clear()
        self._speaker_labels.clear()
        self._speaker_last_flush_monotonic.clear()
        self._speaker_last_drop_monotonic.clear()
        self.input_queues = ()
