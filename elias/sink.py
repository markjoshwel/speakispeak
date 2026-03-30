"""
speakispeak: Discord voice receive sink
  with all my heart, 2026, mark joshwel <mark@joshwel.co>
  SPDX-License-Identifier: Unlicense OR 0BSD
"""

from __future__ import annotations

import logging
import time
from typing import Callable

import discord
from discord.ext import voice_recv

from .audio import is_probably_voice_frame
from .state import (
    AudioChunk,
    DISCORD_CHANNELS,
    DISCORD_SAMPLE_RATE,
    PCM_SAMPLE_WIDTH_BYTES,
    SINK_MIN_FLUSH_BYTES,
    SINK_BATCH_WINDOW_SECONDS,
    SINK_MAX_BUFFER_SECONDS,
    SINK_VOICE_HANGOVER_SECONDS,
)

log = logging.getLogger(__name__)

MAX_BUFFER_BYTES = int(DISCORD_SAMPLE_RATE * DISCORD_CHANNELS * PCM_SAMPLE_WIDTH_BYTES * SINK_MAX_BUFFER_SECONDS)


class SpeakiAudioSink(voice_recv.AudioSink):
    def __init__(
        self,
        guild_id: int,
        channel_id: int,
        route_chunk: Callable[[AudioChunk], None],
        on_speaker_idle: Callable[[int], None],
    ):
        super().__init__()
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.route_chunk = route_chunk
        self.on_speaker_idle = on_speaker_idle
        self.trimmed_buffers = 0
        self._last_drop_log_monotonic = 0.0
        self._speaker_buffers: dict[int, bytearray] = {}
        self._speaker_labels: dict[int, str] = {}
        self._speaker_last_flush_monotonic: dict[int, float] = {}
        self._speaker_last_drop_monotonic: dict[int, float] = {}
        self._speaker_voice_until_monotonic: dict[int, float] = {}
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
        voice_until = self._speaker_voice_until_monotonic.get(member.id, 0.0)
        include_pcm = is_probably_voice_frame(pcm)
        if include_pcm:
            self._speaker_voice_until_monotonic[member.id] = now + SINK_VOICE_HANGOVER_SECONDS
        elif now < voice_until:
            include_pcm = True
        elif not buffer:
            # Voice hangover expired and nothing to flush — speaker is now idle.
            was_active = member.id in self._speaker_voice_until_monotonic
            self._speaker_voice_until_monotonic.pop(member.id, None)
            if was_active:
                self.on_speaker_idle(member.id)
            return

        self._speaker_labels[member.id] = str(member)

        if include_pcm:
            buffer.extend(pcm)
            if len(buffer) > MAX_BUFFER_BYTES:
                del buffer[:-MAX_BUFFER_BYTES]
                self.trimmed_buffers += 1

        if not buffer:
            return

        last_flush = self._speaker_last_flush_monotonic.get(member.id, now)
        should_flush = not include_pcm
        if not should_flush:
            should_flush = (
                now - last_flush >= SINK_BATCH_WINDOW_SECONDS
                or len(buffer) >= SINK_MIN_FLUSH_BYTES
            )
        if not should_flush:
            return

        self._speaker_last_flush_monotonic[member.id] = now
        buffer = self._speaker_buffers.setdefault(member.id, bytearray())
        chunk = AudioChunk(
            guild_id=self.guild_id,
            user_id=member.id,
            user_label=str(member),
            pcm=bytes(buffer),
            received_monotonic=now,
        )
        buffer.clear()

        self.route_chunk(chunk)

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
            self.route_chunk(chunk)
            buffer.clear()
        return None

    def shutdown(self) -> None:
        self._closed = True
        self._speaker_buffers.clear()
        self._speaker_labels.clear()
        self._speaker_last_flush_monotonic.clear()
        self._speaker_last_drop_monotonic.clear()
        self._speaker_voice_until_monotonic.clear()
        self.route_chunk = lambda _chunk: None
        self.on_speaker_idle = lambda _user_id: None
