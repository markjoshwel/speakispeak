"""
speakispeak: PCM conversion helpers
  with all my heart, 2026, mark joshwel <mark@joshwel.co>
  SPDX-License-Identifier: Unlicense OR 0BSD
"""

from __future__ import annotations

from array import array

from .state import (
    DISCORD_CHANNELS,
    SINK_VOICE_AVERAGE_ABS_THRESHOLD,
    SINK_VOICE_PEAK_THRESHOLD,
)


def convert_discord_pcm_to_vosk_pcm(pcm: bytes) -> bytes:
    """Convert Discord 48 kHz stereo PCM into simple 16 kHz mono PCM."""
    if not pcm:
        return b""

    samples = array("h")
    samples.frombytes(pcm[: len(pcm) - (len(pcm) % 4)])
    if not samples:
        return b""

    mono = array("h")
    frame_count = len(samples) // DISCORD_CHANNELS
    mono.extend((samples[frame * 2] + samples[frame * 2 + 1]) // 2 for frame in range(frame_count))
    downsampled = array("h", mono[::3])
    return downsampled.tobytes()


def is_probably_voice_frame(pcm: bytes) -> bool:
    """Cheap gate for obvious silence before work crosses process boundaries."""
    if not pcm:
        return False

    samples = array("h")
    samples.frombytes(pcm[: len(pcm) - (len(pcm) % 2)])
    if not samples:
        return False

    step = max(1, len(samples) // 64)
    total_abs = 0
    peak_abs = 0
    count = 0

    for index in range(0, len(samples), step):
        value = abs(samples[index])
        total_abs += value
        peak_abs = max(peak_abs, value)
        count += 1

    average_abs = total_abs / count if count else 0
    return (
        average_abs >= SINK_VOICE_AVERAGE_ABS_THRESHOLD
        or peak_abs >= SINK_VOICE_PEAK_THRESHOLD
    )
