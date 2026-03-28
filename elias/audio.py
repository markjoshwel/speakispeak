"""
speakispeak: PCM conversion helpers
  with all my heart, 2026, mark joshwel <mark@joshwel.co>
  SPDX-License-Identifier: Unlicense OR 0BSD
"""

from __future__ import annotations

from array import array

from .state import DISCORD_CHANNELS


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

