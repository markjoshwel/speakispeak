"""
speakispeak: local patch for discord-ext-voice-recv decode resilience
  with all my heart, 2026, mark joshwel <mark@joshwel.co>
  SPDX-License-Identifier: Unlicense OR 0BSD
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import discord
from discord.ext.voice_recv.opus import PacketDecoder

if TYPE_CHECKING:
    from discord.ext.voice_recv.rtp import AudioPacket

log = logging.getLogger(__name__)

_PATCHED = False
_ORIGINAL_DECODE_PACKET = PacketDecoder._decode_packet


def apply_voice_recv_patch() -> None:
    global _PATCHED
    if _PATCHED:
        return

    def patched_decode_packet(self: PacketDecoder, packet: AudioPacket) -> tuple[AudioPacket, bytes]:
        assert self._decoder is not None

        try:
            return _ORIGINAL_DECODE_PACKET(self, packet)
        except discord.opus.OpusError:
            log.debug("speaki: debug: concealing corrupted opus packet for ssrc %s", self.ssrc)
            try:
                pcm = self._decoder.decode(None, fec=False)
            except discord.opus.OpusError:
                pcm = b""
            return packet, pcm

    PacketDecoder._decode_packet = patched_decode_packet
    _PATCHED = True
