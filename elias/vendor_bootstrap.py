"""
speakispeak: bootstrap vendored discord-ext-voice-recv fork
  with all my heart, 2026, mark joshwel <mark@joshwel.co>
  SPDX-License-Identifier: Unlicense OR 0BSD
"""

from __future__ import annotations

from pathlib import Path

import discord
import discord.ext

from .state import ROOT_DIR


def bootstrap_voice_recv_vendor() -> Path:
    ext_dir = ROOT_DIR.joinpath("vendor", "discord-ext-voice-recv", "discord", "ext")
    ext_dir_text = str(ext_dir)
    current_paths = list(discord.ext.__path__)
    if ext_dir_text not in current_paths:
        discord.ext.__path__ = [ext_dir_text, *current_paths]

    return ext_dir
