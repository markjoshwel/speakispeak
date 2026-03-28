"""
speakispeak: random sound selection
  with all my heart, 2026, mark joshwel <mark@joshwel.co>
  SPDX-License-Identifier: Unlicense OR 0BSD
"""

from __future__ import annotations

import random
from functools import lru_cache
from pathlib import Path

from .state import GENERAL_SOUNDS_DIRNAME, JAPANESE_SOUNDS_DIRNAME, SOUNDS_DIR


@lru_cache(maxsize=1)
def _sound_bank() -> tuple[tuple[Path, ...], tuple[Path, ...]]:
    japanese = tuple(sorted(SOUNDS_DIR.joinpath(JAPANESE_SOUNDS_DIRNAME).glob("*.mp3")))
    general = tuple(sorted(SOUNDS_DIR.joinpath(GENERAL_SOUNDS_DIRNAME).glob("*.mp3")))
    return (japanese, general)


def pick_random_sound() -> Path:
    japanese, general = _sound_bank()
    if not japanese and not general:
        raise FileNotFoundError("No mp3 files found in sounds/")

    roll = random.random()
    pool = japanese if roll < 0.05 and japanese else general
    if not pool:
        pool = japanese

    if not pool:
        raise FileNotFoundError("No mp3 files found in sounds/")

    return random.choice(pool)


def describe_sound(sound_path: Path) -> str:
    return f"{sound_path.parent.name}: {sound_path.stem}"
