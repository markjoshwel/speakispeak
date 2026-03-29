"""
speakispeak: libopus runtime loading helpers
  with all my heart, 2026, mark joshwel <mark@joshwel.co>
  SPDX-License-Identifier: Unlicense OR 0BSD
"""

from __future__ import annotations

import ctypes.util
import logging
import os
from pathlib import Path

import discord

log = logging.getLogger(__name__)

_OPUS_ENV_KEYS = (
    "SPEAKI_OPUS_LIB",
    "OPUS_LIBRARY",
)
_OPUS_CANDIDATE_PATHS = (
    "/opt/homebrew/lib/libopus.dylib",
    "/opt/homebrew/opt/opus/lib/libopus.dylib",
    "/usr/local/lib/libopus.dylib",
    "/usr/local/opt/opus/lib/libopus.dylib",
    "/opt/local/lib/libopus.dylib",
    "/run/current-system/sw/lib/libopus.so",
    "/run/current-system/sw/lib/libopus.so.0",
)


def ensure_opus_loaded() -> bool:
    if discord.opus.is_loaded():
        return True

    for candidate in _opus_candidates():
        try:
            discord.opus.load_opus(candidate)
        except OSError as exc:
            log.debug("speaki: debug: failed loading opus candidate %s: %s", candidate, exc)
            continue

        if discord.opus.is_loaded():
            log.info("speaki: info: loaded opus library from %s", candidate)
            return True

    return False


def _opus_candidates() -> tuple[str, ...]:
    candidates: list[str] = []

    for key in _OPUS_ENV_KEYS:
        value = os.environ.get(key, "").strip()
        if value:
            candidates.append(value)

    found_library = ctypes.util.find_library("opus")
    if found_library:
        candidates.append(found_library)

    for path_text in _OPUS_CANDIDATE_PATHS:
        if Path(path_text).exists():
            candidates.append(path_text)

    deduped: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        deduped.append(candidate)

    return tuple(deduped)
