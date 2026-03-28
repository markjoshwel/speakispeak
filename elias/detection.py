"""
speakispeak: wake-word and prefix detection
  with all my heart, 2026, mark joshwel <mark@joshwel.co>
  SPDX-License-Identifier: Unlicense OR 0BSD
"""

from __future__ import annotations

import unicodedata
from typing import Final

from .wakewords import DELAYED_WAKE_WORDS, WAKE_WORDS


def normalise_text(text: str) -> str:
    cleaned: list[str] = []

    for char in text.casefold():
        if char.isspace():
            cleaned.append(" ")
            continue

        category = unicodedata.category(char)
        if category.startswith(("L", "N")):
            cleaned.append(char)
            continue

        cleaned.append(" ")

    return " ".join("".join(cleaned).split())


NORMALISED_WAKE_WORDS: Final[set[str]] = {
    normalise_text(variant)
    for variants in WAKE_WORDS.values()
    for variant in variants
    if normalise_text(variant)
}

COMPACT_WAKE_WORDS: Final[set[str]] = {
    normalised.replace(" ", "")
    for normalised in NORMALISED_WAKE_WORDS
    if normalised.replace(" ", "")
}

NORMALISED_DELAYED_WAKE_WORDS: Final[set[str]] = {
    normalise_text(variant)
    for variant in DELAYED_WAKE_WORDS
    if normalise_text(variant)
}

COMPACT_DELAYED_WAKE_WORDS: Final[set[str]] = {
    normalised.replace(" ", "")
    for normalised in NORMALISED_DELAYED_WAKE_WORDS
    if normalised.replace(" ", "")
}


def _iter_token_windows(tokens: list[str], *, max_window_size: int = 3, reverse: bool = False) -> list[str]:
    windows: list[str] = []
    start_indices = range(len(tokens) - 1, -1, -1) if reverse else range(len(tokens))
    size_indices = range(max_window_size, 0, -1) if reverse else range(1, max_window_size + 1)
    for start in start_indices:
        for size in size_indices:
            end = start + size
            if end > len(tokens):
                break
            windows.append(" ".join(tokens[start:end]))
    return windows


def detect_wakeword(text: str) -> str | None:
    normalised = normalise_text(text)
    if not normalised:
        return None

    if normalised in NORMALISED_WAKE_WORDS:
        return normalised

    compact = normalised.replace(" ", "")
    if compact in COMPACT_WAKE_WORDS:
        return normalised

    tokens = normalised.split()
    if not tokens:
        return None

    for candidate in _iter_token_windows(tokens, reverse=True):
        if candidate in NORMALISED_WAKE_WORDS:
            return candidate

        compact_candidate = candidate.replace(" ", "")
        if compact_candidate in COMPACT_WAKE_WORDS:
            return candidate

    return None


def should_delay_wakeword(trigger_text: str) -> bool:
    normalised = normalise_text(trigger_text)
    if not normalised:
        return False

    if normalised in NORMALISED_DELAYED_WAKE_WORDS:
        return True

    return normalised.replace(" ", "") in COMPACT_DELAYED_WAKE_WORDS


def format_recognised_log_window(text: str, trigger_text: str, *, radius: int = 4) -> str:
    normalised_text = normalise_text(text)
    normalised_trigger = normalise_text(trigger_text)
    if not normalised_text:
        return ""

    text_tokens = normalised_text.split()
    trigger_tokens = normalised_trigger.split()
    if not text_tokens or not trigger_tokens:
        return normalised_text

    trigger_len = len(trigger_tokens)
    match_start = -1
    for start in range(len(text_tokens) - trigger_len, -1, -1):
        if text_tokens[start : start + trigger_len] == trigger_tokens:
            match_start = start
            break

    if match_start < 0:
        compact_trigger = normalised_trigger.replace(" ", "")
        compact_text_tokens = [token.replace(" ", "") for token in text_tokens]
        for start in range(len(compact_text_tokens) - trigger_len, -1, -1):
            compact_window = "".join(compact_text_tokens[start : start + trigger_len])
            if compact_window == compact_trigger:
                match_start = start
                break

    if match_start < 0:
        if len(text_tokens) <= (radius * 2) + trigger_len:
            return normalised_text
        return "..." + " ".join(text_tokens[-((radius * 2) + trigger_len) :])

    window_start = max(0, match_start - radius)
    window_end = min(len(text_tokens), match_start + trigger_len + radius)
    window = " ".join(text_tokens[window_start:window_end])

    if window_start > 0:
        window = "..." + window
    if window_end < len(text_tokens):
        window = window + "..."
    return window
