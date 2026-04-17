"""
speakispeak: faster-whisper worker process (stateless)
  with all my heart, 2026, mark joshwel <mark@joshwel.co>
  SPDX-License-Identifier: Unlicense OR 0BSD

Each worker in the pool is stateless: it receives a fully-formed WhisperJob
(already converted to 16 kHz mono), transcribes it, and emits a TriggerEvent if
a wakeword is found.  Per-speaker buffering, overlap, and trigger cooldown are
all handled by WhisperSpeakerRouter in session.py before jobs are enqueued.
"""

from __future__ import annotations

import logging
import signal
import time
from queue import Empty, Full

from .detection import detect_wakeword, format_recognised_log_window
from .state import (
    Shutdown,
    TriggerEvent,
    WHISPER_JOB_MAX_AGE_SECONDS,
    WORKER_POLL_TIMEOUT_SECONDS,
    WhisperJob,
)
from .wakewords import WAKE_WORDS as _WAKE_WORDS


def _build_initial_prompt(enabled_languages: tuple[str, ...]) -> str | None:
    """Build an initial_prompt string to bias Whisper's decoder toward wakeword tokens.

    Follows the pattern recommended for rare/nonstandard word recognition:
      - List each variant with period separators (not commas — stronger token bias)
      - Repeat the primary variant for extra weight
      - Include multilingual variants when non-English languages are enabled
      - End with a clarifying sentence using the primary form
    Prompt is capped at 900 chars; Whisper only uses the last ~224 tokens anyway.
    """
    ascii_variants: list[str] = []
    unicode_variants: list[str] = []
    seen: set[str] = set()

    # "speaki" is the canonical primary wakeword; put it first so it's most weighted
    _PRIMARY = "speaki"

    # Collect in language order so English comes first when en is enabled.
    # Insert primary first if it exists in the enabled wakewords.
    for lang in enabled_languages:
        variants = _WAKE_WORDS.get(lang, set())
        if _PRIMARY in variants and _PRIMARY not in seen:
            seen.add(_PRIMARY)
            ascii_variants.append(_PRIMARY)
        for variant in sorted(variants):
            if variant in seen:
                continue
            seen.add(variant)
            if variant.isascii() and variant.replace(" ", "").isalpha():
                ascii_variants.append(variant)
            elif not variant.isascii():
                unicode_variants.append(variant)

    if not ascii_variants and not unicode_variants:
        return None

    # Primary variant: speaki if present, else first ascii, else first unicode
    primary = ascii_variants[0] if ascii_variants else unicode_variants[0]

    # Build period-separated list; repeat primary once for emphasis
    parts = [v.capitalize() for v in ascii_variants]
    if primary.isascii():
        parts.append(primary.capitalize())   # repeat for weight
    parts.extend(unicode_variants)
    parts.append(f"The wakeword is {primary.capitalize()}.")

    prompt = " ".join(parts)
    return prompt[:900]

log = logging.getLogger(__name__)


def _format_log(text: str, trigger: str | None = None) -> str:
    if trigger:
        return format_recognised_log_window(text, trigger)
    return text[:96] + "..." if len(text) > 96 else text


def _transcribe(model: object, pcm_16k: bytes, language_hint: str | None, initial_prompt: str | None) -> str:
    """Transcribe 16 kHz mono int16 PCM with faster-whisper."""
    import numpy as np  # noqa: PLC0415 — lazy import; only runs inside the worker subprocess

    audio = np.frombuffer(pcm_16k, dtype=np.int16).astype(np.float32) / 32768.0
    if not len(audio):
        return ""
    try:
        segments, _ = model.transcribe(  # type: ignore[attr-defined]
            audio,
            language=language_hint,
            beam_size=1,
            best_of=1,
            temperature=0.0,
            vad_filter=True,  # Silero VAD strips non-speech before main inference
            initial_prompt=initial_prompt,  # biases decoder toward wakeword tokens
        )
        # Filter out low-confidence segments — Whisper tiny/base will return garbage
        # text when audio is corrupted or too short.  avg_logprob is log-probability
        # of the segment; below -0.8 is strongly hallucinated.  no_speech_prob > 0.5
        # means the model itself thinks it heard silence.
        return " ".join(
            seg.text
            for seg in segments
            if seg.avg_logprob >= -0.8 and seg.no_speech_prob <= 0.5
        ).strip()
    except Exception:
        log.exception("speaki: error: [whisper-worker] transcription raised")
        return ""


def worker_main(
    input_queue: object,
    output_queue: object,
    ready_event: object,
    shutdown_event: object,
    enabled_languages: tuple[str, ...],
    model_name: str,
    debug: bool,
) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )

    from faster_whisper import WhisperModel  # noqa: PLC0415

    log.info("speaki: info: [whisper-worker] loading model %r", model_name)
    model = WhisperModel(model_name, device="cpu", compute_type="int8")
    ready_event.set()  # type: ignore[attr-defined]

    # Use a language hint only when exactly one language is enabled; otherwise
    # let Whisper auto-detect so multilingual servers work without configuration.
    language_hint: str | None = enabled_languages[0] if len(enabled_languages) == 1 else None
    initial_prompt = _build_initial_prompt(enabled_languages)

    log.info(
        "speaki: info: whisper worker ready: model=%s language=%s prompt=%r",
        model_name,
        language_hint if language_hint is not None else "auto",
        initial_prompt,
    )

    while True:
        if shutdown_event.is_set():  # type: ignore[attr-defined]
            return

        try:
            message = input_queue.get(timeout=WORKER_POLL_TIMEOUT_SECONDS)  # type: ignore[attr-defined]
        except Empty:
            continue

        if isinstance(message, Shutdown):
            return

        if not isinstance(message, WhisperJob):
            continue

        now = time.monotonic()
        age = now - message.enqueued_at
        if age > WHISPER_JOB_MAX_AGE_SECONDS:
            if debug:
                log.debug(
                    "speaki: debug: [whisper-worker] dropped stale job for %s (age=%.2fs)",
                    message.user_label,
                    age,
                )
            continue

        text = _transcribe(model, message.pcm_16k_mono, language_hint, initial_prompt)
        if not text:
            continue

        if debug:
            log.debug(
                "speaki: debug: [whisper-worker: %s] transcribed: %s",
                message.user_label,
                _format_log(text),
            )

        trigger_text = detect_wakeword(text)
        if trigger_text is None:
            continue

        log.info(
            "speaki: info: [whisper-worker: %s] wakeword: trigger=%s recognised=%s",
            message.user_label,
            trigger_text,
            _format_log(text, trigger_text),
        )
        try:
            output_queue.put_nowait(  # type: ignore[attr-defined]
                TriggerEvent(
                    guild_id=message.guild_id,
                    user_id=message.user_id,
                    user_label=message.user_label,
                    text=trigger_text,
                    trigger_kind="wakeword",
                    detected_monotonic=now,
                    recognised_text=text,
                )
            )
        except Full:
            pass
