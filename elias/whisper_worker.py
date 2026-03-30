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

log = logging.getLogger(__name__)


def _format_log(text: str, trigger: str | None = None) -> str:
    if trigger:
        return format_recognised_log_window(text, trigger)
    return text[:96] + "..." if len(text) > 96 else text


def _transcribe(model: object, pcm_16k: bytes, language_hint: str | None) -> str:
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
        )
        return " ".join(seg.text for seg in segments).strip()
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

    try:
        from faster_whisper import WhisperModel  # noqa: PLC0415
    except ImportError:
        log.error(
            "speaki: error: faster-whisper is not installed. "
            "Install it with: uv sync --extra whisper"
        )
        return

    log.info("speaki: info: [whisper-worker] loading model %r", model_name)
    model = WhisperModel(model_name, device="cpu", compute_type="int8")
    ready_event.set()  # type: ignore[attr-defined]

    # Use a language hint only when exactly one language is enabled; otherwise
    # let Whisper auto-detect so multilingual servers work without configuration.
    language_hint: str | None = enabled_languages[0] if len(enabled_languages) == 1 else None

    log.info(
        "speaki: info: whisper worker ready: model=%s language=%s",
        model_name,
        language_hint if language_hint is not None else "auto",
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

        text = _transcribe(model, message.pcm_16k_mono, language_hint)
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
