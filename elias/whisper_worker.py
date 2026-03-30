"""
speakispeak: OpenAI Whisper worker process
  with all my heart, 2026, mark joshwel <mark@joshwel.co>
  SPDX-License-Identifier: Unlicense OR 0BSD
"""

from __future__ import annotations

import logging
import signal
import time
import wave
from pathlib import Path
from queue import Empty

from .audio import convert_discord_pcm_to_vosk_pcm
from .detection import (
    detect_wakeword,
    format_recognised_log_window,
    should_delay_wakeword,
)
from .state import (
    AudioChunk,
    DISCORD_CHANNELS,
    DISCORD_SAMPLE_RATE,
    PCM_SAMPLE_WIDTH_BYTES,
    SPEAKER_TRIGGER_COOLDOWN_SECONDS,
    STRICT_DOUBLE_HIT_WINDOW_SECONDS,
    TARGET_SAMPLE_RATE,
    WHISPER_BUFFER_OVERLAP_SECONDS,
    WHISPER_MAX_BUFFER_SECONDS,
    WORKER_AUDIO_DIR,
    WORKER_POLL_TIMEOUT_SECONDS,
    Shutdown,
    SpeakerIdle,
    TriggerEvent,
)

log = logging.getLogger(__name__)

_PCM_FLOAT_SCALE: float = 32768.0
_PCM_BYTES_PER_SAMPLE: int = 2  # int16


class SpeakerState:
    def __init__(
        self,
        user_label: str,
        *,
        guild_id: int,
        user_id: int,
        dump_audio: bool,
        audio_dir: Path,
    ):
        self.user_label = user_label
        self.guild_id = guild_id
        self.user_id = user_id
        self.pcm_buffer = bytearray()
        self.last_inference_monotonic = 0.0
        self.last_audio_monotonic = 0.0
        self.last_trigger_text = ""
        self.last_trigger_monotonic = 0.0
        self.last_candidate_text = ""
        self.last_candidate_monotonic = 0.0
        self.candidate_hit_count = 0
        self.pending_trigger: TriggerEvent | None = None
        self.audio_sink: wave.Wave_write | None = None

        if dump_audio:
            safe_label = "".join(c if c.isalnum() else "_" for c in user_label).strip("_")
            if not safe_label:
                safe_label = "unknown_user"
            audio_dir.mkdir(parents=True, exist_ok=True)
            audio_path = audio_dir / f"guild_{guild_id}_{safe_label}.wav"
            wave_file = wave.open(str(audio_path), "wb")
            wave_file.setnchannels(DISCORD_CHANNELS)
            wave_file.setsampwidth(PCM_SAMPLE_WIDTH_BYTES)
            wave_file.setframerate(DISCORD_SAMPLE_RATE)
            self.audio_sink = wave_file

    def should_emit_trigger(self, text: str, now: float) -> bool:
        if text != self.last_trigger_text:
            return True
        return now - self.last_trigger_monotonic >= SPEAKER_TRIGGER_COOLDOWN_SECONDS

    def record_trigger(self, text: str, now: float) -> None:
        self.last_trigger_text = text
        self.last_trigger_monotonic = now
        self.last_candidate_text = ""
        self.last_candidate_monotonic = 0.0
        self.candidate_hit_count = 0

    def confirm_candidate(self, text: str, now: float, *, strict_double_hit: bool) -> bool:
        if not strict_double_hit:
            return True
        if (
            text == self.last_candidate_text
            and now - self.last_candidate_monotonic <= STRICT_DOUBLE_HIT_WINDOW_SECONDS
        ):
            self.candidate_hit_count += 1
        else:
            self.last_candidate_text = text
            self.candidate_hit_count = 1
        self.last_candidate_monotonic = now
        return self.candidate_hit_count >= 2

    def close(self) -> None:
        if self.audio_sink is not None:
            self.audio_sink.close()
            self.audio_sink = None


def _poll_message(input_queue: object) -> AudioChunk | SpeakerIdle | Shutdown | None:
    try:
        return input_queue.get(timeout=WORKER_POLL_TIMEOUT_SECONDS)  # type: ignore[attr-defined]
    except Empty:
        return None


def _format_recognition_log(recognised_text: str, trigger_text: str | None = None) -> str:
    if trigger_text:
        return format_recognised_log_window(recognised_text, trigger_text)
    if len(recognised_text) <= 96:
        return recognised_text
    return recognised_text[:93] + "..."


def _transcribe(model: object, pcm: bytes, language_hint: str | None) -> str:
    """Run Whisper transcription on 16 kHz mono int16 PCM bytes."""
    import numpy as np  # noqa: PLC0415 — imported here so module loads without openai-whisper

    if not pcm:
        return ""
    audio = np.frombuffer(pcm, dtype=np.int16).astype(np.float32) / _PCM_FLOAT_SCALE
    if not len(audio):
        return ""
    try:
        result = model.transcribe(  # type: ignore[attr-defined]
            audio,
            language=language_hint,
            fp16=False,
            task="transcribe",
            condition_on_previous_text=False,
        )
        text = result.get("text", "")
        return text.strip() if isinstance(text, str) else ""
    except Exception:
        log.exception("speaki: error: [whisper-worker] transcription raised")
        return ""


def _run_inference(
    speaker: SpeakerState,
    model: object,
    language_hint: str | None,
    output_queue: object,
    *,
    now: float,
    strict_double_hit: bool,
    worker_finish_wait_seconds: float,
    overlap_bytes: int,
    flush: bool = False,
) -> None:
    """Transcribe buffered audio and emit a TriggerEvent if a wakeword is found."""
    pcm = bytes(speaker.pcm_buffer)
    recognised_text = _transcribe(model, pcm, language_hint)
    speaker.last_inference_monotonic = now

    # Keep an overlap tail so a wakeword straddling two windows is still caught.
    if overlap_bytes > 0 and len(speaker.pcm_buffer) > overlap_bytes:
        del speaker.pcm_buffer[:-overlap_bytes]
    else:
        speaker.pcm_buffer.clear()

    if not recognised_text:
        return

    log.debug(
        "speaki: debug: [whisper-worker: %s] transcribed: %s",
        speaker.user_label,
        _format_recognition_log(recognised_text),
    )

    trigger_text = detect_wakeword(recognised_text)
    if trigger_text is None:
        return

    if not speaker.confirm_candidate(trigger_text, now, strict_double_hit=strict_double_hit):
        return

    if not speaker.should_emit_trigger(trigger_text, now):
        return

    speaker.record_trigger(trigger_text, now)
    # Clear any overlap after a confirmed trigger to avoid re-detection.
    speaker.pcm_buffer.clear()

    trigger_event = TriggerEvent(
        guild_id=speaker.guild_id,
        user_id=speaker.user_id,
        user_label=speaker.user_label,
        text=trigger_text,
        trigger_kind="wakeword",
        detected_monotonic=now,
        recognised_text=recognised_text,
    )

    delay_trigger = (not flush) and worker_finish_wait_seconds > 0 and should_delay_wakeword(trigger_text)
    if delay_trigger:
        log.info(
            "speaki: info: [whisper-worker: %s] wakeword: trigger=%s recognised=%s (waiting %.2fs for speech end)",
            speaker.user_label,
            trigger_text,
            _format_recognition_log(recognised_text, trigger_text),
            worker_finish_wait_seconds,
        )
        speaker.pending_trigger = trigger_event
    else:
        log.info(
            "speaki: info: [whisper-worker: %s] wakeword: trigger=%s recognised=%s",
            speaker.user_label,
            trigger_text,
            _format_recognition_log(recognised_text, trigger_text),
        )
        output_queue.put(trigger_event)  # type: ignore[attr-defined]


def worker_main(
    input_queue: object,
    output_queue: object,
    ready_event: object,
    shutdown_event: object,
    enabled_languages: tuple[str, ...],
    model_name: str,
    strict_double_hit: bool,
    debug: bool,
    dump_audio: bool,
    audio_dir_name: str,
    worker_finish_wait_seconds: float,
    inference_interval_seconds: float,
    min_buffer_seconds: float,
) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )

    # Import here so the module can be imported without whisper installed
    # (only the subprocess that runs worker_main actually needs it).
    try:
        import whisper as openai_whisper
    except ImportError:
        log.error(
            "speaki: error: openai-whisper is not installed. "
            "Install it with: uv add openai-whisper"
        )
        return

    log.info("speaki: info: [whisper-worker] loading model %r (this may take a moment)", model_name)
    model = openai_whisper.load_model(model_name)
    ready_event.set()  # type: ignore[attr-defined]

    # Use a language hint when exactly one language is enabled, auto-detect otherwise.
    language_hint: str | None = enabled_languages[0] if len(enabled_languages) == 1 else None

    min_buffer_bytes = int(TARGET_SAMPLE_RATE * _PCM_BYTES_PER_SAMPLE * min_buffer_seconds)
    max_buffer_bytes = int(TARGET_SAMPLE_RATE * _PCM_BYTES_PER_SAMPLE * WHISPER_MAX_BUFFER_SECONDS)
    overlap_bytes = int(TARGET_SAMPLE_RATE * _PCM_BYTES_PER_SAMPLE * WHISPER_BUFFER_OVERLAP_SECONDS)

    speakers: dict[int, SpeakerState] = {}
    audio_dir = WORKER_AUDIO_DIR / audio_dir_name

    log.info(
        "speaki: info: whisper worker starting: model=%s; language-hint=%s; strict-double-hit=%s; "
        "inference-interval=%.2fs; min-buffer=%.2fs",
        model_name,
        language_hint if language_hint is not None else "auto",
        strict_double_hit,
        inference_interval_seconds,
        min_buffer_seconds,
    )

    try:
        while True:
            if shutdown_event.is_set():  # type: ignore[attr-defined]
                log.info("speaki: info: [whisper-worker] shutting down (shutdown event set)")
                return

            message = _poll_message(input_queue)
            now = time.monotonic()

            # Flush delayed triggers whose speech-end wait has expired.
            if worker_finish_wait_seconds > 0:
                for speaker in speakers.values():
                    if speaker.pending_trigger is None:
                        continue
                    if now - speaker.last_audio_monotonic < worker_finish_wait_seconds:
                        continue
                    output_queue.put(speaker.pending_trigger)  # type: ignore[attr-defined]
                    speaker.pending_trigger = None

            # Run inference for speakers with enough buffered audio.
            # NOTE: model.transcribe() blocks for the duration of inference (CPU: ~1–3 s for
            # the base model). Only one speaker is inferred per loop iteration to keep the
            # message queue responsive; the next speaker will be picked up on the next poll.
            for user_id, speaker in list(speakers.items()):
                if len(speaker.pcm_buffer) < min_buffer_bytes:
                    continue
                if now - speaker.last_inference_monotonic < inference_interval_seconds:
                    continue
                _run_inference(
                    speaker, model, language_hint, output_queue,
                    now=now,
                    strict_double_hit=strict_double_hit,
                    worker_finish_wait_seconds=worker_finish_wait_seconds,
                    overlap_bytes=overlap_bytes,
                )
                break  # one inference per poll cycle; revisit others next iteration

            if message is None:
                continue

            if isinstance(message, Shutdown):
                log.info("speaki: info: [whisper-worker] shutting down (%s)", message.reason)
                return

            if isinstance(message, SpeakerIdle):
                speaker = speakers.pop(message.user_id, None)
                if speaker is not None:
                    # Final inference: use half threshold so short tail audio is transcribed.
                    if len(speaker.pcm_buffer) >= min_buffer_bytes // 2:
                        _run_inference(
                            speaker, model, language_hint, output_queue,
                            now=time.monotonic(),
                            strict_double_hit=strict_double_hit,
                            worker_finish_wait_seconds=worker_finish_wait_seconds,
                            overlap_bytes=0,
                            flush=True,
                        )
                    if speaker.pending_trigger is not None:
                        output_queue.put(speaker.pending_trigger)  # type: ignore[attr-defined]
                        speaker.pending_trigger = None
                    speaker.close()
                    log.debug(
                        "speaki: debug: [whisper-worker] released speaker slot for user %s",
                        message.user_id,
                    )
                continue

            # AudioChunk
            speaker = speakers.get(message.user_id)
            if speaker is None:
                speaker = SpeakerState(
                    message.user_label,
                    guild_id=message.guild_id,
                    user_id=message.user_id,
                    dump_audio=dump_audio,
                    audio_dir=audio_dir,
                )
                speakers[message.user_id] = speaker
                log.info(
                    "speaki: info: [whisper-worker: %s] tracking audio stream",
                    message.user_label,
                )
                if dump_audio:
                    log.info(
                        "speaki: info: [whisper-worker: %s] writing decoded audio to %s",
                        message.user_label,
                        audio_dir,
                    )

            pcm_48k_stereo = message.pcm
            if speaker.audio_sink is not None:
                speaker.audio_sink.writeframes(pcm_48k_stereo)

            pcm = convert_discord_pcm_to_vosk_pcm(pcm_48k_stereo)
            if not pcm:
                continue

            speaker.last_audio_monotonic = message.received_monotonic
            speaker.pcm_buffer.extend(pcm)

            if len(speaker.pcm_buffer) > max_buffer_bytes:
                del speaker.pcm_buffer[:-max_buffer_bytes]

    finally:
        for speaker in speakers.values():
            if speaker.pending_trigger is not None:
                output_queue.put(speaker.pending_trigger)  # type: ignore[attr-defined]
                speaker.pending_trigger = None
            speaker.close()
