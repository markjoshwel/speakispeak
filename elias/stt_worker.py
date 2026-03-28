"""
speakispeak: Vosk worker process
  with all my heart, 2026, mark joshwel <mark@joshwel.co>
  SPDX-License-Identifier: Unlicense OR 0BSD
"""

from __future__ import annotations

import json
import logging
import signal
import time
import wave
from pathlib import Path
from queue import Empty

import discord
from vosk import KaldiRecognizer, Model, SetLogLevel

from .audio import convert_discord_pcm_to_vosk_pcm
from .detection import (
    detect_wakeword,
    format_recognised_log_window,
    get_language_wakeword_grammar,
    should_delay_wakeword,
)
from .state import (
    AudioChunk,
    DISCORD_CHANNELS,
    DISCORD_SAMPLE_RATE,
    MODELS_DIR,
    PCM_SAMPLE_WIDTH_BYTES,
    SPEAKER_TRIGGER_COOLDOWN_SECONDS,
    STRICT_DOUBLE_HIT_WINDOW_SECONDS,
    TARGET_SAMPLE_RATE,
    WORKER_AUDIO_DIR,
    WORKER_POLL_TIMEOUT_SECONDS,
    Shutdown,
    TriggerEvent,
)

log = logging.getLogger(__name__)

MODEL_PREFIXES: dict[str, str] = {
    "en": "vosk-model-small-en-",
    "ko": "vosk-model-small-ko-",
    "ja": "vosk-model-small-ja-",
}


def _format_recognition_log(recognised_text: str, trigger_text: str | None = None) -> str:
    if trigger_text:
        return format_recognised_log_window(recognised_text, trigger_text)
    if len(recognised_text) <= 96:
        return recognised_text
    return recognised_text[:93] + "..."


class SpeakerState:
    def __init__(
        self,
        models: dict[str, Model],
        user_label: str,
        *,
        guild_id: int,
        dump_audio: bool,
        audio_dir: Path,
        use_grammar: bool,
    ):
        self.user_label = user_label
        self.recognisers: dict[str, KaldiRecognizer] = {}
        for language, model in models.items():
            if use_grammar:
                recogniser = KaldiRecognizer(
                    model,
                    TARGET_SAMPLE_RATE,
                    get_language_wakeword_grammar(language),
                )
            else:
                recogniser = KaldiRecognizer(model, TARGET_SAMPLE_RATE)
            recogniser.SetWords(False)
            recogniser.SetPartialWords(False)
            self.recognisers[language] = recogniser

        self.last_partial_texts: dict[str, str] = {}
        self.last_logged_finals: dict[str, tuple[str, float]] = {}
        self.last_trigger_text = ""
        self.last_trigger_monotonic = 0.0
        self.last_candidate_text = ""
        self.last_candidate_monotonic = 0.0
        self.candidate_hit_count = 0
        self.last_audio_monotonic = 0.0
        self.audio_sink: wave.Wave_write | None = None
        self.pending_trigger: TriggerEvent | None = None

        if dump_audio:
            safe_label = "".join(character if character.isalnum() else "_" for character in user_label).strip("_")
            if not safe_label:
                safe_label = "unknown_user"

            audio_dir.mkdir(parents=True, exist_ok=True)
            audio_path = audio_dir.joinpath(f"guild_{guild_id}_{safe_label}.wav")
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

    def close(self) -> None:
        if self.audio_sink is not None:
            self.audio_sink.close()
            self.audio_sink = None

    def should_log_final_text(self, language: str, recognised_text: str, now: float) -> bool:
        previous = self.last_logged_finals.get(language)
        if previous is None:
            self.last_logged_finals[language] = (recognised_text, now)
            return True

        previous_text, previous_monotonic = previous
        if recognised_text != previous_text or now - previous_monotonic >= 5.0:
            self.last_logged_finals[language] = (recognised_text, now)
            return True

        return False

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


def _find_model_dir(prefix: str) -> Path:
    candidates = sorted(path for path in MODELS_DIR.iterdir() if path.is_dir() and path.name.startswith(prefix))
    if not candidates:
        raise FileNotFoundError(f"Missing model directory for prefix {prefix!r}")
    return candidates[-1]


def _load_models(enabled_languages: tuple[str, ...]) -> dict[str, Model]:
    loaded: dict[str, Model] = {}
    for language in enabled_languages:
        prefix = MODEL_PREFIXES[language]
        loaded[language] = Model(str(_find_model_dir(prefix)))
    return loaded


def _extract_text(payload: str, key: str) -> str:
    if not payload:
        return ""

    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return ""

    value = data.get(key, "")
    return value if isinstance(value, str) else ""


def _poll_message(input_queue: object) -> AudioChunk | Shutdown | None:
    try:
        return input_queue.get(timeout=WORKER_POLL_TIMEOUT_SECONDS)  # type: ignore[attr-defined]
    except Empty:
        return None


def worker_main(
    input_queue: object,
    output_queue: object,
    ready_event: object,
    shutdown_event: object,
    enabled_languages: tuple[str, ...],
    use_grammar: bool,
    strict_final_only: bool,
    strict_double_hit: bool,
    debug: bool,
    dump_audio: bool,
    audio_dir_name: str,
    worker_finish_wait_seconds: float,
) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    SetLogLevel(-1)
    if not discord.opus.is_loaded():
        discord.opus._load_default()
    models = _load_models(enabled_languages)
    ready_event.set()  # type: ignore[attr-defined]
    speakers: dict[int, SpeakerState] = {}
    audio_dir = WORKER_AUDIO_DIR.joinpath(audio_dir_name)

    log.info(
        "speaki: info: worker starting with languages loaded: %s; grammar=%s; strict-final-only=%s; strict-double-hit=%s",
        ", ".join(enabled_languages),
        use_grammar,
        strict_final_only,
        strict_double_hit,
    )

    try:
        while True:
            if shutdown_event.is_set():  # type: ignore[attr-defined]
                log.info("speaki: info: worker shutting down (shutdown event set)")
                return

            message = _poll_message(input_queue)
            current_monotonic = time.monotonic()

            if worker_finish_wait_seconds > 0:
                for speaker in speakers.values():
                    pending_trigger = speaker.pending_trigger
                    if pending_trigger is None:
                        continue

                    if current_monotonic - speaker.last_audio_monotonic < worker_finish_wait_seconds:
                        continue

                    output_queue.put(pending_trigger)
                    speaker.pending_trigger = None

            if message is None:
                continue

            if isinstance(message, Shutdown):
                log.info("speaki: info: worker shutting down (%s)", message.reason)
                return

            speaker = speakers.get(message.user_id)
            if speaker is None:
                speaker = SpeakerState(
                    models,
                    message.user_label,
                    guild_id=message.guild_id,
                    dump_audio=dump_audio,
                    audio_dir=audio_dir,
                    use_grammar=use_grammar,
                )
                speakers[message.user_id] = speaker
                log.info("speaki: info: [worker: %s] tracking audio stream", message.user_label)
                if dump_audio:
                    log.info(
                        "speaki: info: [worker: %s] writing decoded audio to %s",
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
            now = time.monotonic()

            for language, recogniser in speaker.recognisers.items():
                is_final = bool(recogniser.AcceptWaveform(pcm))
                payload = recogniser.Result() if is_final else recogniser.PartialResult()
                key = "text" if is_final else "partial"
                recognised_text = _extract_text(payload, key)
                if not recognised_text:
                    continue

                if not is_final and recognised_text == speaker.last_partial_texts.get(language):
                    continue

                if not is_final:
                    speaker.last_partial_texts[language] = recognised_text

                result_kind = "final" if is_final else "partial"
                if is_final:
                    if speaker.should_log_final_text(language, recognised_text, now):
                        log.info(
                            "speaki: info: [worker: %s] recognised %s word from audio stream (lang=%s): %s",
                            speaker.user_label,
                            result_kind,
                            language,
                            _format_recognition_log(recognised_text),
                        )
                else:
                    log.debug(
                        "speaki: debug: [worker: %s] recognised %s word from audio stream (lang=%s): %s",
                        speaker.user_label,
                        result_kind,
                        language,
                        _format_recognition_log(recognised_text),
                    )

                trigger_text = detect_wakeword(recognised_text)
                if trigger_text is None:
                    continue

                if strict_final_only and not is_final:
                    continue

                if not speaker.confirm_candidate(trigger_text, now, strict_double_hit=strict_double_hit):
                    continue

                if not speaker.should_emit_trigger(trigger_text, now):
                    continue

                speaker.record_trigger(trigger_text, now)
                trigger_event = TriggerEvent(
                    guild_id=message.guild_id,
                    user_id=message.user_id,
                    user_label=speaker.user_label,
                    text=trigger_text,
                    trigger_kind="wakeword",
                    detected_monotonic=now,
                    recognised_text=recognised_text,
                )
                delay_trigger = worker_finish_wait_seconds > 0 and should_delay_wakeword(trigger_text)
                if delay_trigger:
                    log.info(
                        "speaki: info: [worker: %s] recognised wakeword from audio stream: trigger=%s recognised=%s (waiting %.2fs for speech end)",
                        speaker.user_label,
                        trigger_text,
                        _format_recognition_log(recognised_text, trigger_text),
                        worker_finish_wait_seconds,
                    )
                else:
                    log.info(
                        "speaki: info: [worker: %s] recognised wakeword from audio stream: trigger=%s recognised=%s",
                        speaker.user_label,
                        trigger_text,
                        _format_recognition_log(recognised_text, trigger_text),
                    )
                if delay_trigger:
                    speaker.pending_trigger = trigger_event
                else:
                    output_queue.put(trigger_event)
    finally:
        for speaker in speakers.values():
            if speaker.pending_trigger is not None:
                output_queue.put(speaker.pending_trigger)
                speaker.pending_trigger = None
            speaker.close()
