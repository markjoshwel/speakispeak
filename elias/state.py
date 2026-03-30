"""
speakispeak: shared runtime state types
  with all my heart, 2026, mark joshwel <mark@joshwel.co>
  SPDX-License-Identifier: Unlicense OR 0BSD
"""

from __future__ import annotations

from pathlib import Path
from typing import Final, NamedTuple

ROOT_DIR: Final[Path] = Path(__file__).resolve().parent.parent
MODELS_DIR: Final[Path] = ROOT_DIR.joinpath("models")
SOUNDS_DIR: Final[Path] = ROOT_DIR.joinpath("sounds")
WORKER_AUDIO_DIR: Final[Path] = ROOT_DIR.joinpath("worker_audio")

JANITOR_INTERVAL_SECONDS: Final[float] = 15.0
PLAYBACK_COOLDOWN_SECONDS: Final[float] = 1.2
SPEAKER_TRIGGER_COOLDOWN_SECONDS: Final[float] = 2.0
WORKER_POOL_SIZE: Final[int] = 8
WORKER_QUEUE_MAXSIZE: Final[int] = 2048
WORKER_POLL_TIMEOUT_SECONDS: Final[float] = 0.5
WORKER_STARTUP_TIMEOUT_SECONDS: Final[float] = 15.0
VOICE_CONNECT_TIMEOUT_SECONDS: Final[float] = 8.0
VOICE_DISCONNECT_TIMEOUT_SECONDS: Final[float] = 5.0
VOICE_HEALTH_POLL_INTERVAL_SECONDS: Final[float] = 1.0
VOICE_HEALTH_RECOVERY_GRACE_SECONDS: Final[float] = 3.0
VOICE_LISTENER_RECOVERY_GRACE_SECONDS: Final[float] = 2.0
VOICE_ERROR_BURST_RECOVERY_GRACE_SECONDS: Final[float] = 2.0
VOICE_RECOVERY_MIN_INTERVAL_SECONDS: Final[float] = 3.0
VOICE_SOFT_RECONNECT_LIMIT: Final[int] = 2
VOICE_RECONNECT_WINDOW_SECONDS: Final[float] = 20.0
VOICE_HARD_RESET_RETRY_DELAY_SECONDS: Final[float] = 10.0
VOICE_POST_RECONNECT_UNSTABLE_SECONDS: Final[float] = 5.0
VOICE_POST_RECONNECT_DECRYPT_RESET_THRESHOLD: Final[int] = 2
VOICE_POST_RECONNECT_OPUS_RESET_THRESHOLD: Final[int] = 8
VOICE_SELF_DISCONNECT_CONFIRMATION_SECONDS: Final[float] = 2.5
SINK_BATCH_WINDOW_SECONDS: Final[float] = 0.5
SINK_MAX_BUFFER_SECONDS: Final[float] = 1.0
SINK_MIN_FLUSH_BYTES: Final[int] = 3840 * 10
SINK_VOICE_HANGOVER_SECONDS: Final[float] = 0.35
SINK_VOICE_AVERAGE_ABS_THRESHOLD: Final[int] = 120
SINK_VOICE_PEAK_THRESHOLD: Final[int] = 600

DISCORD_SAMPLE_RATE: Final[int] = 48_000
TARGET_SAMPLE_RATE: Final[int] = 16_000
DISCORD_CHANNELS: Final[int] = 2
TARGET_CHANNELS: Final[int] = 1
PCM_SAMPLE_WIDTH_BYTES: Final[int] = 2

WHISPER_MODEL_NAME: Final[str] = "tiny"
WHISPER_INFERENCE_INTERVAL_SECONDS: Final[float] = 1.0
WHISPER_MIN_BUFFER_SECONDS: Final[float] = 1.5
WHISPER_MAX_BUFFER_SECONDS: Final[float] = 30.0
WHISPER_BUFFER_OVERLAP_SECONDS: Final[float] = 0.5

JAPANESE_SOUNDS_DIRNAME: Final[str] = "ﾆﾎﾝｽﾋﾟｷ"
GENERAL_SOUNDS_DIRNAME: Final[str] = "一般的ｽﾋﾟｷ"
TRIGGER_TEXT: Final[str] = "speaki"
DEFAULT_WAIT_UNTIL_VOICE_FINISHED_SECONDS: Final[float] = 1.0
DEFAULT_VC_TIMEOUT_SECONDS: Final[float] = 600.0
STRICT_DOUBLE_HIT_WINDOW_SECONDS: Final[float] = 2.0


class AudioChunk(NamedTuple):
    guild_id: int
    user_id: int
    user_label: str
    pcm: bytes
    received_monotonic: float


class SpeakerIdle(NamedTuple):
    user_id: int


class Shutdown(NamedTuple):
    reason: str = "shutdown"


class TriggerEvent(NamedTuple):
    guild_id: int
    user_id: int
    user_label: str
    text: str
    trigger_kind: str
    detected_monotonic: float
    recognised_text: str


class WorkerStats(NamedTuple):
    dropped_chunks: int
    active_speakers: int
