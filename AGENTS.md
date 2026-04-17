# speakispeak — agent orientation

Discord wakeword bot. Joins a VC when someone types `speaki`, listens with Vosk STT, fires an SFX when it hears the wakeword. Python 3.13+, run with `uv run main.py`.

## Entry point & commands

`main.py` — `SpeakiClient(discord.Client)`, `RuntimeConfig` (live-reloads `config.toml`).
- `speaki` — joins sender's VC, plays SFX
- `speaki stop` — kicks bot immediately (sender must be in VC, or be admin)
- `speaki config [key = val]` — show/update live config (admin only)

## Core modules (`elias/`)

| File | Role |
|---|---|
| `session.py` | `SpeakiSession` — per-guild state, worker lifecycle, voice health monitor & recovery, 60 s empty-channel heartbeat |
| `sink.py` | `SpeakiAudioSink` — voice receive callback, silence gate, PCM batching, fan-out to all language worker queues |
| `stt_worker.py` | `worker_main` subprocess — Vosk recognition loop, emits `TriggerEvent` + `TranscriptionEvent` to output queue |
| `audio.py` | PCM conversion 48 kHz stereo → 16 kHz mono, silence gate |
| `detection.py` | Wakeword detection, normalisation, double-hit / delay logic |
| `state.py` | All constants and `NamedTuple` message types: `AudioChunk`, `TriggerEvent`, `TranscriptionEvent`, `Shutdown` |
| `dashboard.py` | `SpeakiDashboard` — aiohttp WebSocket server at `127.0.0.1:4000`, pushes JSON state every 500 ms |
| `sounds.py` | SFX pool pickers |
| `opus.py` | libopus loader |
| `vendor_bootstrap.py` | Injects vendored `discord-ext-voice-recv` into `discord.ext.__path__` |

## Audio pipeline

Discord RTP → `SpeakiAudioSink.write()` (silence gate, 0.5 s batches) → fan-out to one `multiprocessing.Queue` **per language** → `worker_main` subprocess → `KaldiRecognizer` per speaker → `TriggerEvent` / `TranscriptionEvent` on shared output queue → `_consume_worker_events` asyncio task in session.

## Worker pool facts

- **One subprocess per enabled language** (`en`, `ko`, `ja`). NOT one per user.
- Fan-out: every user's audio chunk goes to **every** language worker queue.
- Pool size capped at `min(channel_human_count, MAX_WORKERS)` (8 Darwin / 10 Windows, defined in `state.py`).
- `worker_processes: dict[str, Process]` keyed by language string.
- Workers share one output queue; `_consume_worker_events` drains it.

## Dashboard state

- `SpeakiSession.recent_transcriptions: dict[int, dict]` — latest transcription per `user_id`, written by `_consume_worker_events`, read by dashboard.
- Dashboard serves `dashboard/index.html` at `/` and WebSocket at `/ws`.

## Key things NOT to change without care

- Sink fan-out architecture (all queues get all chunks).
- Worker spawn/teardown in `_ensure_worker` / `_stop_worker_processes`.
- Voice recovery pipeline in `_recover_voice_transport` / `_recover_listener`.
