# AGENTS.md

## Project Shape

- `main.py` is the Discord client entrypoint.
- Helper code lives under `elias/`.
- Voice receive uses `discord-ext-voice-recv`.
- Speech recognition runs in a spawned worker process with `vosk`.

## Runtime Behaviour

- Text `speaki` joins the sender's voice channel, starts voice receive, and plays a random SFX immediately.
- Spoken wake words or phrases starting with `speaki` trigger another SFX and refresh session activity.
- Sessions are keyed per guild and expire after 10 minutes of no text or spoken trigger activity.

## Implementation Notes

- Keep heavy STT work out of the voice receive callback.
- Use `Path.joinpath(...)` rather than `/` for new path composition.
- Worker language loading is configurable from `config.toml`.
- Config accepts standard `ko` and `ja` keys and also `kr` and `jp` aliases.
- Sound playback currently uses `discord.FFmpegPCMAudio`, so `ffmpeg` must be available on `PATH`.

## Current Files

- `elias/state.py`: shared constants and queue event types
- `elias/audio.py`: PCM conversion from Discord audio to Vosk input
- `elias/detection.py`: text normalisation and trigger matching
- `elias/sink.py`: bounded queue audio sink
- `elias/stt_worker.py`: spawned Vosk worker
- `elias/session.py`: per-guild session lifecycle and playback

