# AGENTS.md

## Project Shape

- `main.py` is the Discord client entrypoint.
- Helper code lives under `elias/`.
- Voice receive uses `discord-ext-voice-recv`.
- Speech recognition runs in spawned worker processes — Vosk (per-language) and Whisper
  (shared pool, stateless).
- A real-time admin dashboard is served on port 6782 via `elias/dashboard.py`.
- The React frontend lives under `web/` and is built with Bun + Vite.

## Runtime Behaviour

- Text `speaki` joins the sender's voice channel, starts voice receive, and plays a random SFX immediately.
- Spoken wake words or phrases starting with `speaki` trigger another SFX and refresh session activity.
- Sessions are keyed per guild and expire after 10 minutes of no text or spoken trigger activity.
- `speaki stop` requests that speaki leave the channel — requires a 1/3-of-members vote, or admin bypass.
- The Whisper worker pool scales dynamically: `min(human_vc_count, WORKER_POOL_SIZE)` workers are
  live at any time. Scale-up is immediate; scale-down uses a 30 s grace period.
- The dashboard streams `audio_peak`, `worker_routing`, `trigger`, `member_join/leave`,
  `worker_pool_resize`, `vote_update`, and `session_close` events over WebSocket.

## Implementation Notes

- Keep heavy STT work out of the voice receive callback.
- Use `Path.joinpath(...)` rather than `/` for new path composition.
- Worker language loading is configurable from `config.toml`.
- Config accepts standard `ko` and `ja` keys and also `kr` and `jp` aliases.
- Sound playback currently uses `discord.FFmpegPCMAudio`, so `ffmpeg` must be available on `PATH`.
- Dashboard event emission must be thread-safe: always use `loop.call_soon_threadsafe(loop.create_task, coro)`.
- Session close requested from the health monitor (e.g., empty-VC guard) is done by setting
  `close_requested_reason` on the session; the janitor polls and calls `close()`.
- Whisper workers are stateless — `WhisperSpeakerRouter` assigns a round-robin virtual worker index
  for dashboard routing visualisation only; actual inference is shared-queue.
- `_current_worker_signature()` must include `_desired_pool_size()` so that a change in VC count
  triggers `_sync_worker_state` to detect a mismatch and restart workers.

## Current Files

- `elias/state.py`: shared constants and queue event types
- `elias/audio.py`: PCM conversion from Discord audio to Whisper/Vosk input
- `elias/detection.py`: text normalisation and trigger matching
- `elias/sink.py`: bounded queue audio sink with per-speaker amplitude reporting
- `elias/stt_worker.py`: spawned Vosk worker
- `elias/whisper_worker.py`: spawned Whisper worker (stateless, shared queue)
- `elias/session.py`: per-guild session lifecycle, worker orchestration, playback, shutdown
- `elias/dashboard.py`: aiohttp server — serves `web/dist/` SPA and `/ws` WebSocket endpoint
- `web/`: React 19 + Vite 6 + TypeScript frontend (built with Bun)

## Frontend (web/)

Built with `bun run build` from `web/`. Dev server via `bun run dev` proxies `/ws` to port 6782.

Key source files:

- `web/src/types.ts`: `DashboardEvent` discriminated union, `UserState`, `MemberInfo`
- `web/src/hooks/useDashboard.ts`: WebSocket state management via `useReducer`, auto-reconnect
- `web/src/utils.ts`: `userHue()` — djb2 hash of user_id mapped to oklch hue, shifted away from
  forest-green background
- `web/src/components/UserCard.tsx`: avatar, username, waveform per speaker
- `web/src/components/WaveformCanvas.tsx`: 50-bar canvas bar chart, DPR-scaled
- `web/src/components/WorkerNode.tsx`: idle/active circle with glow for active Whisper slots
- `web/src/components/ConnectionLines.tsx`: rAF-driven SVG bezier curves user→worker routing
- `web/src/components/TranscriptionLine.tsx`: scrolling per-user trigger text
- `web/src/components/SpeakiSprite.tsx`: random sprite (01–31), hops on trigger events
- `web/src/components/VoteBanner.tsx`: `speaki stop` vote progress overlay

Static assets (gitignored) go in `web/public/assets/` — sprites, background image, font.
