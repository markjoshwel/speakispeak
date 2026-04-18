# speakispeak

a message-triggered speaki sound effect bot with wakeword detection

## repo shape

- `main.py`  
  Discord client entrypoint

- `elias/`  
  bot logic, audio handling, wakeword detection, worker code, and dashboard server

- `web/`  
  React dashboard frontend (Bun + Vite + TypeScript)

- `vendor/discord-ext-voice-recv/`  
  vendored receive library snapshot with local fixes

## setup

create `config.toml` from `config.example.toml`:

```toml
app_token = "..."
admin_user_id = "123456789012345678"
master_user_ids = []

debug = false
dump-worker-audio = false
vc-worker = true
vc-worker-finish-wait = 1.0
vc-timeout = 600
vc-worker-use-grammar = true
vc-worker-strict-final-only = true
vc-worker-strict-double-hit = true
vc-worker-pool-size = 8
vc-worker-use-whisper = false
vc-worker-whisper-model = "tiny"

vc-worker-load-en = true
vc-worker-load-ko = true
vc-worker-load-ja = false
```

install requirements:

```text
uv sync
```

`uv sync` installs the voice receive runtime dependencies (`pynacl`, `davey`).
on `darwin/arm64`, it also installs the vendored Vosk wheel from `vendor/wheels/`
instead of trying to download an unsupported PyPI wheel.

put `ffmpeg` on `PATH`.

### Vosk (default)

download the small Vosk models you want and place them under `models/`.
the worker looks for directories matching these prefixes:

- `vosk-model-small-en-`
- `vosk-model-small-ko-`
- `vosk-model-small-ja-`

example:

```text
models
├── vosk-model-small-en-us-0.15
├── vosk-model-small-ja-0.22
└── vosk-model-small-ko-0.22
```

### Whisper (optional)

set `vc-worker-use-whisper = true` and choose a model size with
`vc-worker-whisper-model` (`tiny`, `base`, `small`, …).  
the model is downloaded automatically from HuggingFace on first run.

Whisper workers share a single stateless queue, so all pool slots process audio
from any speaker. Pool size scales to the number of humans currently in the VC
(`min(humans, vc-worker-pool-size)`).

put your SFX folders under `sounds/`.

## dashboard

the bot serves a real-time admin dashboard at `http://0.0.0.0:6782/` by default.

the three-column layout shows:

- **left** — per-user live waveforms
- **centre** — worker pool nodes with animated routing lines
- **right** — scrolling transcription per user

the speaki sprite hops on each wakeword trigger.

to change the port, add `dashboard-port = <port>` to `config.toml`.  
set `dashboard-port = 0` to disable the dashboard entirely.

## how to use it

run the bot:

```text
uv run main.py
```

then type `speaki` in a guild text channel while you are in a voice channel.

### admin commands

the configured `admin_user_id` can inspect and update the live non-sensitive config:

```text
speaki config
```

or with inline updates:

```toml
speaki config

vc-worker = true
vc-worker-strict-double-hit = true
```

single-line updates also work:

```text
speaki config vc_worker = false
```

both hyphenated keys and underscore aliases are accepted. toggling `vc_worker`
off stops wakeword workers immediately; toggling it back on redeploys them for
active sessions.

### stop command

any member can vote to make speaki leave:

```text
speaki stop
```

a vote passes at `ceil(humans / 3)` votes. `admin_user_id` bypasses the vote
immediately.

### master users

`master_user_ids` is a list of Discord user IDs that are completely invisible to
speaki: their audio is dropped at the sink, they do not appear on the dashboard,
they cannot summon or stop speaki, and they are excluded from pool sizing and
empty-VC counts.

## current behaviour

- joins your current voice channel
- plays a random SFX immediately
- if `vc-worker = true`, listens for configured wakewords
- plays another SFX when a wakeword is recognised
- leaves after `vc-timeout` seconds of inactivity
- leaves when no human members remain in the voice channel

for receive debugging, use:

```text
uv run scripts/recv_to_wav.py
```

## licencing

the code in this repo is dual-licensed as `Unlicense OR 0BSD`

see:

- `UNLICENSE`
- `LICENSE-0BSD`

the vendored `discord-ext-voice-recv` code under `vendor/discord-ext-voice-recv/`
remains under its original MIT licence

see:

- `THIRD_PARTY_NOTICES.md`
- `vendor/discord-ext-voice-recv/LICENSE`
