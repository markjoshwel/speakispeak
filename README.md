# speakispeak

a message-triggered speaki sound effect bot with wakeword detection

## repo and setup

repo shape:

- `main.py`  
  Discord client entrypoint

- `elias/`  
  bot logic, audio handling, wakeword detection, and worker code

- `vendor/discord-ext-voice-recv/`  
  vendored receive library snapshot with local fixes

create `config.toml` from `config.example.toml` with this shape:

```toml
app_token = "..."
admin_user_id = "123456789012345678"

debug = false
dump-worker-audio = false
vc-worker = true
vc-worker-finish-wait = 1.0
vc-timeout = 600
vc-worker-use-grammar = false
vc-worker-strict-final-only = true
vc-worker-strict-double-hit = true

vc-worker-load-en = true
vc-worker-load-ko = true
vc-worker-load-ja = false
```

`vc-worker-load-ko` also accepts the alias `vc-worker-load-kr`.  
`vc-worker-load-ja` also accepts the alias `vc-worker-load-jp`.

install requirements:

```text
uv sync
```

`uv sync` installs the voice receive runtime dependencies (`pynacl`, `davey`).
on `darwin/arm64`, it also installs the vendored Vosk wheel from `vendor/wheels/`
instead of trying to download an unsupported PyPI wheel.

put `ffmpeg` on `PATH`.

download the small Vosk models you want to use and place them under `models/`.
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

put your SFX folders under `sounds/`.

## how to use it

run the bot:

```text
uv run main.py
```

then type `speaki` in a guild text channel while you are in a voice channel.

the configured `admin_user_id` can inspect and update the live non-sensitive config with:

```text
speaki config
```

or:

```toml
speaki config

vc-worker = true
vc-worker-strict-double-hit = true
vc-worker-strict-final-only = false
vc-worker-use-grammar = false
```

single-line updates also work:

```text
speaki config vc_worker = false
```

the live update path accepts both the canonical hyphenated keys and underscore
aliases in commands. existing connected voice sessions are refreshed
immediately, so toggling `vc_worker` off stops wakeword workers and toggling it
back on redeploys them for active sessions.

current behaviour:

- joins your current voice channel
- plays a random SFX immediately
- if `vc-worker = true`, listens for configured wakewords through Vosk
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
