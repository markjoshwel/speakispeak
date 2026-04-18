# speakispeak

a message-triggered speaki sound effect bot with wakeword detection

## setup

**1. install prerequisites**

- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- [Bun](https://bun.sh/) (for the dashboard frontend)
- `ffmpeg` on `PATH`

**2. config**

copy `config.example.toml` to `config.toml` and fill in at minimum:

```toml
app_token = "your-bot-token"
admin_user_id = "your-discord-user-id"
```

set `vc-worker-use-whisper = true` if you want Whisper instead of Vosk.
Whisper downloads its model automatically; Vosk needs model folders under `models/`
(see [Vosk models](#vosk-models) below).

**3. install Python deps**

```text
uv sync
```

**4. build the dashboard**

```text
cd web
bun install
bun run build
cd ..
```

**5. place dashboard assets under `web/public/assets/`**

these are gitignored and must be added manually:

```text
web/public/assets/
├── bg.jpg                  background image for the dashboard
├── fonts/
│   └── ONE Mobile POP.ttf  display font
└── sprites/
    ├── speaki_01.png
    ├── speaki_02.png
    └── … (speaki_01.png – speaki_31.png)
```

**6. put SFX under `sounds/`**

any folder layout works — the bot picks a random file recursively.

**7. run**

```text
uv run main.py
```

then type `speaki` in a text channel while in a voice channel.  
the dashboard is at `http://localhost:6782/`.

---

## Vosk models

if not using Whisper, download small Vosk models and place them under `models/`:

```text
models/
├── vosk-model-small-en-us-0.15
├── vosk-model-small-ko-0.22
└── vosk-model-small-ja-0.22
```

the worker looks for directories prefixed with `vosk-model-small-en-`,
`vosk-model-small-ko-`, or `vosk-model-small-ja-`.

---

## commands

| command | who | effect |
|---|---|---|
| `speaki` | anyone in VC | summon speaki to your channel |
| `speaki stop` | anyone | start a leave vote (passes at ceil(humans/3)) |
| `speaki config` | admin | show live config |
| `speaki config key = value` | admin | update a config value live |

`admin_user_id` bypasses the stop vote immediately.

`master_user_ids` in config is a list of user IDs invisible to speaki — their audio
is ignored, they don't appear on the dashboard, and they can't summon or stop speaki.

---

## repo shape

- `main.py` — Discord client entrypoint
- `elias/` — bot logic, audio, wakeword detection, workers, dashboard server
- `web/` — React dashboard frontend (Bun + Vite + TypeScript)
- `vendor/discord-ext-voice-recv/` — vendored receive library with local fixes

---

## licencing

dual-licensed as `Unlicense OR 0BSD` — see `UNLICENSE` and `LICENSE-0BSD`.

vendored `discord-ext-voice-recv` under `vendor/discord-ext-voice-recv/` remains
under its original MIT licence — see `THIRD_PARTY_NOTICES.md`.
