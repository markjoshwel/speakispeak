# speaki architecture

this document describes the architecture that is actually implemented now, plus
the main lessons learned while getting Discord voice receive into a usable
state.

## current behaviour

typed `speaki` in a guild text channel:

1. checks whether the sender is in voice
2. joins or moves to that voice channel
3. starts the voice receive path
4. plays a random SFX immediately
5. keeps listening for wakewords

spoken wakewords:

- trigger another SFX
- refresh session activity

`speaki stop`:

- any VC member can start a vote-to-leave
- threshold: `ceil(human_count * 1/3)` unique voters
- admins bypass the vote entirely
- speaki replies with in-character voicelines during the process

`master_user_ids` config list:

- listed Discord user IDs are completely invisible to speaki
- not shown on dashboard; audio is silently dropped before VAD/routing
- typing `speaki` or `speaki stop` from a master is ignored
- not counted toward pool sizing, empty-VC guard, or stop-vote threshold

session shutdown:

- leaves after `vc-timeout` seconds of no typed or spoken trigger activity
- leaves when no human members remain in the tracked voice channel (30 s grace)
- leaves when `speaki stop` vote passes or admin forces it
- health monitor sets `close_requested_reason`; the janitor polls and calls `close()`

## process model

the bot is split into two domains:

### main process

owned by `main.py` and `elias/session.py`

responsibilities:

- `discord.py` client lifecycle
- session registry per guild
- message trigger handling (`speaki`, `speaki stop`)
- voice connect, move, and disconnect
- SFX playback
- session inactivity checks
- consuming worker trigger events
- broadcasting dashboard events
- vote-to-stop tracking

### worker processes

owned by `elias/stt_worker.py` (Vosk) and `elias/whisper_worker.py` (Whisper)

**Vosk workers** — one process per enabled language per active session:

```text
guild session
├── en worker  (Vosk)
├── ko worker  (Vosk)
└── ja worker  (Vosk)
```

**Whisper workers** — a shared pool, stateless, sized to the voice channel:

```text
guild session
└── whisper pool  (N workers, N = min(human_vc_count, WORKER_POOL_SIZE))
    ├── worker 0
    ├── worker 1
    └── ...
```

the Whisper pool grows immediately when a member joins and shrinks after a
30 s grace period when a member leaves, to avoid churn from rapid reconnects.

we started with a single guild worker handling all languages serially, but that
backed up too easily once more than one speaker was active. splitting by
language reduced the worst bottleneck and gave cleaner shutdown control.

## dynamic whisper worker pool

the number of live Whisper workers is:

```python
desired = min(human_vc_count, WORKER_POOL_SIZE)
```

pool sizing is governed by these constants in `elias/state.py`:

| constant | value | meaning |
|---|---|---|
| `WORKER_POOL_SIZE` | 8 | hard upper bound |
| `WORKER_SCALE_DOWN_GRACE_SECONDS` | 30.0 | delay before shrink |

scale-up path:

1. `on_member_count_changed(count)` called from `on_voice_state_update`
2. `_scale_pool_immediate()` → `_sync_worker_state()` discovers mismatch via
   `_current_worker_signature()` (which includes `_desired_pool_size()`)
3. new workers spawned, old surplus workers stopped

scale-down path:

1. same entry point, but count decreased
2. `_scale_pool_deferred()` schedules an `asyncio.Task` to sleep
   `WORKER_SCALE_DOWN_GRACE_SECONDS` then call `_scale_pool_immediate()`
3. if a member rejoins before the sleep expires, the signature matches at
   wake-up and `_sync_worker_state` returns "unchanged" — no restart

`_current_worker_signature()` deliberately encodes `_desired_pool_size()`.
any change in desired size therefore triggers a worker restart.

## runtime flow

typed trigger flow:

```text
on_message("speaki")
  -> find or create guild session
  -> join or move to author's voice channel
  -> spawn workers if needed
  -> wait for workers to report ready
  -> attach receive sink if workers are enabled
  -> play random SFX
  -> emit session_state to dashboard
```

voice flow:

```text
Discord voice receive
  -> vendored discord-ext-voice-recv
  -> SpeakiAudioSink
  -> cheap voice gate drops obvious silence
  -> per-speaker PCM batching
  -> fan out AudioChunk:
       Vosk path: each language worker queue
       Whisper path: WhisperSpeakerRouter -> shared Whisper queue (round-robin)
  -> worker converts PCM
  -> wakeword recognised
  -> TriggerEvent back to main process via result queue
  -> main process refreshes activity and plays SFX
  -> dashboard emits trigger event
```

stop-vote flow:

```text
on_message("speaki stop")
  -> if admin: request_close() immediately, emit session_close
  -> else: create or update StopVote for guild
  -> check voters >= ceil(human_count * 1/3)
  -> if passed: request_close(), emit session_close
  -> else: reply with vote-progress voiceline, emit vote_update to dashboard
```

empty-VC guard:

```text
_check_voice_health() [every VOICE_HEALTH_POLL_INTERVAL_SECONDS]
  -> counts human members in tracked channel
  -> if 0 humans for >= EMPTY_VC_GRACE_SECONDS:
       session.close_requested_reason = "empty vc"
  -> _recover_voice_transport() refuses to reconnect if channel has 0 humans
_run_session_janitor() [every JANITOR_INTERVAL_SECONDS]
  -> if close_requested_reason is set: session.close()
```

## audio handling

audio handling is where most of the architecture ended up being decided.

### what we use now

- `discord-ext-voice-recv` with `wants_opus() -> False`
- decoded `48 kHz`, `16-bit`, stereo PCM from the receive library
- a cheap energy gate before queueing audio
- batching in `elias/sink.py`
- downmix and resample in `elias/audio.py`
- `16 kHz` mono PCM fed into Vosk or Whisper

the current sink is deliberately cheap:

- validate the speaker
- drop obvious silence before queue fanout
- accumulate a short per-speaker PCM buffer
- flush a batched `AudioChunk` and emit an `audio_peak` dashboard event
- never do STT work inside the sink callback

the sink also computes RMS amplitude per chunk for the dashboard waveform.

### why we do not decode Opus in the worker

we tried pushing Opus packets into the worker and decoding there.
that produced obviously garbled WAV output and was the wrong layer to own
decoder state.

the working design is to let the receive library own RTP, jitter, and Opus
decode state, then only pass PCM forward.

### why the receive library is vendored

the stock receive path initially produced:

- `discord.opus.OpusError: corrupted stream`
- badly garbled captured WAV output

that turned out not to be a Speaki bug. the problem was inside
`discord-ext-voice-recv` on the current Discord voice behaviour.

we confirmed this by building a minimal receive-to-WAV script in
`scripts/recv_to_wav.py`. once the minimal script also produced broken audio, it
was clear the app layer was innocent.

the fix was to vendor `discord-ext-voice-recv` locally and port in the upstream
receive-path fixes that handled the newer voice behaviour correctly.

lesson:

- if Discord receive audio is garbled in a minimal capture script, stop blaming
  STT and fix the receive stack first

### why batching exists

passing every tiny PCM frame straight into multiprocessing queues was too
expensive.

the current sink batches PCM per speaker before queueing it. this reduces
cross-process overhead and keeps the sink callback lighter.

lesson:

- realtime voice bots should batch audio before crossing a process boundary
- cheap silence gating is worth doing before multiprocessing fanout

### why queues are bounded and lossy

this bot is a trigger bot, not an archival transcription system.

once the worker falls behind, stale audio is worse than dropped audio. a late
wakeword is basically useless. because of that, the sink uses bounded queues and
drops work under pressure instead of letting memory grow forever.

lesson:

- for wakeword detection, "latest audio wins" is the correct overload policy

## recognition model

the current recogniser model has two independent STT paths:

**Vosk path** (per-language, per-speaker recognisers):

- one recogniser per speaker, per language worker
- wakeword-only detection
- optional grammar-limited recognisers per language
- partial results for low-latency wakeword detection
- per-speaker recogniser boundary is essential — mixing speakers corrupts history

**Whisper path** (shared pool, no per-speaker state):

- stateless workers, shared input queue
- `WhisperSpeakerRouter` buffers incoming `AudioChunk`s and dispatches them to
  the shared Whisper queue
- uses `initial_prompt` to bias the model towards the `speaki` wakeword
- `WhisperJob.enqueued_at` enables age-based drop: jobs older than
  `WHISPER_JOB_MAX_AGE_SECONDS` are discarded to prevent stale detections

because Whisper workers are stateless, round-robin virtual slot assignment in
`WhisperSpeakerRouter` provides a stable worker index purely for dashboard
routing visualisation. it has no effect on which process handles the audio.

we originally explored handling sentences that begin with `speaki`, but in
practice it kept expanding the phrase basket and did not hold up well against
the noisy transcripts we were actually getting back. the current design only
cares about wakewords.

### wakeword detection in phrases

`detect_wakeword` in `elias/detection.py` normalises the transcript text (strips
punctuation, casefolds, collapses whitespace) then checks for wakewords via a
sliding token-window scan so "What's up, Speaky?" or "Neu speaki." both trigger
the same as a bare "speaki".

the scan is ordered right-to-left (`reverse=True`) so the most recently spoken
tokens are matched first — important for response latency in a wakeword bot.

**gotcha**: the inner window loop must use `continue`, not `break`, when a
candidate window overflows the token list. in reverse mode the size decrements
(3 → 2 → 1), so an overflow on size=3 does not mean size=2 will also overflow.
`break` silently skipped the valid 1-token window for any wakeword that appeared
at the end of a phrase.

recognition details (Vosk):

- partial results are mainly useful for wakeword latency
- final results are logged more conservatively
- each recogniser can optionally be constrained to that language's wakeword grammar
- repeated identical trigger text is rate-limited per speaker
- some wakewords are delayed until shortly after speech ends
- other wakewords fire immediately
- strict trigger mode can require final-only hits, double-hit confirmation, or both

the per-speaker recogniser boundary matters a lot. mixing speakers into one
recogniser causes transcript history and cooldown logic to become nonsense.

lesson:

- never mix multiple speakers into one Vosk recogniser if what you care about is
  low-latency wakeword spotting

## session model

there is one `SpeakiSession` per guild.

session state includes:

- current tracked voice channel
- voice receive client
- receive sink
- worker processes
- worker input and output queues
- worker ready and shutdown events
- playback state
- last activity timestamp
- `close_requested_reason`: set by the health monitor or vote system to request
  a deferred close; the janitor calls `close()` when this is set
- `_scale_down_task`: pending asyncio task for deferred pool shrink

activity only refreshes on:

- typed `speaki`
- recognised wakeword trigger

arbitrary speech does **not** refresh activity. otherwise the bot would stay in
voice forever as long as anybody kept talking.

## real-time dashboard

### server side

`elias/dashboard.py` runs an aiohttp server on port 6782 (configurable,
0 to disable).

- serves the built React SPA from `web/dist/` with SPA fallback routing
- `/ws` WebSocket endpoint with `heartbeat=20.0`
- caches the last `session_state` event and replays it to new connections
- `make_emitter(loop)` returns a thread-safe callable; it uses
  `loop.call_soon_threadsafe(loop.create_task, coro)` so it can be called from
  any thread (including the Discord receive thread)

event types emitted:

| event type | when |
|---|---|
| `session_state` | on connect (cached), and after join/leave; includes `transcription_history` |
| `bot_status` | loading / listening / reconnecting transitions; also reflected in cached `session_state` |
| `live_audio` | ~20 fps per speaker, raw RMS amplitude (pre-VAD, for waveform) |
| `worker_routing` | each Whisper dispatch (virtual round-robin slot) |
| `transcription` | each Whisper segment; `wakeword` field is non-null on trigger |
| `trigger` | Vosk wakeword detection (legacy, kept for Vosk path) |
| `member_join` / `member_leave` | voice state update |
| `worker_pool_resize` | pool size change |
| `vote_update` | `speaki stop` vote progress |
| `session_close` | session teardown |

the cached `session_state` is updated on every `bot_status` event so late-joining
dashboard clients see the current status, not a stale snapshot from before a
reconnection cycle.

### client side

`web/` — React 19 + Vite 6 + TypeScript, built with Bun.

layout: three-column grid

```
┌───────────────┬───────────────┬───────────────┐
│  user cards   │  worker nodes │ transcription │
│  (left)       │  (centre)     │  (right)      │
└───────────────┴───────────────┴───────────────┘
         SVG bezier lines overlay (ConnectionLines)
                      speaki sprite (bottom-right)
```

design choices:

- ONE Mobile POP font for the trickal-viewer aesthetic
- forest background `bg_natural_mori.jpg` darkened with CSS brightness/saturate
- per-user tonal colours derived from `userHue(userId)` — djb2 hash mapped to
  oklch hue, shifted 190° away from the green background range
- glass-morphism cards: `backdrop-filter: blur(10px)` + oklch tonal fill
- `ConnectionLines` runs a `requestAnimationFrame` loop reading `routesRef.current`
  and querying DOM positions via `getBoundingClientRect` to draw live SVG paths
- routes expire after 2200 ms TTL (opacity fades with age)
- speaki sprite randomly picks one of 31 art assets on mount, hops on each trigger
- users are vertically centered in the card (`justify-content: safe center`) with
  fixed-height rows; when users overflow the card they clip at the bottom
- user row order updates lazily every 3 s — most recently active rises to the top
  without constant shuffling; new joiners start at the bottom (`last_active_at: 0`)

## shutdown behaviour

shutdown turned out to need explicit ordering.

the current close path does this:

1. mark session closed
2. emit `session_close` dashboard event
3. shut down the receive sink
4. stop voice listening
5. stop the worker consumer task
6. stop playback
7. signal worker shutdown
8. join or terminate workers
9. close multiprocessing queues
10. cancel `_scale_down_task` if pending
11. disconnect the voice client

this ordering exists because loose shutdown left the parent process hanging on
queue feeder threads, and sometimes let the sink keep logging dropped chunks
after workers were already gone.

lesson:

- for multiprocessing voice bots on Windows, queue shutdown is part of process
  shutdown

## current file layout

```text
main.py
elias/
├── __init__.py
├── audio.py
├── dashboard.py
├── detection.py
├── opus.py
├── session.py
├── sink.py
├── sounds.py
├── state.py
├── stt_worker.py
├── vendor_bootstrap.py
├── whisper_worker.py
└── wakewords.py
web/
├── index.html
├── package.json
├── vite.config.ts
├── tsconfig.json
└── src/
    ├── App.tsx
    ├── app.css
    ├── main.tsx
    ├── types.ts
    ├── utils.ts
    ├── hooks/
    │   └── useDashboard.ts
    └── components/
        ├── ConnectionLines.tsx
        ├── SpeakiSprite.tsx
        ├── TranscriptionLine.tsx
        ├── UserCard.tsx
        ├── VoteBanner.tsx
        ├── WaveformCanvas.tsx
        └── WorkerNode.tsx
scripts/
├── recv_to_wav.py
└── send_message.py
vendor/
└── discord-ext-voice-recv/
```

module responsibilities:

- `main.py`  
  Discord client, typed trigger handling (`speaki`, `speaki stop`), voice-state
  join/leave handling, stop-vote tracking, janitor, dashboard startup

- `elias/session.py`  
  per-guild session lifecycle, dynamic Whisper worker pool, worker orchestration,
  playback, shutdown, dashboard event emission

- `elias/sink.py`  
  cheap receive callback, batching, bounded queue fanout, RMS amplitude reporting

- `elias/audio.py`  
  Discord PCM to target PCM conversion (downmix, resample)

- `elias/stt_worker.py`  
  Vosk workers, per-speaker recognisers, wakeword detection, worker logging

- `elias/whisper_worker.py`  
  Whisper workers, stateless shared-queue design, age-based job drop

- `elias/detection.py`  
  text normalisation, wakeword matching, log-window formatting

- `elias/sounds.py`  
  SFX selection and description helpers

- `elias/state.py`  
  shared constants and queue message types

- `elias/dashboard.py`  
  aiohttp server — serves built React SPA and `/ws` WebSocket endpoint

- `elias/wakewords.py`  
  wakeword vocabulary and delayed-vs-immediate trigger grouping

- `scripts/recv_to_wav.py`  
  minimal receive-path verification script

## main lessons learned

- fix the receive layer before touching recognition if the captured WAV is bad
- do not decode Opus again outside the receive library
- keep heavy work out of the sink callback
- batch audio before crossing process boundaries
- bounded, lossy queues are correct for wakeword bots
- one serial worker for every language and speaker is too easy to overload
- shutdown order matters, especially on Windows multiprocessing
- stateless Whisper workers need a virtual slot index for dashboard routing only
- dynamic pool sizing requires encoding desired size in the worker signature
- deferred scale-down with a grace period prevents churn from rapid reconnects
- dashboard event emission must be thread-safe; use `loop.call_soon_threadsafe`
- empty-VC close requested from the health monitor must go via a flag + janitor,
  not a direct close call, because the health monitor doesn't own the session
- sliding window wakeword scan must use `continue` not `break` on overflow in
  reverse mode — sizes shrink, so an oversized window does not rule out smaller ones

## whisper initial_prompt behaviour

### how it works

`initial_prompt` is prepended to the decoder as **context tokens** before the
first 30-second audio window. it shifts token probabilities in the cross-attention
softmax toward words that appear in the prompt. it does **not** apply to
subsequent windows (use `prefix` for that).

this is the only mechanism available for biasing Whisper toward a made-up word
like "speaki" that is absent from the model's training vocabulary.

### the hallucination problem

Whisper echoes the initial_prompt verbatim when:

- audio is short, quiet, or ambiguous relative to the prompt length
- `condition_on_previous_text=True` (default) allows the model to reuse its
  own prior output as the next context window's "previous text", propagating a
  hallucination forward indefinitely

the specific pattern observed: audio with a short mic-bleed blip produced
`"speaki the wakeword is speaki"` — a verbatim substring of the old prompt
`"Speaki BK Speaki The wakeword is Speaki."`.

**fix applied**: removed all natural-language sentences from the prompt (full
grammatical sentences are most susceptible to echo). prompt is now a short
period-separated word list: `"Speaki. Speaki. Speaki. Speaki. BK."`.

### why period separation

periods tokenise as hard delimiters. each period-separated word gets independent
attention from the decoder's cross-attention head. space-separated words blend
into a single soft context and provide weaker per-word bias.

### repetition count

2–4 repetitions of the primary wakeword provide effective bias. beyond 4 the
model ignores the redundancy. the current implementation uses 4 repetitions of
the primary and 1 of each secondary variant.

### condition_on_previous_text

set to `False` in the transcribe call. this prevents a hallucinated segment from
being used as the context for the next segment, breaking the feedback loop that
turns one echo into a continuous stream of hallucinated prompt text.

### echo detection

after transcription, `_is_prompt_echo()` checks whether ≥85% of the words in
the result appear in the prompt's vocabulary. results of ≤2 words are never
filtered (a real single-word "Speaki" detection shares a word with the prompt
but is legitimate).

**do not use substring matching** for this check — `"speaki"` is a substring of
`"speaki. speaki. speaki."` and would incorrectly suppress every real detection.

### initial_prompt vs prefix (faster-whisper)

| parameter | scope | use case |
|---|---|---|
| `initial_prompt` | first window only | opening context / one-shot bias |
| `prefix` | each window | recurring keyword across long audio |

for short discrete clips (≤ 5 s each) as used here, `initial_prompt` is
sufficient. `prefix` is preferable for streaming long-form transcription.

### hotwords parameter

newer versions of faster-whisper expose a `hotwords` parameter that provides
native keyword bias without prompt engineering. check the installed version
before adopting it; the API is not yet stable across releases.

### lessons

- never put full sentences in `initial_prompt`; lists of words only
- always set `condition_on_previous_text=False` for short-clip transcription
- detect echo by word-overlap ratio, not substring match
- `compression_ratio < 2.2` already catches repetitive hallucination loops
  before the echo detector fires

## non-goals of the current design

the current implementation does **not** try to be:

- a full transcription bot
- a reliable sentence parser
- a backlog-preserving recorder
- a high-accuracy multilingual ASR pipeline

it is a message-triggered SFX bot with wakeword detection. that narrower goal is
what the current architecture is optimised around.
