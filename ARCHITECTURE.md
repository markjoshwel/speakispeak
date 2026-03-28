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

session shutdown:

- leaves after `vc-timeout` seconds of no typed or spoken trigger activity
- leaves when no human members remain in the tracked voice channel

## process model

the bot is split into two domains:

### main process

owned by `main.py` and `elias/session.py`

responsibilities:

- `discord.py` client lifecycle
- session registry per guild
- message trigger handling
- voice connect, move, and disconnect
- SFX playback
- session inactivity checks
- consuming worker trigger events

### worker processes

owned by `elias/stt_worker.py`

responsibilities:

- loading Vosk models
- converting Discord PCM into Vosk PCM
- maintaining per-speaker recogniser state
- wakeword detection
- emitting trigger events back to the main process

the current implementation runs **one worker process per enabled language** for
each active guild session when `vc-worker = true`.

that means a session with `en`, `ko`, and `ja` enabled spawns three workers:

```text
guild session
├── en worker
├── ko worker
└── ja worker
```

we started with a single guild worker handling all languages serially, but that
backed up too easily once more than one speaker was active. splitting by
language reduced the worst bottleneck and gave cleaner shutdown control.

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
```

voice flow:

```text
Discord voice receive
  -> vendored discord-ext-voice-recv
  -> SpeakiAudioSink
  -> cheap voice gate drops obvious silence
  -> per-speaker PCM batching
  -> fan out AudioChunk to each language worker queue
  -> worker converts PCM for Vosk
  -> worker keeps one recogniser per speaker
  -> wakeword recognised
  -> TriggerEvent back to main process
  -> main process refreshes activity and plays SFX
```

## audio handling

audio handling is where most of the architecture ended up being decided.

### what we use now

- `discord-ext-voice-recv` with `wants_opus() -> False`
- decoded `48 kHz`, `16-bit`, stereo PCM from the receive library
- a cheap energy gate before queueing audio
- batching in `elias/sink.py`
- downmix and resample in `elias/audio.py`
- `16 kHz` mono PCM fed into Vosk

the current sink is deliberately cheap:

- validate the speaker
- drop obvious silence before queue fanout
- accumulate a short per-speaker PCM buffer
- flush a batched `AudioChunk`
- never do STT work inside the sink callback

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

the current recogniser model is:

- one recogniser per speaker, per language worker
- wakeword-only detection
- optional grammar-limited recognisers per language
- no open-ended sentence detection

we originally explored handling sentences that begin with `speaki`, but in
practice it kept expanding the phrase basket and did not hold up well against
the noisy transcripts we were actually getting back. the current design only
cares about wakewords.

recognition details:

- partial results are mainly useful for wakeword latency
- final results are logged more conservatively
- each recogniser can optionally be constrained to that language's wakeword
  grammar
- repeated identical trigger text is rate-limited per speaker
- some wakewords are delayed until shortly after speech ends
- other wakewords fire immediately
- strict trigger mode can require final-only hits, double-hit confirmation, or
  both

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

activity only refreshes on:

- typed `speaki`
- recognised wakeword trigger

arbitrary speech does **not** refresh activity. otherwise the bot would stay in
voice forever as long as anybody kept talking.

## shutdown behaviour

shutdown turned out to need explicit ordering.

the current close path does this:

1. mark session closed
2. shut down the receive sink
3. stop voice listening
4. stop the worker consumer task
5. stop playback
6. signal worker shutdown
7. join or terminate workers
8. close multiprocessing queues
9. disconnect the voice client

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
├── detection.py
├── session.py
├── sink.py
├── sounds.py
├── state.py
├── stt_worker.py
├── vendor_bootstrap.py
├── voice_recv_patch.py
└── wakewords.py
scripts/
├── recv_to_wav.py
└── send_message.py
vendor/
└── discord-ext-voice-recv/
```

module responsibilities:

- `main.py`  
  Discord client, typed trigger handling, voice-state leave handling, janitor

- `elias/session.py`  
  per-guild session lifecycle, worker orchestration, playback, shutdown

- `elias/sink.py`  
  cheap receive callback, batching, bounded queue fanout

- `elias/audio.py`  
  Discord PCM to Vosk PCM conversion

- `elias/stt_worker.py`  
  Vosk workers, per-speaker recognisers, wakeword detection, worker logging

- `elias/detection.py`  
  text normalisation, wakeword matching, log-window formatting

- `elias/sounds.py`  
  SFX selection and description helpers

- `elias/state.py`  
  shared constants and queue message types

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

## non-goals of the current design

the current implementation does **not** try to be:

- a full transcription bot
- a reliable sentence parser
- a backlog-preserving recorder
- a high-accuracy multilingual ASR pipeline

it is a message-triggered SFX bot with wakeword detection. that narrower goal is
what the current architecture is optimised around.
