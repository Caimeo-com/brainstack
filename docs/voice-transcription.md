# Telegram Voice Transcription

Telemux can turn Telegram `voice` and `audio` attachments into normal instruction text before dispatching a bound topic to the harness. The control host downloads the Telegram file, streams it to the configured processor, and then continues through the same plain-text path as a typed Telegram message. No separate Telegram DM is used.

## Install

Use the canonical Brainstack command:

```bash
brainctl capabilities install voice --target erbine
```

Equivalent Telegram operator phrases are supported by Telemux:

```text
install voice on erbine
install transcription on valkyrie
/voice install erbine
```

Telegram sends an immediate acknowledgement, then periodic progress messages while the first install is still running. The default progress interval is `45s`; set `FACTORY_CAPABILITY_PROGRESS_INTERVAL_MS=0` in the Telemux runtime env to disable those heartbeats.

The command:

- checks the target machine from `brainstack.yaml`
- verifies `ffmpeg` is available on the selected processor
- downloads a Mozilla `whisperfile` executable model on that target
- verifies the pinned checksum when one is known
- writes `capabilities.voice` into `brainstack.yaml`
- updates the live `telemux.runtime.env`
- restarts or schedules a restart of `telemux.service`
- prints how to test

The default model is `tiny.en`, because it is small enough for a first install and good enough for short Telegram voice notes. Larger options are available:

```bash
brainctl capabilities install voice --target erbine --model small.en
brainctl capabilities install voice --target erbine --model medium.en
brainctl capabilities install voice --target erbine --model large-v3
```

Use `--install-root DIR` to choose where the model executable is stored on the target machine. The default is `~/.local/share/brainstack/capabilities/voice`.

## Status And Test

Check installed state:

```bash
brainctl capabilities doctor voice
```

From Telegram:

```text
voice status
/voice status
```

Smoke a local audio file from the control host:

```bash
brainctl capabilities test voice --file /path/to/sample.ogg
```

The user-facing smoke is simpler: send a Telegram voice note in any bound Brainstack topic. If `echoTranscript` is enabled, Telemux echoes the transcript in the same topic before dispatching it.

## Runtime Contract

The installer persists durable config under `capabilities.voice` in `brainstack.yaml`. Generated runtime env mirrors that config for Telemux:

```env
FACTORY_TRANSCRIPTION_ENABLED=1
FACTORY_TRANSCRIPTION_TARGET=worker
FACTORY_TRANSCRIPTION_WORKER=erbine
FACTORY_TRANSCRIPTION_COMMAND=/home/operator/.local/share/brainstack/capabilities/voice/whisper-tiny.en.llamafile
FACTORY_TRANSCRIPTION_ARGS_JSON=["-f","{input}","-pc"]
FACTORY_TRANSCRIPTION_TIMEOUT_MS=120000
FACTORY_TRANSCRIPTION_ECHO=1
FACTORY_TRANSCRIPTION_MAX_BYTES=20971520
FACTORY_TRANSCRIPTION_MAX_DURATION_SECONDS=300
FACTORY_CAPABILITY_PROGRESS_INTERVAL_MS=45000
```

Do not hand-edit the env file as the primary setup path; `brainctl lifecycle repair`, `upgrade`, and `apply-runtime` regenerate it from config.

Targets:

- `target: local` runs transcription on the Telemux host.
- `target: worker` runs transcription on `worker`, using that machine's worker transport.

Command contract:

- Telemux streams downloaded audio bytes into a temporary file on the selected target.
- If `ffmpeg` is available, Telemux converts the input to 16 kHz mono WAV before transcription. This is the reliable path for Telegram voice notes, which usually arrive as OGG/Opus.
- The configured command is executed with `args`.
- `{input}` as a whole argument is replaced by the temporary audio file path. If no argument is exactly `{input}`, Brainstack appends it.
- Stdout is treated as the transcript. Non-zero exit, timeout, empty stdout, oversize files, and over-duration files are reported in the same Telegram topic and do not start the harness.

Mozilla `whisperfile` is Mozilla's llamafile-based Whisper packaging. See [Mozilla/whisperfile on Hugging Face](https://huggingface.co/Mozilla/whisperfile).
