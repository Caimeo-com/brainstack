# Brainstack Diagrams

These diagrams show the current customer-zero architecture without adding new services or hidden state.

## Shared-Brain Read, Write, And Outbox Flow

```mermaid
sequenceDiagram
  participant H as Harness
  participant LC as Local clone
  participant B as braind
  participant G as Canon bare repo
  participant O as Local outbox

  Note over H,LC: Read path
  H->>LC: consult AGENTS.shared-client.md, wiki, search
  alt session start or explicit sync
    LC->>G: git pull --ff-only over SSH
    G-->>LC: latest canon
  else no sync
    Note over LC: reads may be stale but local
  end

  Note over H,B: Write path
  H->>B: POST import/propose
  alt brain reachable
    B->>G: persist raw/proposal or ingest result
    G-->>LC: visible to others on next pull
  else brain unreachable / timeout
    H->>O: queue payload + idempotency key
    O-->>H: queued warning
  end

  Note over H,O: Recovery
  H->>O: auto-retry or brainctl outbox flush
  O->>B: replay queued writes
  B->>G: persist canon-adjacent data
  Note over LC: freshness stays pull-based,\nwrite continuity comes from outbox
```

## Telegram Text Coalescing

```mermaid
flowchart TD
  A[Telegram text msg 1] --> P[Pending text buffer]
  B[Telegram text msg 2] --> C{same chat/topic/user\nwithin coalesce window?}
  C -- yes --> P
  C -- no --> F[Flush prior text as prompt]

  D[/command/] --> X[Flush pending text first]
  E[attachment/caption] --> X
  T[window timeout] --> X

  P --> T
  X --> R[Submit one prompt to harness]
  F --> R
```

## Control, Client, Worker Topology

```mermaid
flowchart LR
  subgraph ClientSide[Client side]
    M1[Mac / local harness]
    C1[Local clone\n~/shared-brain]
    O1[Local outbox]
  end

  subgraph ControlHost[Control host]
    TG[Telegram topics]
    TM[telemux]
    BD[braind API/UI]
    ST[staging clone]
    SV[serve clone]
    BR[(bare repo / canon)]
  end

  subgraph WorkerSide[Worker side]
    W1[yoda worker]
    C2[Worker local clone\n~/shared-brain]
    O2[Worker outbox]
  end

  M1 -->|read/search locally| C1
  C1 -->|bootstrap or explicit sync\ngit pull --ff-only over SSH| BR

  TG -->|messages| TM
  TM -->|dispatch over SSH/Tailscale network| W1
  W1 -->|execute selected worker harness| W1
  W1 -->|read/search locally| C2
  C2 -->|bootstrap or explicit sync\ngit pull --ff-only over SSH| BR

  M1 -->|import/propose POST| BD
  W1 -->|import/propose POST| BD
  TM -->|run summary / proposal POST| BD

  M1 -->|brain unreachable| O1
  W1 -->|brain unreachable| O2
  O1 -->|flush later| BD
  O2 -->|flush later| BD

  BD -->|writes raw/proposals,\nadmin may ingest| ST
  ST -->|commit/push| BR
  BR -->|post-receive updates| SV
  SV -->|served privately via Tailscale Serve| M1
```
