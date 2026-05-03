# pfc-ingest-watchdog

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![PFC-JSONL](https://img.shields.io/badge/PFC--JSONL-v3.4-green.svg)](https://github.com/ImpossibleForge/pfc-jsonl)
[![Version](https://img.shields.io/badge/pfc--ingest--watchdog-v0.1.0-brightgreen.svg)](https://github.com/ImpossibleForge/pfc-ingest-watchdog/releases)

Automatic file watcher for the [PFC Ecosystem](https://github.com/ImpossibleForge/pfc-jsonl). Monitors local folders or S3 prefixes for new files and converts them to `.pfc` automatically — no manual invocation needed.

**Part of the [PFC Ecosystem](https://github.com/ImpossibleForge/pfc-jsonl).**

---

## What it does

```
New file arrives in folder or S3
         ↓
pfc-ingest-watchdog detects it
         ↓
Calls pfc-convert or pfc-migrate (your choice)
         ↓
.pfc archive ready for DuckDB / pfc-gateway queries
```

The watchdog is **tool-agnostic**: it monitors and triggers. The actual conversion logic lives in the configured tool.

| Converter | When to use |
|---|---|
| `pfc-convert` | Apache CLF, nginx, CSV, NDJSON → JSONL → `.pfc` (schema changes) |
| `pfc-migrate` | gzip/zstd/bzip2/lz4 → `.pfc` (compression swap, content unchanged) |

---

## Installation

```bash
pip install pfc-ingest-watchdog
```

Requires either [pfc-convert](https://github.com/ImpossibleForge/pfc-convert) or [pfc-migrate](https://github.com/ImpossibleForge/pfc-migrate) depending on your use case.

Both converters require the `pfc_jsonl` binary on the machine running the watchdog:

```bash
# Linux x64:
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-linux-x64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl

# macOS (Apple Silicon M1–M4):
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-macos-arm64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl
```

> **License note:** `pfc_jsonl` is free for personal and open-source use. Commercial use requires a written license — see [pfc-jsonl](https://github.com/ImpossibleForge/pfc-jsonl).

---

## Quick Start

**1. Create a config file:**

```toml
# watchdog.toml
[watcher]
mode           = "local"
converter      = "pfc-convert"
poll_interval  = 30
state_file     = "/var/run/pfc-watchdog/watchdog.state"

[source]
path           = "/var/log/apache2/"

[output]
path           = "/archive/pfc/"

[converter_options]
schema         = "apache"
on_error       = "skip"
```

**2. Run:**

```bash
# Watch continuously (30s interval)
pfc-ingest-watchdog --config watchdog.toml

# Scan once and exit
pfc-ingest-watchdog --config watchdog.toml --once

# Show what would be converted, do nothing
pfc-ingest-watchdog --config watchdog.toml --dry-run --verbose
```

---

## Config Reference

### `[watcher]`

| Key | Default | Description |
|---|---|---|
| `mode` | `"local"` | `"local"` or `"s3"` |
| `converter` | `"pfc-convert"` | `"pfc-convert"` or `"pfc-migrate"` |
| `poll_interval` | `30` | Seconds between scans |
| `state_file` | `"watchdog.state"` | Tracks processed files (JSON) |
| `audit_log` | — | Path to JSONL audit log (optional) |
| `verbose` | `false` | Verbose output |

### `[source]`

| Key | Default | Description |
|---|---|---|
| `path` | — | Local directory to watch (`mode = "local"`) |
| `recursive` | `false` | Recurse into subdirectories |

### `[output]`

| Key | Default | Description |
|---|---|---|
| `path` | — | Output directory for `.pfc` files (`mode = "local"`) |

### `[converter_options]`

These are passed directly to the configured converter:

**For pfc-convert:**

| Key | Default | Description |
|---|---|---|
| `schema` | `"auto"` | `auto` \| `apache` \| `nginx` \| `csv` \| `ndjson` |
| `on_error` | `"skip"` | `skip` \| `fail` \| `log` |
| `output_format` | `"pfc"` | `pfc` or `jsonl` |
| `timestamp_field` | — | CSV timestamp column name (auto-detected if empty) |

**For pfc-migrate:**

| Key | Default | Description |
|---|---|---|
| `format` | — | Force format: `gz` \| `zst` \| `bz2` \| `lz4` (auto-detected if empty) |

### `[s3]` (for `mode = "s3"`)

| Key | Required | Description |
|---|---|---|
| `source_bucket` | ✓ | S3 bucket to watch |
| `source_prefix` | — | Key prefix to scan (default: all) |
| `dest_bucket` | — | Destination bucket (default: same as source) |
| `dest_prefix` | — | Destination prefix |
| `region` | — | AWS region |
| `endpoint_url` | — | Custom endpoint (MinIO, etc.) |
| `access_key` | — | AWS access key (default: env/IAM role) |
| `secret_key` | — | AWS secret key |

---

## Examples

### Apache logs → PFC (local)

```toml
[watcher]
mode      = "local"
converter = "pfc-convert"

[source]
path = "/var/log/apache2/"

[output]
path = "/archive/pfc/"

[converter_options]
schema   = "apache"
on_error = "skip"
```

### JSONL archives on S3 → PFC

```toml
[watcher]
mode      = "s3"
converter = "pfc-migrate"

[s3]
source_bucket = "my-logs"
source_prefix = "jsonl/incoming/"
dest_bucket   = "my-pfc-archive"
dest_prefix   = "pfc/"
region        = "eu-central-1"
```

### CSV data → PFC (with audit log)

```toml
[watcher]
mode      = "local"
converter = "pfc-convert"
audit_log = "/var/log/pfc-watchdog/audit.jsonl"

[source]
path      = "/data/csv-exports/"
recursive = true

[output]
path      = "/archive/pfc/"

[converter_options]
schema          = "csv"
timestamp_field = "event_time"
on_error        = "log"
```

---

## State File

The watchdog tracks which files have been processed in a JSON state file:

```json
{
  "processed": [
    "/var/log/apache2/access.log.1.gz",
    "/var/log/apache2/access.log.2.gz"
  ],
  "updated_at": "2026-04-29T14:30:00+00:00"
}
```

Files in the state are never re-processed. Delete the state file to re-process everything.

---

## Audit Log

Every converted file is recorded as a JSONL entry:

```json
{"logged_at": "2026-04-29T14:30:01+00:00", "input": "/var/log/apache2/access.log.gz", "output": "/archive/pfc/access.pfc", "converter": "pfc-convert", "rows": 84231, "duration_s": 1.2}
```

---

## Python API

For programmatic integration:

```python
from pfc_ingest_watchdog import Watchdog

wdog = Watchdog(
    mode       = "local",
    converter  = "pfc-convert",
    source     = "/var/log/apache2/",
    output     = "/archive/pfc/",
    options    = {"schema": "apache", "on_error": "skip"},
    state_file = "/var/run/pfc-watchdog/watchdog.state",
    audit_log  = "/var/log/pfc-watchdog/audit.jsonl",
)

# Single scan
converted, failed = wdog.scan_once()

# Continuous loop
wdog.run_loop(poll_interval=30)

# Dry run
converted, failed = wdog.scan_once(dry_run=True)
```

---

## Ecosystem

```
Incoming data                  watchdog                    PFC archive
─────────────────────────────────────────────────────────────────────────
/var/log/apache/*.log  ──────► pfc-convert ──────────────► /archive/*.pfc
s3://bucket/incoming/  ──────► pfc-migrate ──────────────► s3://bucket/pfc/
/data/exports/*.csv    ──────► pfc-convert ──────────────► /archive/*.pfc
```

After conversion, query with:
- **[pfc-duckdb](https://github.com/ImpossibleForge/pfc-duckdb)** — `SELECT * FROM read_pfc_jsonl('archive.pfc')`
- **[pfc-gateway](https://github.com/ImpossibleForge/pfc-gateway)** — REST API with time-range queries
- **[pfc-grafana](https://github.com/ImpossibleForge/pfc-grafana)** — Grafana data source plugin

---

## Part of the PFC Ecosystem

**[→ View all PFC tools & integrations](https://github.com/ImpossibleForge/pfc-jsonl#ecosystem)**

| Direct integration | Why |
|---|---|
| [pfc-convert](https://github.com/ImpossibleForge/pfc-convert) | Schema conversion — converts Apache CLF, CSV, NDJSON → JSONL → .pfc |
| [pfc-migrate](https://github.com/ImpossibleForge/pfc-migrate) | Compression migration — swaps gzip/zstd/lz4 → .pfc, content unchanged |

---

## Pipe mode (without watchdog)

pfc-convert and pfc-migrate can also be combined directly via pipe — no watchdog needed for one-shot conversions:

```bash
# Apache logs → JSONL → .pfc in one streaming step
pfc-convert convert access.log.gz --schema apache --stdout \
  | pfc-migrate convert --stdin --out archive.pfc
```

See [pfc-convert](https://github.com/ImpossibleForge/pfc-convert) and [pfc-migrate](https://github.com/ImpossibleForge/pfc-migrate) for full documentation.

---

## License

pfc-ingest-watchdog (this repository) is released under the MIT License — see [LICENSE](LICENSE).

The PFC-JSONL binary (`pfc_jsonl`) is proprietary software — free for personal and open-source use. Commercial use requires a license: [info@impossibleforge.com](mailto:info@impossibleforge.com)
