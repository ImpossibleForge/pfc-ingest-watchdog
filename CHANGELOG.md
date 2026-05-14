# Changelog — pfc-ingest-watchdog

## v0.1.1 (2026-05-14)

### Enhanced — Test helper renamed for pytest compatibility

Renamed internal helper  to  in both
test files. The previous name caused pytest to pick it up as a test case and report
a spurious "fixture 'name' not found" error — all 37 actual tests still passed, but
the noise was misleading. No functional changes.

## v0.1.0 — 2026-04-29

Initial release.

### Features
- Local folder watching (polling, configurable interval via `poll_interval`)
- S3 prefix watching (polling + state tracking)
- Tool-agnostic: `converter = "pfc-convert"` or `"pfc-migrate"`
- TOML config file
- State file (JSON) — tracks processed files, prevents double-processing
- Audit log (JSONL) — one entry per converted file
- `--once` mode — single scan then exit
- `--dry-run` mode — shows what would be converted, no action
- `--verbose` flag
- Python API: `Watchdog` class with `scan_once()` and `run_loop()`
