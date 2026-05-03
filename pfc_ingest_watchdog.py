#!/usr/bin/env python3
"""
pfc-ingest-watchdog — Automatic file watcher for PFC conversion.

Monitors a local folder or S3 prefix for new non-PFC files and converts
them automatically using either pfc-migrate or pfc-convert.

Tool-agnostic: the converter (pfc-migrate or pfc-convert) is configured
separately — the watchdog only monitors and triggers.

Config file (TOML):
  [watcher]
  mode            = "local"          # "local" or "s3"
  converter       = "pfc-convert"    # "pfc-convert" or "pfc-migrate"
  poll_interval   = 30               # seconds between checks
  state_file      = "watchdog.state" # tracks processed files

  [source]
  path            = "/var/log/apache/"   # local mode
  recursive       = false

  [output]
  path            = "/archive/pfc/"      # local mode

  [converter_options]
  schema          = "auto"           # pfc-convert: auto|apache|nginx|csv|ndjson
  on_error        = "skip"
  output_format   = "pfc"
  timestamp_field = ""               # CSV only, empty = auto-detect

  [s3]                               # s3 mode only
  source_bucket   = "my-logs"
  source_prefix   = "apache/"
  dest_bucket     = "my-pfc"
  dest_prefix     = "pfc/"
  region          = "eu-central-1"

Usage:
  pfc-ingest-watchdog --config watchdog.toml
  pfc-ingest-watchdog --config watchdog.toml --once      # single scan, then exit
  pfc-ingest-watchdog --config watchdog.toml --dry-run   # show what would be converted
"""

__version__ = "0.1.0"

import argparse
import json
import os
import sys
import time
import tempfile
import shutil
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# TOML config loader (stdlib tomllib in Python 3.11+, fallback to tomli)
# ---------------------------------------------------------------------------

def load_toml(path: str) -> dict:
    try:
        import tomllib
        with open(path, "rb") as f:
            return tomllib.load(f)
    except ImportError:
        pass
    try:
        import tomli
        with open(path, "rb") as f:
            return tomli.load(f)
    except ImportError:
        raise ImportError(
            "TOML support requires Python 3.11+ or: pip install tomli"
        )


# ---------------------------------------------------------------------------
# State file — tracks which files have been processed
# ---------------------------------------------------------------------------

class StateStore:
    """JSON-backed set of processed file paths/keys."""

    def __init__(self, path: str):
        self._path = Path(path)
        self._processed: set = set()
        self._load()

    def _load(self):
        if self._path.exists():
            try:
                data = json.loads(self._path.read_text())
                self._processed = set(data.get("processed", []))
            except (json.JSONDecodeError, KeyError):
                self._processed = set()

    def _save(self):
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps({
            "processed": sorted(self._processed),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }, indent=2))

    def is_done(self, key: str) -> bool:
        return key in self._processed

    def mark_done(self, key: str):
        self._processed.add(key)
        self._save()

    def count(self) -> int:
        return len(self._processed)


# ---------------------------------------------------------------------------
# Converter dispatch — calls pfc-convert or pfc-migrate via Python API
# ---------------------------------------------------------------------------

_CONVERTIBLE_EXTS = {
    ".gz", ".zst", ".bz2", ".lz4", ".xz",
    ".log", ".jsonl", ".json", ".ndjson", ".csv", ".tsv",
}

def _is_convertible(filename: str) -> bool:
    """True if this file looks like something we should convert."""
    p = Path(filename)
    # Always skip .pfc and sidecar files
    if p.suffix.lower() in (".pfc", ".bidx", ".idx"):
        return False
    # Skip watchdog internal files by specific suffix patterns
    name_lower = p.name.lower()
    if name_lower.endswith(".state") or name_lower.endswith(".state.json"):
        return False
    if name_lower.endswith("convert_errors.log"):
        return False
    # Accept if any extension matches a known convertible format
    suffixes = {s.lower() for s in p.suffixes}
    return bool(suffixes & _CONVERTIBLE_EXTS)


def _output_path(input_path: Path, output_dir: str) -> Path:
    """Derive .pfc output path for a given input file."""
    name = input_path.name
    for comp in (".gz", ".zst", ".bz2", ".lz4", ".xz"):
        if name.lower().endswith(comp):
            name = name[:-len(comp)]
            break
    for data_ext in (".log", ".jsonl", ".json", ".ndjson", ".csv", ".tsv", ".txt"):
        if name.lower().endswith(data_ext):
            name = name[:-len(data_ext)]
            break
    return Path(output_dir) / (name + ".pfc")


def convert_one_file(
    input_path: str,
    output_path: str,
    converter: str,
    options: dict,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict:
    """
    Convert a single file using the configured tool.

    Returns: {success: bool, rows: int, error: str|None, duration_s: float}
    """
    if dry_run:
        print(f"  [DRY-RUN] Would convert: {input_path} -> {output_path}")
        return {"success": True, "rows": 0, "error": None, "duration_s": 0.0}

    t0 = time.time()

    if converter == "pfc-convert":
        try:
            for _p in [
                str(Path(__file__).parent.parent / "pfc-convert"),
                str(Path(__file__).parent.parent),
                "/root/pfc-convert", "/root",
            ]:
                if (Path(_p) / "pfc_convert.py").exists() and _p not in sys.path:
                    sys.path.insert(0, _p)
            from pfc_convert import ConvertPipeline
        except ImportError:
            raise ImportError(
                "pfc-convert not found. Install it or place pfc_convert.py in the parent directory."
            )
        pipeline = ConvertPipeline(
            source          = input_path,
            destination     = output_path,
            schema          = options.get("schema", "auto"),
            output_format   = options.get("output_format", "pfc"),
            timestamp_field = options.get("timestamp_field") or None,
            on_error        = options.get("on_error", "skip"),
            verbose         = verbose,
        )
        result = pipeline.run()
        return {
            "success":    True,
            "rows":       result.get("rows_ok", 0),
            "error":      None,
            "duration_s": time.time() - t0,
        }

    elif converter == "pfc-migrate":
        try:
            for _p in [
                str(Path(__file__).parent.parent / "pfc-migrate"),
                str(Path(__file__).parent.parent),
                "/root",
            ]:
                if (Path(_p) / "pfc_migrate.py").exists() and _p not in sys.path:
                    sys.path.insert(0, _p)
            from pfc_migrate import convert_file, find_pfc_binary
        except ImportError:
            raise ImportError(
                "pfc-migrate not found. Install it or place pfc_migrate.py in the parent directory."
            )
        pfc_bin = find_pfc_binary(options.get("pfc_binary"))
        if not pfc_bin:
            raise RuntimeError("pfc_jsonl binary not found. Set PFC_JSONL_BINARY env var.")
        result = convert_file(
            input_path, output_path, pfc_bin,
            fmt     = options.get("format") or None,
            verbose = verbose,
        )
        return {
            "success":    True,
            "rows":       0,
            "error":      None,
            "duration_s": time.time() - t0,
        }

    else:
        raise ValueError(f"Unknown converter: {converter!r}. Use 'pfc-convert' or 'pfc-migrate'.")


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

def append_audit(log_path: str, record: dict):
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps({
            "logged_at": datetime.now(timezone.utc).isoformat(),
            **record,
        }) + "\n")


# ---------------------------------------------------------------------------
# Local folder watcher
# ---------------------------------------------------------------------------

def scan_local(
    source_dir: str,
    output_dir: str,
    converter: str,
    options: dict,
    state: StateStore,
    recursive: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
    audit_log: str = None,
) -> tuple:
    """Scan source_dir, convert new files. Returns (converted, failed)."""
    source = Path(source_dir)
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    glob_fn = source.rglob if recursive else source.glob
    files = [f for f in glob_fn("*") if f.is_file() and _is_convertible(f.name)]

    new_files = [f for f in files if not state.is_done(str(f))]
    if not new_files:
        if verbose:
            print(f"  [local] No new files in {source_dir}")
        return 0, 0

    converted = failed = 0
    for f in sorted(new_files):
        out = _output_path(f, output_dir)
        if verbose or dry_run:
            print(f"  -> {f.name}")
        try:
            result = convert_one_file(str(f), str(out), converter, options,
                                      dry_run=dry_run, verbose=verbose)
            if not dry_run:
                state.mark_done(str(f))
            converted += 1
            if audit_log:
                append_audit(audit_log, {
                    "input": str(f), "output": str(out),
                    "converter": converter, "rows": result["rows"],
                    "duration_s": result["duration_s"],
                })
            if verbose:
                print(f"     OK  {result['rows']} rows  {result['duration_s']:.1f}s")
        except Exception as exc:
            failed += 1
            print(f"  ERROR {f.name}: {exc}", file=sys.stderr)
            if audit_log:
                append_audit(audit_log, {
                    "input": str(f), "output": str(out),
                    "converter": converter, "error": str(exc),
                })

    return converted, failed


# ---------------------------------------------------------------------------
# S3 watcher
# ---------------------------------------------------------------------------

def scan_s3(
    s3_config: dict,
    converter: str,
    options: dict,
    state: StateStore,
    dry_run: bool = False,
    verbose: bool = False,
    audit_log: str = None,
) -> tuple:
    """Scan S3 prefix, convert new objects. Returns (converted, failed)."""
    try:
        for _p in [
            str(Path(__file__).parent.parent / "pfc-migrate"),
            str(Path(__file__).parent.parent),
            "/root",
        ]:
            if (Path(_p) / "pfc_migrate.py").exists() and _p not in sys.path:
                sys.path.insert(0, _p)
        from pfc_migrate import get_s3_client, upload_pfc_to_s3
    except ImportError:
        raise ImportError("pfc-migrate not found — required for S3 support.")

    s3 = get_s3_client(
        region       = s3_config.get("region"),
        endpoint_url = s3_config.get("endpoint_url"),
        access_key   = s3_config.get("access_key"),
        secret_key   = s3_config.get("secret_key"),
    )

    src_bucket  = s3_config["source_bucket"]
    src_prefix  = s3_config.get("source_prefix", "")
    dst_bucket  = s3_config.get("dest_bucket", src_bucket)
    dst_prefix  = s3_config.get("dest_prefix", src_prefix)

    paginator = s3.get_paginator("list_objects_v2")
    all_keys = []
    for page in paginator.paginate(Bucket=src_bucket, Prefix=src_prefix):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if _is_convertible(k):
                all_keys.append(k)

    new_keys = [k for k in all_keys if not state.is_done(k)]
    if not new_keys:
        if verbose:
            print(f"  [s3] No new objects under s3://{src_bucket}/{src_prefix}")
        return 0, 0

    converted = failed = 0
    for key in sorted(new_keys):
        src_path = Path(key)
        out_name = _output_path(src_path, ".").name
        out_key  = (dst_prefix.rstrip("/") + "/" + out_name) if dst_prefix else out_name

        if verbose or dry_run:
            print(f"  -> s3://{src_bucket}/{key}")
        if dry_run:
            print(f"     [DRY-RUN] would write s3://{dst_bucket}/{out_key}")
            converted += 1
            continue

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_in  = Path(tmpdir) / src_path.name
            tmp_out = Path(tmpdir) / out_name
            try:
                s3.download_file(src_bucket, key, str(tmp_in))
                result = convert_one_file(str(tmp_in), str(tmp_out), converter, options,
                                          verbose=verbose)
                upload_pfc_to_s3(s3, tmp_out, dst_bucket, out_key)
                state.mark_done(key)
                converted += 1
                if verbose:
                    print(f"     OK  -> s3://{dst_bucket}/{out_key}")
                if audit_log:
                    append_audit(audit_log, {
                        "input":  f"s3://{src_bucket}/{key}",
                        "output": f"s3://{dst_bucket}/{out_key}",
                        "converter": converter,
                        "rows": result["rows"],
                        "duration_s": result["duration_s"],
                    })
            except Exception as exc:
                failed += 1
                print(f"  ERROR {key}: {exc}", file=sys.stderr)
                if audit_log:
                    append_audit(audit_log, {
                        "input": f"s3://{src_bucket}/{key}",
                        "converter": converter, "error": str(exc),
                    })

    return converted, failed


# ---------------------------------------------------------------------------
# Python API
# ---------------------------------------------------------------------------

class Watchdog:
    """
    High-level Python API for pfc-ingest-watchdog.

    Usage:
        from pfc_ingest_watchdog import Watchdog

        wdog = Watchdog(
            mode       = "local",
            converter  = "pfc-convert",
            source     = "/var/log/apache/",
            output     = "/archive/pfc/",
            options    = {"schema": "apache", "on_error": "skip"},
            state_file = "/var/run/watchdog.state",
        )
        converted, failed = wdog.scan_once()
    """

    def __init__(
        self,
        mode: str,
        converter: str,
        source: str = None,
        output: str = None,
        s3_config: dict = None,
        options: dict = None,
        state_file: str = "watchdog.state",
        recursive: bool = False,
        audit_log: str = None,
        verbose: bool = False,
    ):
        self.mode       = mode
        self.converter  = converter
        self.source     = source
        self.output     = output
        self.s3_config  = s3_config or {}
        self.options    = options or {}
        self.state      = StateStore(state_file)
        self.recursive  = recursive
        self.audit_log  = audit_log
        self.verbose    = verbose

    def scan_once(self, dry_run: bool = False) -> tuple:
        """Run one scan. Returns (converted, failed)."""
        if self.mode == "local":
            return scan_local(
                self.source, self.output, self.converter, self.options,
                self.state, recursive=self.recursive,
                dry_run=dry_run, verbose=self.verbose, audit_log=self.audit_log,
            )
        elif self.mode == "s3":
            return scan_s3(
                self.s3_config, self.converter, self.options,
                self.state, dry_run=dry_run, verbose=self.verbose, audit_log=self.audit_log,
            )
        else:
            raise ValueError(f"Unknown mode: {self.mode!r}. Use 'local' or 's3'.")

    def run_loop(self, poll_interval: int = 30, once: bool = False, dry_run: bool = False):
        """Run continuously, polling every poll_interval seconds."""
        print(f"pfc-ingest-watchdog v{__version__} starting")
        print(f"  mode={self.mode}  converter={self.converter}  interval={poll_interval}s")
        if self.mode == "local":
            print(f"  source={self.source}  output={self.output}")
        elif self.mode == "s3":
            b = self.s3_config.get("source_bucket", "?")
            p = self.s3_config.get("source_prefix", "")
            print(f"  source=s3://{b}/{p}")

        total_converted = total_failed = 0

        while True:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[{ts}] Scanning...")
            try:
                converted, failed = self.scan_once(dry_run=dry_run)
                total_converted += converted
                total_failed    += failed
                if converted or failed:
                    print(f"  {converted} converted, {failed} failed  "
                          f"(total: {total_converted} / {self.state.count()} tracked)")
            except Exception as exc:
                print(f"  ERROR during scan: {exc}", file=sys.stderr)

            if once:
                break
            time.sleep(poll_interval)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser():
    parser = argparse.ArgumentParser(
        prog="pfc-ingest-watchdog",
        description="Watch folders or S3 prefixes and auto-convert new files to PFC.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  pfc-ingest-watchdog --config watchdog.toml
  pfc-ingest-watchdog --config watchdog.toml --once
  pfc-ingest-watchdog --config watchdog.toml --dry-run
  pfc-ingest-watchdog --config watchdog.toml --verbose

minimal watchdog.toml:
  [watcher]
  mode      = "local"
  converter = "pfc-convert"

  [source]
  path = "/var/log/apache/"

  [output]
  path = "/archive/pfc/"

  [converter_options]
  schema = "apache"
        """,
    )
    parser.add_argument("--version", action="version", version=f"pfc-ingest-watchdog {__version__}")
    parser.add_argument("--config",    required=True, metavar="FILE", help="Path to TOML config file")
    parser.add_argument("--once",      action="store_true", help="Scan once then exit (no loop)")
    parser.add_argument("--dry-run",   action="store_true", help="Show what would be converted, do nothing")
    parser.add_argument("--verbose",   action="store_true", help="Verbose output")
    return parser


def main(argv=None):
    parser = build_parser()
    args   = parser.parse_args(argv)

    cfg = load_toml(args.config)

    watcher_cfg   = cfg.get("watcher",   {})
    source_cfg    = cfg.get("source",    {})
    output_cfg    = cfg.get("output",    {})
    conv_opts     = cfg.get("converter_options", {})
    s3_cfg        = cfg.get("s3",        {})

    mode          = watcher_cfg.get("mode",          "local")
    converter     = watcher_cfg.get("converter",     "pfc-convert")
    poll_interval = watcher_cfg.get("poll_interval", 30)
    state_file    = watcher_cfg.get("state_file",    "watchdog.state")
    audit_log     = watcher_cfg.get("audit_log",     None)
    recursive     = source_cfg.get("recursive",      False)

    verbose = args.verbose or watcher_cfg.get("verbose", False)

    wdog = Watchdog(
        mode        = mode,
        converter   = converter,
        source      = source_cfg.get("path"),
        output      = output_cfg.get("path"),
        s3_config   = s3_cfg,
        options     = conv_opts,
        state_file  = state_file,
        recursive   = recursive,
        audit_log   = audit_log,
        verbose     = verbose,
    )

    wdog.run_loop(
        poll_interval = poll_interval,
        once          = args.once,
        dry_run       = args.dry_run,
    )


if __name__ == "__main__":
    main()
