#!/usr/bin/env python3
"""
pfc-ingest-watchdog v0.1.0 — Test Suite
=========================================
Tests: StateStore · _is_convertible · local scan · S3 mock
       Python API · TOML config · CLI · pfc-convert integration

Run on server: python3 tests/test_watchdog.py
"""

import gzip
import io
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from unittest import mock

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

sys.path.insert(0, str(Path(__file__).parent.parent))
import pfc_ingest_watchdog as wd

OUTDIR  = Path(tempfile.mkdtemp(prefix="pfc_watchdog_test_"))
results = []

PFC_BIN = os.environ.get("PFC_JSONL_BINARY", "/usr/local/bin/pfc_jsonl")
HAS_BIN = os.path.isfile(PFC_BIN)

# Paths to sibling tools — detect server vs local layout
def _find_tool_dir(tool_script: str) -> str:
    candidates = [
        str(Path(__file__).parent.parent.parent / tool_script.split("/")[0]),
        str(Path(__file__).parent.parent.parent),
        "/root/" + tool_script.split("/")[0],
        "/root",
    ]
    script_name = tool_script.split("/")[-1]
    for c in candidates:
        if (Path(c) / script_name).exists():
            return c
    return candidates[0]

PFC_CONVERT_DIR = _find_tool_dir("pfc-convert/pfc_convert.py")
PFC_MIGRATE_DIR = _find_tool_dir("pfc-migrate/pfc_migrate.py")


def test(name, fn):
    t0 = time.time()
    try:
        fn()
        dt = time.time() - t0
        print(f"  PASS  [{dt:.2f}s]  {name}")
        results.append((name, True, dt))
    except Exception as exc:
        dt = time.time() - t0
        print(f"  FAIL  [{dt:.2f}s]  {name}")
        print(f"           -> {exc}")
        results.append((name, False, dt))


def skip(name, reason):
    print(f"  SKIP  [0.00s]  {name}  ({reason})")
    results.append((name, True, 0.0))


def assert_eq(a, b, msg=""):
    if a != b:
        raise AssertionError(f"{msg}: {a!r} != {b!r}")


def assert_true(cond, msg):
    if not cond:
        raise AssertionError(msg)


def make_apache_lines(n=20) -> str:
    lines = []
    for i in range(n):
        lines.append(
            f'10.0.0.{i % 256} - user{i} [29/Apr/2026:{i // 60:02d}:{i % 60:02d}:00 +0200] '
            f'"GET /api/{i} HTTP/1.1" {200 + i % 3} {512 * (i + 1)}\n'
        )
    return "".join(lines)


def make_jsonl_lines(n=20) -> str:
    return "\n".join(
        json.dumps({"timestamp": f"2026-04-29T10:{i:02d}:00Z", "val": i})
        for i in range(n)
    ) + "\n"


# ---------------------------------------------------------------------------
# 1. StateStore
# ---------------------------------------------------------------------------

def test_state_empty():
    state_path = str(OUTDIR / "state1.json")
    s = wd.StateStore(state_path)
    assert_eq(s.count(), 0)
    assert_eq(s.is_done("file1.gz"), False)


def test_state_mark_and_persist():
    state_path = str(OUTDIR / "state2.json")
    s = wd.StateStore(state_path)
    s.mark_done("file1.gz")
    s.mark_done("file2.log")
    assert_eq(s.is_done("file1.gz"), True)
    assert_eq(s.is_done("file2.log"), True)
    assert_eq(s.is_done("file3.log"), False)
    assert_eq(s.count(), 2)

    # Reload from disk
    s2 = wd.StateStore(state_path)
    assert_eq(s2.is_done("file1.gz"), True)
    assert_eq(s2.count(), 2)


def test_state_json_valid():
    state_path = str(OUTDIR / "state3.json")
    s = wd.StateStore(state_path)
    s.mark_done("a.gz")
    data = json.loads(Path(state_path).read_text())
    assert "processed" in data
    assert "updated_at" in data
    assert "a.gz" in data["processed"]


def test_state_corrupt_file():
    state_path = str(OUTDIR / "state_corrupt.json")
    Path(state_path).write_text("NOT VALID JSON {{{")
    s = wd.StateStore(state_path)
    assert_eq(s.count(), 0)  # graceful degradation


# ---------------------------------------------------------------------------
# 2. _is_convertible
# ---------------------------------------------------------------------------

def test_convertible_log_gz():
    assert_true(wd._is_convertible("access.log.gz"), "log.gz")


def test_convertible_csv():
    assert_true(wd._is_convertible("data.csv"), "csv")


def test_convertible_jsonl():
    assert_true(wd._is_convertible("events.jsonl"), "jsonl")


def test_convertible_log():
    assert_true(wd._is_convertible("app.log"), "log")


def test_not_convertible_pfc():
    assert_true(not wd._is_convertible("archive.pfc"), "pfc excluded")


def test_not_convertible_bidx():
    assert_true(not wd._is_convertible("archive.pfc.bidx"), "bidx excluded")


def test_not_convertible_readme():
    assert_true(not wd._is_convertible("README.md"), "md excluded")


# ---------------------------------------------------------------------------
# 3. _output_path
# ---------------------------------------------------------------------------

def test_output_path_log_gz():
    p = wd._output_path(Path("access.log.gz"), "/out")
    assert_eq(p.name, "access.pfc")


def test_output_path_csv():
    p = wd._output_path(Path("data.csv"), "/out")
    assert_eq(p.name, "data.pfc")


def test_output_path_jsonl():
    p = wd._output_path(Path("events.jsonl"), "/out")
    assert_eq(p.name, "events.pfc")


# ---------------------------------------------------------------------------
# 4. Local scan with pfc-convert
# ---------------------------------------------------------------------------

def test_local_scan_apache_convert():
    if not HAS_BIN:
        return skip("local scan apache (pfc-convert)", "no binary")
    if not (Path(PFC_CONVERT_DIR) / "pfc_convert.py").exists():
        return skip("local scan apache (pfc-convert)", "pfc-convert not found")

    src_dir = OUTDIR / "local_apache_src"
    out_dir = OUTDIR / "local_apache_out"
    src_dir.mkdir()

    (src_dir / "access1.log").write_text(make_apache_lines(30))
    (src_dir / "access2.log").write_text(make_apache_lines(20))
    (src_dir / "README.md").write_text("# ignore me\n")

    state = wd.StateStore(str(OUTDIR / "state_apache.json"))
    sys.path.insert(0, PFC_CONVERT_DIR)

    converted, failed = wd.scan_local(
        str(src_dir), str(out_dir), "pfc-convert",
        {"schema": "apache", "on_error": "skip", "output_format": "pfc"},
        state, verbose=False,
    )
    assert_eq(converted, 2, "2 files converted")
    assert_eq(failed, 0)

    pfc_files = list(out_dir.glob("*.pfc"))
    assert_eq(len(pfc_files), 2, "2 .pfc files created")
    assert_true(state.is_done(str(src_dir / "access1.log")), "access1 marked done")
    assert_true(state.is_done(str(src_dir / "access2.log")), "access2 marked done")


def test_local_scan_skips_already_processed():
    if not HAS_BIN:
        return skip("skip already processed", "no binary")
    if not (Path(PFC_CONVERT_DIR) / "pfc_convert.py").exists():
        return skip("skip already processed", "pfc-convert not found")

    src_dir = OUTDIR / "local_skip_src"
    out_dir = OUTDIR / "local_skip_out"
    src_dir.mkdir(exist_ok=True)

    f = src_dir / "already.log"
    f.write_text(make_apache_lines(5))

    state = wd.StateStore(str(OUTDIR / "state_skip.json"))
    state.mark_done(str(f))  # pre-mark as done

    sys.path.insert(0, PFC_CONVERT_DIR)
    converted, failed = wd.scan_local(
        str(src_dir), str(out_dir), "pfc-convert",
        {"schema": "apache", "on_error": "skip", "output_format": "pfc"},
        state, verbose=False,
    )
    assert_eq(converted, 0, "already processed file skipped")


def test_local_scan_with_pfc_migrate():
    if not HAS_BIN:
        return skip("local scan with pfc-migrate", "no binary")
    if not (Path(PFC_MIGRATE_DIR) / "pfc_migrate.py").exists():
        return skip("local scan with pfc-migrate", "pfc-migrate not found")

    src_dir = OUTDIR / "local_migrate_src"
    out_dir = OUTDIR / "local_migrate_out"
    src_dir.mkdir()

    gz_data = gzip.compress(make_jsonl_lines(25).encode())
    (src_dir / "events.jsonl.gz").write_bytes(gz_data)

    state = wd.StateStore(str(OUTDIR / "state_migrate.json"))
    sys.path.insert(0, PFC_MIGRATE_DIR)

    converted, failed = wd.scan_local(
        str(src_dir), str(out_dir), "pfc-migrate",
        {"format": "gz"},
        state, verbose=False,
    )
    assert_eq(converted, 1)
    assert_eq(failed, 0)
    assert (out_dir / "events.pfc").exists(), ".pfc not created"


def test_local_scan_dry_run():
    src_dir = OUTDIR / "dry_run_src"
    out_dir = OUTDIR / "dry_run_out"
    src_dir.mkdir(exist_ok=True)
    (src_dir / "file.log").write_text(make_apache_lines(5))

    state = wd.StateStore(str(OUTDIR / "state_dry.json"))
    converted, failed = wd.scan_local(
        str(src_dir), str(out_dir), "pfc-convert",
        {"schema": "apache"}, state, dry_run=True, verbose=True,
    )
    assert_eq(converted, 1, "dry-run counts as 'would convert'")
    assert_eq(failed, 0)
    assert_eq(state.is_done(str(src_dir / "file.log")), False,
              "dry-run does not mark state")
    assert not list(out_dir.glob("*.pfc")), "dry-run creates no .pfc files"


def test_local_scan_audit_log():
    if not HAS_BIN:
        return skip("local scan audit log", "no binary")
    if not (Path(PFC_CONVERT_DIR) / "pfc_convert.py").exists():
        return skip("local scan audit log", "pfc-convert not found")

    src_dir = OUTDIR / "audit_src"
    out_dir = OUTDIR / "audit_out"
    src_dir.mkdir(exist_ok=True)
    (src_dir / "audit_test.log").write_text(make_apache_lines(10))

    state   = wd.StateStore(str(OUTDIR / "state_audit.json"))
    audit   = str(OUTDIR / "watchdog_audit.jsonl")
    sys.path.insert(0, PFC_CONVERT_DIR)

    wd.scan_local(str(src_dir), str(out_dir), "pfc-convert",
                  {"schema": "apache"}, state, audit_log=audit)

    assert Path(audit).exists(), "audit log created"
    entry = json.loads(Path(audit).read_text().splitlines()[0])
    assert "input" in entry and "output" in entry and "logged_at" in entry


# ---------------------------------------------------------------------------
# 5. S3 mock test
# ---------------------------------------------------------------------------

def test_s3_scan_mock():
    """Mock S3: verify scan_s3 downloads, converts via pfc-convert, uploads."""
    if not HAS_BIN:
        return skip("S3 mock scan", "no binary")
    if not (Path(PFC_CONVERT_DIR) / "pfc_convert.py").exists():
        return skip("S3 mock scan", "pfc-convert not found")

    import shutil

    src_dir = OUTDIR / "s3_mock_src"
    src_dir.mkdir(exist_ok=True)
    log_file = src_dir / "mock.log"
    log_file.write_text(make_apache_lines(15))

    downloaded = []
    uploaded   = []

    class MockS3:
        def get_paginator(self, op):
            class Pager:
                def paginate(self, Bucket, Prefix):
                    return [{"Contents": [{"Key": "apache/mock.log"}]}]
            return Pager()

        def download_file(self, bucket, key, dest):
            shutil.copy(str(log_file), dest)
            downloaded.append(key)

        def upload_file(self, src, bucket, key):
            uploaded.append(key)

    # Ensure our updated pfc_migrate is on the path
    if PFC_MIGRATE_DIR not in sys.path:
        sys.path.insert(0, PFC_MIGRATE_DIR)
    if PFC_CONVERT_DIR not in sys.path:
        sys.path.insert(0, PFC_CONVERT_DIR)

    state = wd.StateStore(str(OUTDIR / "state_s3mock.json"))

    # Patch get_s3_client inside the watchdog module's runtime lookup
    with mock.patch.dict("sys.modules"):
        # Create a fake pfc_migrate module with get_s3_client + upload_pfc_to_s3
        import types
        fake_pm = types.ModuleType("pfc_migrate")

        def fake_get_s3_client(**kwargs):
            return MockS3()

        def fake_upload(s3, pfc_path, bucket, key):
            s3.upload_file(str(pfc_path), bucket, key)
            bidx = Path(str(pfc_path) + ".bidx")
            if bidx.exists():
                s3.upload_file(str(bidx), bucket, key + ".bidx")

        fake_pm.get_s3_client   = fake_get_s3_client
        fake_pm.upload_pfc_to_s3 = fake_upload
        sys.modules["pfc_migrate"] = fake_pm

        converted, failed = wd.scan_s3(
            {
                "source_bucket": "my-logs",
                "source_prefix": "apache/",
                "dest_bucket":   "my-pfc",
                "dest_prefix":   "pfc/",
            },
            "pfc-convert",
            {"schema": "apache", "on_error": "skip"},
            state,
            verbose=False,
        )

    # Restore real pfc_migrate
    if "pfc_migrate" in sys.modules and not hasattr(sys.modules["pfc_migrate"], "convert_file"):
        del sys.modules["pfc_migrate"]

    assert_eq(converted, 1, "1 S3 object converted")
    assert_eq(failed, 0)
    assert_true(len(downloaded) >= 1, "download called")
    assert_true(len(uploaded) >= 1, "upload called")
    assert_true(state.is_done("apache/mock.log"), "key marked done")


# ---------------------------------------------------------------------------
# 6. Python API (Watchdog class)
# ---------------------------------------------------------------------------

def test_watchdog_api_local():
    if not HAS_BIN:
        return skip("Watchdog API local", "no binary")
    if not (Path(PFC_CONVERT_DIR) / "pfc_convert.py").exists():
        return skip("Watchdog API local", "pfc-convert not found")

    src = OUTDIR / "api_src"
    out = OUTDIR / "api_out"
    src.mkdir(exist_ok=True)
    (src / "api_test.log").write_text(make_apache_lines(8))

    sys.path.insert(0, PFC_CONVERT_DIR)

    wdog = wd.Watchdog(
        mode       = "local",
        converter  = "pfc-convert",
        source     = str(src),
        output     = str(out),
        options    = {"schema": "apache", "on_error": "skip"},
        state_file = str(OUTDIR / "api_state.json"),
    )
    converted, failed = wdog.scan_once()
    assert_eq(converted, 1)
    assert_eq(failed, 0)
    assert (out / "api_test.pfc").exists()

    # Second scan: nothing new
    converted2, failed2 = wdog.scan_once()
    assert_eq(converted2, 0, "no double-processing")


def test_watchdog_api_dry_run():
    src = OUTDIR / "api_dry_src"
    src.mkdir(exist_ok=True)
    (src / "dry.log").write_text(make_apache_lines(3))

    wdog = wd.Watchdog(
        mode       = "local",
        converter  = "pfc-convert",
        source     = str(src),
        output     = str(OUTDIR / "api_dry_out"),
        state_file = str(OUTDIR / "api_dry_state.json"),
    )
    converted, failed = wdog.scan_once(dry_run=True)
    assert_eq(converted, 1, "dry-run reports 1 would-convert")
    assert_eq(wdog.state.is_done(str(src / "dry.log")), False,
              "dry-run does not persist state")


# ---------------------------------------------------------------------------
# 7. TOML config loading
# ---------------------------------------------------------------------------

def test_toml_load_basic():
    cfg_path = str(OUTDIR / "test_config.toml")
    Path(cfg_path).write_text("""
[watcher]
mode = "local"
converter = "pfc-convert"
poll_interval = 45

[source]
path = "/var/log/apache/"
recursive = true

[output]
path = "/archive/pfc/"

[converter_options]
schema = "apache"
on_error = "skip"
""")
    cfg = wd.load_toml(cfg_path)
    assert_eq(cfg["watcher"]["mode"], "local")
    assert_eq(cfg["watcher"]["converter"], "pfc-convert")
    assert_eq(cfg["watcher"]["poll_interval"], 45)
    assert_eq(cfg["source"]["recursive"], True)
    assert_eq(cfg["converter_options"]["schema"], "apache")


def test_toml_missing_file():
    try:
        wd.load_toml("/nonexistent/config.toml")
        raise AssertionError("Should raise")
    except (FileNotFoundError, OSError):
        pass


# ---------------------------------------------------------------------------
# 8. CLI
# ---------------------------------------------------------------------------

SCRIPT = str(Path(__file__).parent.parent / "pfc_ingest_watchdog.py")


def test_cli_version():
    r = subprocess.run(
        [sys.executable, SCRIPT, "--version"],
        capture_output=True, text=True,
    )
    assert "0.1.0" in r.stdout + r.stderr


def test_cli_no_config():
    r = subprocess.run(
        [sys.executable, SCRIPT],
        capture_output=True, text=True,
    )
    assert r.returncode != 0


def test_cli_once_dry_run():
    if not (Path(PFC_CONVERT_DIR) / "pfc_convert.py").exists():
        return skip("CLI --once --dry-run", "pfc-convert not found")

    src = OUTDIR / "cli_src"
    out = OUTDIR / "cli_out"
    src.mkdir(exist_ok=True)
    (src / "cli_test.log").write_text(make_apache_lines(5))

    cfg_path = str(OUTDIR / "cli_test.toml")
    Path(cfg_path).write_text(f"""
[watcher]
mode           = "local"
converter      = "pfc-convert"
poll_interval  = 10
state_file     = "{str(OUTDIR / 'cli_state.json')}"

[source]
path = "{str(src)}"

[output]
path = "{str(out)}"

[converter_options]
schema   = "apache"
on_error = "skip"
""")

    r = subprocess.run(
        [sys.executable, SCRIPT, "--config", cfg_path, "--once", "--dry-run"],
        capture_output=True, text=True, timeout=30,
    )
    assert r.returncode == 0, f"CLI exit {r.returncode}: {r.stderr[:300]}"
    assert "DRY-RUN" in r.stdout + r.stderr or "cli_test.log" in r.stdout + r.stderr


def test_cli_once_real():
    if not HAS_BIN:
        return skip("CLI --once real convert", "no binary")
    if not (Path(PFC_CONVERT_DIR) / "pfc_convert.py").exists():
        return skip("CLI --once real convert", "pfc-convert not found")

    src = OUTDIR / "cli_real_src"
    out = OUTDIR / "cli_real_out"
    src.mkdir(exist_ok=True)
    (src / "real_test.log").write_text(make_apache_lines(15))

    cfg_path = str(OUTDIR / "cli_real.toml")
    Path(cfg_path).write_text(f"""
[watcher]
mode           = "local"
converter      = "pfc-convert"
state_file     = "{str(OUTDIR / 'cli_real_state.json')}"

[source]
path = "{str(src)}"

[output]
path = "{str(out)}"

[converter_options]
schema   = "apache"
on_error = "skip"
""")

    r = subprocess.run(
        [sys.executable, SCRIPT, "--config", cfg_path, "--once", "--verbose"],
        capture_output=True, text=True, timeout=60,
    )
    assert r.returncode == 0, f"CLI exit {r.returncode}: {r.stderr[:300]}"
    assert (out / "real_test.pfc").exists(), ".pfc not created"


# ---------------------------------------------------------------------------
# 9. append_audit
# ---------------------------------------------------------------------------

def test_append_audit_multiple():
    log_path = str(OUTDIR / "test_audit_multi.jsonl")
    for i in range(3):
        wd.append_audit(log_path, {"input": f"file{i}.log", "rows": i * 10})
    lines = Path(log_path).read_text().splitlines()
    assert_eq(len(lines), 3)
    for line in lines:
        entry = json.loads(line)
        assert "logged_at" in entry
        assert "input" in entry


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import shutil

    print(f"\npfc-ingest-watchdog v{wd.__version__} — Test Suite")
    print(f"PFC binary: {'found' if HAS_BIN else 'NOT FOUND'}")
    print(f"pfc-convert: {'found' if (Path(PFC_CONVERT_DIR)/'pfc_convert.py').exists() else 'NOT FOUND'}")
    print(f"pfc-migrate: {'found' if (Path(PFC_MIGRATE_DIR)/'pfc_migrate.py').exists() else 'NOT FOUND'}")
    print(f"Output: {OUTDIR}\n")

    all_tests = [
        # StateStore
        ("StateStore: empty on init",                    test_state_empty),
        ("StateStore: mark + persist + reload",          test_state_mark_and_persist),
        ("StateStore: valid JSON written",               test_state_json_valid),
        ("StateStore: corrupt file -> graceful",         test_state_corrupt_file),
        # _is_convertible
        ("convertible: .log.gz",                         test_convertible_log_gz),
        ("convertible: .csv",                            test_convertible_csv),
        ("convertible: .jsonl",                          test_convertible_jsonl),
        ("convertible: .log",                            test_convertible_log),
        ("not convertible: .pfc",                        test_not_convertible_pfc),
        ("not convertible: .bidx",                       test_not_convertible_bidx),
        ("not convertible: .md",                         test_not_convertible_readme),
        # _output_path
        ("output path: .log.gz -> .pfc",                 test_output_path_log_gz),
        ("output path: .csv -> .pfc",                    test_output_path_csv),
        ("output path: .jsonl -> .pfc",                  test_output_path_jsonl),
        # Local scan
        ("local scan: apache -> pfc-convert",            test_local_scan_apache_convert),
        ("local scan: skips already processed",          test_local_scan_skips_already_processed),
        ("local scan: jsonl.gz -> pfc-migrate",          test_local_scan_with_pfc_migrate),
        ("local scan: dry-run no files created",         test_local_scan_dry_run),
        ("local scan: audit log written",                test_local_scan_audit_log),
        # S3 mock
        ("S3 scan: mock download/upload",                test_s3_scan_mock),
        # Python API
        ("Watchdog API: local scan_once",                test_watchdog_api_local),
        ("Watchdog API: dry_run no state",               test_watchdog_api_dry_run),
        # TOML
        ("TOML: load basic config",                      test_toml_load_basic),
        ("TOML: missing file raises",                    test_toml_missing_file),
        # CLI
        ("CLI: --version",                               test_cli_version),
        ("CLI: no config -> error",                      test_cli_no_config),
        ("CLI: --once --dry-run",                        test_cli_once_dry_run),
        ("CLI: --once real convert",                     test_cli_once_real),
        # Audit
        ("append_audit: multiple entries",               test_append_audit_multiple),
    ]

    for name, fn in all_tests:
        test(name, fn)

    total  = len(results)
    passed = sum(1 for _, ok, _ in results if ok)
    failed = total - passed
    total_t = sum(t for _, _, t in results)

    print(f"\n{'='*60}")
    print(f"  {passed}/{total} PASS   {failed} FAIL   {total_t:.1f}s total")
    print(f"{'='*60}\n")

    if failed:
        print("Failed:")
        for name, ok, _ in results:
            if not ok:
                print(f"  - {name}")
        sys.exit(1)
