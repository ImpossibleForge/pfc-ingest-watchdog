#!/usr/bin/env python3
"""
PFC Ecosystem — Integration Test
==================================
Tests the full chain: pfc-convert + pfc-migrate + pfc-ingest-watchdog
working together end-to-end.

Scenarios:
  1. pfc-convert --stdout | pfc-migrate --stdin  (pipe chain)
  2. Watchdog → pfc-convert (Apache logs, local)
  3. Watchdog → pfc-migrate (JSONL.gz, local)
  4. Watchdog detects, converts, second scan skips (state persistence)
  5. Mixed folder: apache + csv + jsonl.gz — watchdog handles all
  6. Full roundtrip: convert → decompress → verify field integrity

Run on server: python3 tests/test_integration.py
Requires: pfc_jsonl binary
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

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Locate tools
SELF_DIR = Path(__file__).parent.parent

def _find(script):
    for c in [SELF_DIR.parent / script.split("/")[0], SELF_DIR.parent, Path("/root"), Path("/root") / script.split("/")[0]]:
        if (c / script.split("/")[-1]).exists():
            return str(c)
    return None

CONVERT_DIR = _find("pfc-convert/pfc_convert.py")
MIGRATE_DIR = _find("pfc-migrate/pfc_migrate.py")
WATCHDOG_SCRIPT = str(SELF_DIR / "pfc_ingest_watchdog.py")

sys.path.insert(0, str(SELF_DIR))
if CONVERT_DIR: sys.path.insert(0, CONVERT_DIR)
if MIGRATE_DIR: sys.path.insert(0, MIGRATE_DIR)

CONVERT_SCRIPT = str(Path(CONVERT_DIR) / "pfc_convert.py") if CONVERT_DIR else None
MIGRATE_SCRIPT = str(Path(MIGRATE_DIR) / "pfc_migrate.py") if MIGRATE_DIR else None
PFC_BIN        = os.environ.get("PFC_JSONL_BINARY", "/usr/local/bin/pfc_jsonl")
HAS_BIN        = os.path.isfile(PFC_BIN)
HAS_CONVERT    = CONVERT_DIR and (Path(CONVERT_DIR) / "pfc_convert.py").exists()
HAS_MIGRATE    = MIGRATE_DIR and (Path(MIGRATE_DIR) / "pfc_migrate.py").exists()

OUTDIR = Path(tempfile.mkdtemp(prefix="pfc_integration_"))
results = []


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
        import traceback; traceback.print_exc()
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


# ---------------------------------------------------------------------------
# Data generators
# ---------------------------------------------------------------------------

def apache_lines(n):
    lines = []
    for i in range(n):
        lines.append(
            f'10.0.{i//256}.{i%256} - user{i} '
            f'[29/Apr/2026:{i//3600%24:02d}:{i//60%60:02d}:{i%60:02d} +0200] '
            f'"GET /api/item/{i} HTTP/1.1" {200+i%5} {512*(i%20+1)} '
            f'"-" "TestAgent/1.0"\n'
        )
    return "".join(lines)


def csv_lines(n):
    rows = ["timestamp,service,status,latency_ms\n"]
    for i in range(n):
        rows.append(f"2026-04-29T{i//3600%24:02d}:{i//60%60:02d}:{i%60:02d}Z,svc-{i%5},{200+i%3},{i%300}\n")
    return "".join(rows)


def jsonl_lines(n):
    return "\n".join(
        json.dumps({"timestamp": f"2026-04-29T10:{i//60%60:02d}:{i%60:02d}Z",
                    "level": "INFO", "service": f"svc-{i%3}", "val": i})
        for i in range(n)
    ) + "\n"


def decompress_pfc(pfc_path, out_path):
    r = subprocess.run([PFC_BIN, "decompress", str(pfc_path), str(out_path)],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"decompress failed: {r.stderr[:200]}")


def read_jsonl(path):
    return [json.loads(l) for l in Path(path).read_text().splitlines() if l.strip()]


# ===========================================================================
# 1. PIPE CHAIN: pfc-convert --stdout | pfc-migrate --stdin
# ===========================================================================

def test_pipe_apache_convert_then_migrate():
    """Full pipe: pfc-convert (apache) -> stdout -> pfc-migrate --stdin -> .pfc"""
    if not (HAS_BIN and HAS_CONVERT and HAS_MIGRATE):
        return skip("pipe: convert|migrate", f"missing tools/binary")

    n = 100
    src = OUTDIR / "pipe_apache.log"
    src.write_text(apache_lines(n))
    out_pfc  = OUTDIR / "pipe_apache_out.pfc"
    out_dec  = OUTDIR / "pipe_apache_dec.jsonl"

    p_conv = subprocess.Popen(
        [sys.executable, CONVERT_SCRIPT, "convert", str(src), "--schema", "apache", "--stdout"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    p_migr = subprocess.Popen(
        [sys.executable, MIGRATE_SCRIPT, "convert", "--stdin", "--out", str(out_pfc)],
        stdin=p_conv.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    p_conv.stdout.close()
    _, migr_err = p_migr.communicate(timeout=60)
    p_conv.wait()

    assert_eq(p_migr.returncode, 0, f"pipe exit: {migr_err.decode()[:200]}")
    assert_true(out_pfc.exists(), ".pfc created via pipe")
    assert_true((Path(str(out_pfc) + ".bidx")).exists(), ".bidx created")

    decompress_pfc(out_pfc, out_dec)
    recs = read_jsonl(out_dec)
    assert_eq(len(recs), n, "pipe: row count matches")
    assert_true(all("timestamp" in r and "ip" in r and "status" in r for r in recs[:5]),
                "pipe: JSONL fields correct")
    print(f"     {n} rows, .pfc + .bidx OK")


def test_pipe_csv_convert_then_migrate():
    """Full pipe: pfc-convert (csv) -> stdout -> pfc-migrate --stdin -> .pfc"""
    if not (HAS_BIN and HAS_CONVERT and HAS_MIGRATE):
        return skip("pipe: csv convert|migrate", "missing tools/binary")

    n = 80
    src = OUTDIR / "pipe_csv.csv"
    src.write_text(csv_lines(n))
    out_pfc = OUTDIR / "pipe_csv_out.pfc"
    out_dec = OUTDIR / "pipe_csv_dec.jsonl"

    p_conv = subprocess.Popen(
        [sys.executable, CONVERT_SCRIPT, "convert", str(src), "--schema", "csv", "--stdout"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    p_migr = subprocess.Popen(
        [sys.executable, MIGRATE_SCRIPT, "convert", "--stdin", "--out", str(out_pfc)],
        stdin=p_conv.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    p_conv.stdout.close()
    _, err = p_migr.communicate(timeout=60)
    p_conv.wait()

    assert_eq(p_migr.returncode, 0, f"csv pipe exit: {err.decode()[:200]}")
    decompress_pfc(out_pfc, out_dec)
    recs = read_jsonl(out_dec)
    assert_eq(len(recs), n, "csv pipe row count")
    assert_true("timestamp" in recs[0], "csv pipe: timestamp field present")
    assert_true("service" in recs[0], "csv pipe: data fields present")
    print(f"     {n} rows, fields: {list(recs[0].keys())}")


# ===========================================================================
# 2. WATCHDOG → pfc-convert (Apache)
# ===========================================================================

def test_watchdog_apache_convert():
    """Watchdog detects new .log files and converts with pfc-convert."""
    if not (HAS_BIN and HAS_CONVERT):
        return skip("watchdog -> pfc-convert apache", "missing tools/binary")

    src = OUTDIR / "wdog_apache_src"
    out = OUTDIR / "wdog_apache_out"
    src.mkdir()

    (src / "access1.log").write_text(apache_lines(50))
    (src / "access2.log").write_text(apache_lines(30))
    (src / ".gitkeep").write_text("")  # should be ignored

    cfg = OUTDIR / "wdog_apache.toml"
    state = OUTDIR / "wdog_apache.state"
    cfg.write_text(f"""
[watcher]
mode       = "local"
converter  = "pfc-convert"
state_file = "{state}"

[source]
path = "{src}"

[output]
path = "{out}"

[converter_options]
schema   = "apache"
on_error = "skip"
""")

    r = subprocess.run(
        [sys.executable, WATCHDOG_SCRIPT, "--config", str(cfg), "--once", "--verbose"],
        capture_output=True, text=True, timeout=120,
    )
    assert_eq(r.returncode, 0, f"watchdog exit: {r.stderr[:300]}")

    pfc_files = list(out.glob("*.pfc"))
    assert_eq(len(pfc_files), 2, f"2 .pfc files, got {[f.name for f in pfc_files]}")

    # Verify row counts via decompress
    total_rows = 0
    for pfc in pfc_files:
        dec = OUTDIR / (pfc.stem + "_dec.jsonl")
        decompress_pfc(pfc, dec)
        total_rows += len(read_jsonl(dec))
    assert_eq(total_rows, 80, f"total rows: {total_rows}")
    print(f"     2 files, 80 rows total, all .pfc OK")


# ===========================================================================
# 3. WATCHDOG → pfc-migrate (JSONL.gz)
# ===========================================================================

def test_watchdog_jsonl_migrate():
    """Watchdog detects new .jsonl.gz and converts with pfc-migrate."""
    if not (HAS_BIN and HAS_MIGRATE):
        return skip("watchdog -> pfc-migrate jsonl", "missing tools/binary")

    src = OUTDIR / "wdog_migrate_src"
    out = OUTDIR / "wdog_migrate_out"
    src.mkdir()

    (src / "events_a.jsonl.gz").write_bytes(gzip.compress(jsonl_lines(40).encode()))
    (src / "events_b.jsonl.gz").write_bytes(gzip.compress(jsonl_lines(60).encode()))

    cfg   = OUTDIR / "wdog_migrate.toml"
    state = OUTDIR / "wdog_migrate.state"
    cfg.write_text(f"""
[watcher]
mode       = "local"
converter  = "pfc-migrate"
state_file = "{state}"

[source]
path = "{src}"

[output]
path = "{out}"

[converter_options]
format = "gz"
""")

    r = subprocess.run(
        [sys.executable, WATCHDOG_SCRIPT, "--config", str(cfg), "--once"],
        capture_output=True, text=True, timeout=120,
    )
    assert_eq(r.returncode, 0, f"watchdog exit: {r.stderr[:300]}")

    pfc_files = list(out.glob("*.pfc"))
    assert_eq(len(pfc_files), 2, "2 .pfc files")

    total = 0
    for pfc in pfc_files:
        dec = OUTDIR / (pfc.stem + "_mig_dec.jsonl")
        decompress_pfc(pfc, dec)
        total += len(read_jsonl(dec))
    assert_eq(total, 100, f"total rows via migrate: {total}")
    print(f"     2 files, 100 rows, migrate roundtrip OK")


# ===========================================================================
# 4. STATE PERSISTENCE — second scan skips already-processed files
# ===========================================================================

def test_state_persistence_no_double_process():
    """Second watchdog scan skips files already in state."""
    if not (HAS_BIN and HAS_CONVERT):
        return skip("state persistence", "missing tools/binary")

    src = OUTDIR / "state_persist_src"
    out = OUTDIR / "state_persist_out"
    src.mkdir()
    (src / "first.log").write_text(apache_lines(10))

    cfg   = OUTDIR / "state_persist.toml"
    state = OUTDIR / "state_persist.json"
    cfg.write_text(f"""
[watcher]
mode       = "local"
converter  = "pfc-convert"
state_file = "{state}"

[source]
path = "{src}"

[output]
path = "{out}"

[converter_options]
schema = "apache"
""")

    # First scan
    r1 = subprocess.run(
        [sys.executable, WATCHDOG_SCRIPT, "--config", str(cfg), "--once"],
        capture_output=True, text=True, timeout=60,
    )
    assert_eq(r1.returncode, 0, "first scan OK")
    assert_true((out / "first.pfc").exists(), "first.pfc created")

    # Add new file
    (src / "second.log").write_text(apache_lines(15))

    # Second scan — should only process second.log
    r2 = subprocess.run(
        [sys.executable, WATCHDOG_SCRIPT, "--config", str(cfg), "--once", "--verbose"],
        capture_output=True, text=True, timeout=60,
    )
    assert_eq(r2.returncode, 0, "second scan OK")

    # Verify state has both files
    import json as _json
    state_data = _json.loads(Path(state).read_text())
    processed = state_data["processed"]
    assert_true(any("first.log" in p for p in processed), "first.log in state")
    assert_true(any("second.log" in p for p in processed), "second.log in state")
    assert_eq(len(processed), 2, "exactly 2 files in state")
    print(f"     State: {len(processed)} files tracked, no double-processing")


# ===========================================================================
# 5. MIXED FOLDER — apache + csv + jsonl.gz
# ===========================================================================

def test_mixed_folder_all_formats():
    """Watchdog converts mixed folder: apache .log, .csv, .jsonl.gz."""
    if not (HAS_BIN and HAS_CONVERT):
        return skip("mixed folder", "missing tools/binary")

    src = OUTDIR / "mixed_src"
    out = OUTDIR / "mixed_out"
    src.mkdir()

    (src / "apache.log").write_text(apache_lines(25))
    (src / "data.csv").write_text(csv_lines(20))
    (src / "events.jsonl.gz").write_bytes(gzip.compress(jsonl_lines(30).encode()))
    (src / "README.md").write_text("# should be ignored\n")
    (src / "archive.pfc").write_bytes(b"fake pfc - should be ignored")

    cfg   = OUTDIR / "mixed.toml"
    state = OUTDIR / "mixed.state"
    cfg.write_text(f"""
[watcher]
mode       = "local"
converter  = "pfc-convert"
state_file = "{state}"

[source]
path = "{src}"

[output]
path = "{out}"

[converter_options]
schema   = "auto"
on_error = "skip"
""")

    r = subprocess.run(
        [sys.executable, WATCHDOG_SCRIPT, "--config", str(cfg), "--once", "--verbose"],
        capture_output=True, text=True, timeout=120,
    )
    assert_eq(r.returncode, 0, f"mixed exit: {r.stderr[:300]}")

    pfc_files = list(out.glob("*.pfc"))
    assert_eq(len(pfc_files), 3, f"3 .pfc files, got {[f.name for f in pfc_files]}")

    total = 0
    for pfc in sorted(pfc_files):
        dec = OUTDIR / (pfc.stem + "_mix_dec.jsonl")
        decompress_pfc(pfc, dec)
        rows = read_jsonl(dec)
        total += len(rows)
        print(f"     {pfc.name}: {len(rows)} rows")
    assert_eq(total, 75, f"total rows: {total} (expected 25+20+30)")


# ===========================================================================
# 6. FULL ROUNDTRIP — field integrity verification
# ===========================================================================

def test_full_roundtrip_field_integrity():
    """Convert apache log -> .pfc -> decompress -> verify each field is correct."""
    if not (HAS_BIN and HAS_CONVERT):
        return skip("field integrity roundtrip", "missing tools/binary")

    n = 50
    src = OUTDIR / "integrity_src.log"
    pfc = OUTDIR / "integrity.pfc"
    dec = OUTDIR / "integrity_dec.jsonl"

    src.write_text(apache_lines(n))

    r = subprocess.run(
        [sys.executable, CONVERT_SCRIPT, "convert", str(src),
         "--schema", "apache", "--out", str(pfc)],
        capture_output=True, text=True, timeout=60,
    )
    assert_eq(r.returncode, 0, f"convert exit: {r.stderr[:200]}")
    decompress_pfc(pfc, dec)
    recs = read_jsonl(dec)
    assert_eq(len(recs), n)

    for i, rec in enumerate(recs):
        # Every record must have these fields
        for field in ("timestamp", "ip", "method", "path", "status", "bytes"):
            assert field in rec, f"record {i} missing field '{field}'"

        # Timestamp must be ISO 8601
        ts = rec["timestamp"]
        assert "2026" in ts and "T" in ts, f"record {i} bad timestamp: {ts}"

        # IP format: 10.0.x.y
        assert rec["ip"].startswith("10.0."), f"record {i} bad IP: {rec['ip']}"

        # Method must be GET
        assert_eq(rec["method"], "GET", f"record {i} method")

        # Status must be 200-204
        assert 200 <= rec["status"] <= 204, f"record {i} status: {rec['status']}"

        # Bytes must be positive
        assert rec["bytes"] > 0, f"record {i} bytes: {rec['bytes']}"

    print(f"     {n} records, all fields valid")


# ===========================================================================
# 7. WATCHDOG AUDIT LOG — end-to-end
# ===========================================================================

def test_watchdog_audit_log_entries():
    """Watchdog creates audit log with correct entries for each converted file."""
    if not (HAS_BIN and HAS_CONVERT):
        return skip("watchdog audit log", "missing tools/binary")

    src   = OUTDIR / "audit_e2e_src"
    out   = OUTDIR / "audit_e2e_out"
    audit = OUTDIR / "audit_e2e.jsonl"
    state = OUTDIR / "audit_e2e.state"
    src.mkdir()

    (src / "log_a.log").write_text(apache_lines(20))
    (src / "log_b.log").write_text(apache_lines(15))

    cfg = OUTDIR / "audit_e2e.toml"
    cfg.write_text(f"""
[watcher]
mode       = "local"
converter  = "pfc-convert"
state_file = "{state}"
audit_log  = "{audit}"

[source]
path = "{src}"

[output]
path = "{out}"

[converter_options]
schema = "apache"
""")

    r = subprocess.run(
        [sys.executable, WATCHDOG_SCRIPT, "--config", str(cfg), "--once"],
        capture_output=True, text=True, timeout=60,
    )
    assert_eq(r.returncode, 0)
    assert audit.exists(), "audit log not created"

    entries = read_jsonl(audit)
    assert_eq(len(entries), 2, "2 audit entries")

    for entry in entries:
        for field in ("logged_at", "input", "output", "converter", "rows", "duration_s"):
            assert field in entry, f"audit missing field: {field}"
        assert_eq(entry["converter"], "pfc-convert")

    row_counts = sorted(e["rows"] for e in entries)
    assert row_counts == [15, 20], f"row counts: {row_counts}"
    print(f"     audit log: {len(entries)} entries, rows={row_counts}")


# ===========================================================================
# Run
# ===========================================================================

if __name__ == "__main__":
    print(f"\nPFC Ecosystem — Integration Test Suite")
    print(f"pfc-convert: {'found at ' + str(CONVERT_DIR) if HAS_CONVERT else 'NOT FOUND'}")
    print(f"pfc-migrate: {'found at ' + str(MIGRATE_DIR) if HAS_MIGRATE else 'NOT FOUND'}")
    print(f"pfc_jsonl:   {'found' if HAS_BIN else 'NOT FOUND'}")
    print(f"Output:      {OUTDIR}\n")

    all_tests = [
        ("pipe: apache -> pfc-convert --stdout | pfc-migrate --stdin -> .pfc", test_pipe_apache_convert_then_migrate),
        ("pipe: csv -> pfc-convert --stdout | pfc-migrate --stdin -> .pfc",    test_pipe_csv_convert_then_migrate),
        ("watchdog: apache .log -> pfc-convert -> .pfc",                       test_watchdog_apache_convert),
        ("watchdog: jsonl.gz -> pfc-migrate -> .pfc",                         test_watchdog_jsonl_migrate),
        ("state: second scan skips already-processed files",                   test_state_persistence_no_double_process),
        ("mixed folder: apache + csv + jsonl.gz all converted",                test_mixed_folder_all_formats),
        ("field integrity: all apache CLF fields correct in output",           test_full_roundtrip_field_integrity),
        ("watchdog audit log: entries per file with correct fields",           test_watchdog_audit_log_entries),
    ]

    for name, fn in all_tests:
        test(name, fn)

    total   = len(results)
    passed  = sum(1 for _, ok, _ in results if ok)
    failed  = total - passed
    total_t = sum(t for _, _, t in results)

    print(f"\n{'='*65}")
    print(f"  {passed}/{total} PASS   {failed} FAIL   {total_t:.1f}s total")
    print(f"{'='*65}\n")

    if failed:
        print("Failed:")
        for name, ok, _ in results:
            if not ok:
                print(f"  - {name}")
        sys.exit(1)
