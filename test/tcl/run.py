#!/usr/bin/env python3
# usage: run.py BIN OUT.json [timeout] [jobs]   runs every test/*.test with BIN in its own directory
import subprocess, sys, os, glob, json, re, tempfile, shutil, concurrent.futures as cf
BIN, OUT = sys.argv[1], sys.argv[2]; TO = int(sys.argv[3]) if len(sys.argv) > 3 else 120; J = int(sys.argv[4]) if len(sys.argv) > 4 else 8
T = "/tmp/sqlite-src/sqlite-src-3530400/test"
skip = set(os.environ.get("SKIP", "").split())
files = sorted(glob.glob(T + "/*.test"))
only = os.environ.get("ONLY")
if only: files = [f for f in files if re.search(only, os.path.basename(f))]
def one(f):
    name = os.path.basename(f)
    d = tempfile.mkdtemp(prefix="t_", dir="/tmp/tcl/work")
    try:
        p = subprocess.run([BIN, f], cwd=d, capture_output=True, text=True, timeout=TO, errors="replace")
        out = p.stdout + p.stderr
        m = re.search(r"(\d+) errors out of (\d+) tests", out)
        r = {"name": name, "rc": p.returncode, "errors": int(m.group(1)) if m else None, "tests": int(m.group(2)) if m else None}
        if not m: r["tail"] = out[-300:]
        r["err_lines"] = [l[:160] for l in out.split("\n") if l.startswith("Error:") or "multiwriter" in l][:6]
        r["first_fail"] = (re.findall(r"^! (\S+) expected: (.*)\n! \S+ got: +(.*)$", out, re.M) or [None])[0]
        r["failed"] = re.findall(r"^! ?(\S+) expected", out, re.M)[:20] or re.findall(r"^(\S+)\.\.\.\s*\nExpected", out, re.M)[:20]
        return r
    except subprocess.TimeoutExpired:
        return {"name": name, "rc": "timeout", "errors": None, "tests": None}
    finally:
        shutil.rmtree(d, ignore_errors=True)
os.makedirs("/tmp/tcl/work", exist_ok=True)
res = []
with cf.ThreadPoolExecutor(J) as ex:
    for r in ex.map(one, files): res.append(r); print(r["name"], r["rc"], r["errors"], r["tests"], flush=True)
json.dump(res, open(OUT, "w"))
