"""Anvil core: durable store, fair scheduler, process runner, events, hooks, retention."""
import contextlib
import hashlib
import inspect
import json
import math
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

from . import config, sched

WRAP = str(Path(__file__).with_name("wrap.py"))
FINAL = ("succeeded", "failed", "canceled", "timed_out", "interrupted")
ATTENTION = ("failed", "timed_out", "interrupted")
NOTE_MAX = 500
CODE_SUFFIXES = {".py", ".sh", ".swift", ".metal", ".jl", ".js", ".ts", ".lua", ".r", ".R", ".m", ".mm", ".c", ".cc",
                 ".cpp", ".h", ".hpp", ".cu", ".rs", ".go", ".rb", ".pl"}
FILE_HASH_MAX = 64 << 20  # larger files named in a command are identified by size and mtime

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs(
 seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE, project TEXT, agent TEXT, name TEXT, note TEXT,
 est REAL, status TEXT, sub TEXT, timeout REAL, start_before REAL, cmd TEXT, cwd TEXT, env TEXT,
 identity TEXT, skey TEXT UNIQUE, rerun_of TEXT, retry TEXT, attempt INTEGER DEFAULT 0, next_at REAL,
 hold_reason TEXT, rev INTEGER DEFAULT 1, submitted REAL, started REAL, finished REAL, exit_code INTEGER,
 signal INTEGER, reason TEXT, progress TEXT, order_key REAL, wait_accum REAL DEFAULT 0, elig_since REAL,
 run_next_at REAL, run_next_by TEXT, run_next_reason TEXT, position INTEGER, est_wait REAL, pgid INTEGER,
 acked INTEGER DEFAULT 0, logs_removed INTEGER DEFAULT 0, term_s REAL, cleanup_s REAL,
 hidden INTEGER DEFAULT 0,  -- hidden: removed from views; the compact receipt stays
 reported INTEGER DEFAULT 0,  -- reported: 1 = a wait has returned this finished job, 2 = this hold
 last_at REAL);  -- moved to the back by hand: runs only when nothing else is eligible
CREATE INDEX IF NOT EXISTS jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS jobs_agent ON jobs(agent, status);
CREATE TABLE IF NOT EXISTS attempts(job_id TEXT, n INTEGER, started REAL, finished REAL, pgid INTEGER,
 exit_code INTEGER, signal INTEGER, status TEXT, reason TEXT, timeout REAL, term_s REAL, cleanup_s REAL,
 PRIMARY KEY(job_id, n));
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, kind TEXT, job_id TEXT,
 project TEXT, agent TEXT, data TEXT);
CREATE TABLE IF NOT EXISTS actions(id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, job_id TEXT, actor TEXT,
 action TEXT, detail TEXT);
CREATE INDEX IF NOT EXISTS actions_job ON actions(job_id);
CREATE TABLE IF NOT EXISTS parts(key TEXT PRIMARY KEY, charge REAL DEFAULT 0, last REAL);
CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS hooks(id INTEGER PRIMARY KEY AUTOINCREMENT, agent TEXT, command TEXT);
CREATE TABLE IF NOT EXISTS deliveries(event_id INTEGER, hook TEXT, tries INTEGER DEFAULT 0, next_at REAL,
 done INTEGER DEFAULT 0, err TEXT, PRIMARY KEY(event_id, hook));
"""


def _m1(db):  # version-0 databases predate versioning; only the earliest of them lack `hidden`
    if "hidden" not in {r[1] for r in db.execute("PRAGMA table_info(jobs)")}:
        db.execute("ALTER TABLE jobs ADD COLUMN hidden INTEGER DEFAULT 0")


def _m2(db):  # jobs that finished before waits tracked delivery count as reported
    db.execute("ALTER TABLE jobs ADD COLUMN reported INTEGER DEFAULT 0")
    db.execute(f"UPDATE jobs SET reported=1 WHERE status IN ({','.join('?' * len(FINAL))})", FINAL)


# Schema upgrades for existing databases, in order; PRAGMA user_version records the last one applied.
# New databases get SCHEMA directly at the latest version.
def _m3(db):
    db.execute("ALTER TABLE jobs ADD COLUMN last_at REAL")


MIGRATIONS = [_m1, _m2, _m3]


def open_db(path):
    db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    fresh = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='jobs'").fetchone() is None
    db.executescript(SCHEMA)  # creates the whole schema when fresh, and tables added later otherwise
    if fresh:
        db.execute(f"PRAGMA user_version={len(MIGRATIONS)}")
        return db
    version = db.execute("PRAGMA user_version").fetchone()[0]
    for n, migrate in enumerate(MIGRATIONS[version:], version + 1):
        db.execute("BEGIN IMMEDIATE")
        try:
            migrate(db)
            db.execute(f"PRAGMA user_version={n}")
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
    return db


class Err(Exception):
    def __init__(self, code, message, http=400):
        super().__init__(message)
        self.code, self.message, self.http = code, message, http


def group_alive(pgid):
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _sh(args, cwd):
    try:
        return subprocess.run(args, cwd=cwd, capture_output=True, timeout=10).stdout
    except (OSError, subprocess.TimeoutExpired):
        return b""


def _git(args, cwd, env=None, stdin=None):
    try:
        r = subprocess.run(["git", *args], cwd=cwd, capture_output=True, timeout=30, input=stdin,
                           env=dict(os.environ, **env) if env else None)
        return r.stdout.decode().strip() if r.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        return None


def _content_tree(cwd, watch):
    """Git tree hash of the checkout's content, from a scratch copy of the index so the real one is untouched.
    Unwatched: tracked files only (job outputs inside the repo are not source). Watched: those paths,
    untracked files included. Commits, amends and touches with identical bytes give the same tree."""
    idx = _git(["rev-parse", "--path-format=absolute", "--git-path", "index"], cwd)
    if not idx:
        return None
    tmp = tempfile.mkdtemp(prefix="anvil-index-")
    full, part = os.path.join(tmp, "full"), os.path.join(tmp, "watched")
    try:
        if os.path.exists(idx):
            shutil.copyfile(idx, full)  # keeps the stat cache, so only changed files are rehashed
        env = {"GIT_INDEX_FILE": full}
        add = ["-c", "core.hooksPath=/dev/null", "add", "-A", "--", *watch] if watch else \
            ["-c", "core.hooksPath=/dev/null", "add", "-u", "--", ":/"]
        if _git(add, cwd, env) is None:
            return None
        if not watch:
            return _git(["write-tree"], cwd, env)
        entries = _git(["ls-files", "-s", "--full-name", "--", *watch], cwd, env)
        if entries is None or _git(["update-index", "--index-info"], cwd, {"GIT_INDEX_FILE": part},
                                   (entries + "\n").encode()) is None:
            return None
        return _git(["write-tree"], cwd, {"GIT_INDEX_FILE": part})
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _file_hash(p):
    st = p.stat()
    if st.st_size > FILE_HASH_MAX:
        return f"{st.st_size}:{int(st.st_mtime)}"
    return hashlib.sha1(p.read_bytes()).hexdigest()[:16]


def _watch_hashes(cwd, watch):
    """Content of watched paths outside git (bounded walk)."""
    out = {}
    for w in watch:
        p = Path(w) if os.path.isabs(w) else Path(cwd) / w
        for f in ([p] if p.is_file() else sorted(p.rglob("*")) if p.is_dir() else []):
            if f.is_file() and len(out) < 2000:
                out[str(f)] = _file_hash(f)
    return out


def snapshot_identity(cmd, cwd, watch=None):
    """Source identity by content: the checkout's tree (or only the watched paths) plus every file named in the
    command. watch: paths relative to cwd that the job depends on; default the whole tracked checkout."""
    ident = {"v": 2}
    watch = [w for w in (watch or []) if w]
    if watch:
        ident["watch"] = watch
    tree = _content_tree(cwd, watch)
    if tree:
        ident["tree"] = tree
    elif watch:
        ident["watched"] = _watch_hashes(cwd, watch)
    files = {}
    for a in cmd[1:8]:  # cmd[0] is the interpreter or binary; scripts and configs follow it
        p = Path(a) if os.path.isabs(a) else Path(cwd) / a
        with contextlib.suppress(OSError):
            # Source, not data: in a repo only tracked files count (a checkpoint or results file the previous
            # job rewrites is not drift); outside one, only files that look like code.
            if p.is_file() and (_git(["ls-files", "--error-unmatch", "--", str(p)], cwd) is not None if tree
                                else p.suffix in CODE_SUFFIXES):
                files[str(p)] = _file_hash(p)
    if files:
        ident["files"] = files
    return ident


def _legacy_identity(cmd, cwd):
    """Identity as recorded before v2 (git HEAD + uncommitted diff + script stats), for jobs queued then."""
    ident = {}
    head = _sh(["git", "rev-parse", "HEAD"], cwd).decode().strip()
    if head:
        ident["git_head"] = head
        ident["git_changes"] = hashlib.sha1(_sh(["git", "diff", "HEAD"], cwd) +
                                            _sh(["git", "status", "--porcelain"], cwd)).hexdigest()[:12]
    files = {}
    for a in cmd[:2]:
        p = Path(a) if os.path.isabs(a) else Path(cwd) / a
        if p.is_file():
            st = p.stat()
            files[str(p)] = f"{st.st_size}:{int(st.st_mtime)}"
    if files:
        ident["files"] = files
    return ident


def current_identity(old, cmd, cwd):
    return snapshot_identity(cmd, cwd, old.get("watch")) if old.get("v") else _legacy_identity(cmd, cwd)


def drift(old, new, cwd=None):
    out = []
    if not old.get("v"):
        if old.get("git_head") != new.get("git_head"):
            out.append(f"git HEAD {str(old.get('git_head'))[:8]}→{str(new.get('git_head'))[:8]}")
        elif old.get("git_changes") != new.get("git_changes"):
            out.append("uncommitted changes differ")
    elif old.get("tree") and new.get("tree") and old["tree"] != new["tree"]:
        names = _git(["diff-tree", "-r", "--name-only", old["tree"], new["tree"]], cwd) \
            if old.get("tree") and new.get("tree") and cwd else None
        names = (names or "").split("\n") if names else []
        out.append(("source changed: " + ", ".join(names[:5]) + (f" (+{len(names) - 5} more)" if len(names) > 5
                                                                 else "")) if names else "source changed")
    elif old.get("watched") is not None and new.get("watched") is not None and old["watched"] != new["watched"]:
        ch = [os.path.basename(f) for f in set(old.get("watched") or {}) | set(new.get("watched") or {})
              if (old.get("watched") or {}).get(f) != (new.get("watched") or {}).get(f)]
        out.append("watched files changed: " + ", ".join(sorted(ch)[:5]))
    for f, v in (old.get("files") or {}).items():
        if (new.get("files") or {}).get(f) != v:
            out.append(f"{os.path.basename(f)} changed")
    return "; ".join(dict.fromkeys(out))


def number(v, name, lo=None):
    """A finite float from a request field, or a clear error (never a Python traceback)."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        raise Err("invalid", f"{name} must be a number, got {v!r}")
    if not math.isfinite(f) or (lo is not None and f < lo):
        raise Err("invalid", f"{name} must be a finite number >= {lo}")
    return f


def clean_retry(r):
    if r is None:
        return None
    if not isinstance(r, dict):
        raise Err("invalid", "retry must be an object {max_attempts, backoff_seconds, exit_codes, reasons}")
    codes = r.get("exit_codes") or []
    reasons = r.get("reasons") or []
    if not isinstance(codes, list) or not all(isinstance(c, int) and not isinstance(c, bool) for c in codes):
        raise Err("invalid", "retry.exit_codes must be a list of integers")
    if not isinstance(reasons, list) or not set(reasons) <= {"timed_out", "signal"}:
        raise Err("invalid", "retry.reasons must be a list drawn from timed_out, signal")
    bo = r.get("backoff_seconds")
    return {"max_attempts": int(number(r.get("max_attempts", 1), "retry.max_attempts", 1)),
            "backoff_seconds": number(30 if bo is None else bo, "retry.backoff_seconds", 0),
            "exit_codes": codes, "reasons": reasons}


def clean_env(env):
    if env is None:
        return {}
    if not isinstance(env, dict):
        raise Err("invalid", "env must be an object of NAME: value strings")
    out = {}
    for k, v in env.items():
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            v = str(v)
        if not isinstance(v, str) or not k or "=" in k or "\0" in k + v:
            raise Err("invalid", f"env {k!r}: names and values must be strings without '=' in the name or NUL")
        out[k] = v
    return out


def team(agent):
    """Fairness unit: sub-agents named parent:lane share their parent's turn and GPU time."""
    return agent.split(":", 1)[0]


class Service:
    def __init__(self):
        self.cfg = config.load()
        self.home = config.home()
        (self.home / "logs").mkdir(exist_ok=True)
        (self.home / "results").mkdir(exist_ok=True)
        self.db = open_db(self.home / "anvil.db")
        self.L = threading.RLock()
        self.kick_cv = threading.Condition(self.L)
        self.ev_cv = threading.Condition()
        self.slot = None
        self.outside = []
        self.stopping = False
        self.started_at = time.time()

    # ---------- store helpers ----------
    def q(self, sql, *a):
        return self.db.execute(sql, a).fetchall()

    def q1(self, sql, *a):
        return self.db.execute(sql, a).fetchone()

    def x(self, sql, *a):
        return self.db.execute(sql, a)

    def tx(self):
        """Re-entrant transaction (callers hold self.L)."""
        svc = self

        class T:
            def __enter__(s):
                if getattr(svc, "_depth", 0) == 0:
                    svc.db.execute("BEGIN IMMEDIATE")  # a failed BEGIN must not leave the depth raised
                svc._depth = getattr(svc, "_depth", 0) + 1

            def __exit__(s, et, *_):
                svc._depth -= 1
                if svc._depth == 0:
                    svc.db.execute("ROLLBACK" if et else "COMMIT")
        return T()

    def kv(self, k, default=None):
        r = self.q1("SELECT v FROM kv WHERE k=?", k)
        return json.loads(r["v"]) if r else default

    def set_kv(self, k, v):
        self.x("INSERT OR REPLACE INTO kv VALUES(?,?)", k, json.dumps(v))

    def job(self, jid):
        r = self.q1("SELECT * FROM jobs WHERE id=?", jid)
        if not r:
            raise Err("not_found", f"no job {jid}", 404)
        return r

    def upd(self, jid, bump=True, **f):
        if bump:
            f["rev"] = self.job(jid)["rev"] + 1
        sets = ",".join(f"{k}=?" for k in f)
        self.x(f"UPDATE jobs SET {sets} WHERE id=?", *f.values(), jid)

    def event(self, kind, job=None, **data):
        self.x("INSERT INTO events(ts,kind,job_id,project,agent,data) VALUES(?,?,?,?,?,?)", time.time(), kind,
               job["id"] if job else None, job["project"] if job else None, job["agent"] if job else None,
               json.dumps(data, separators=(",", ":")) if data else None)
        eid = self.q1("SELECT last_insert_rowid() i")["i"]
        if kind in ("finished", "action_needed") and job:
            for h in self.hooks_for(job["agent"]):
                self.x("INSERT OR IGNORE INTO deliveries(event_id,hook,next_at) VALUES(?,?,?)", eid, h, time.time())
        return eid

    def action(self, jid, actor, act, detail=None):
        self.x("INSERT INTO actions(ts,job_id,actor,action,detail) VALUES(?,?,?,?,?)", time.time(), jid, actor, act,
               json.dumps(detail) if detail is not None else None)

    def cursor(self):
        return self.q1("SELECT COALESCE(MAX(id),0) c FROM events")["c"]

    def _wake(self):
        """Call after committing: wake waiters and the scheduler."""
        with self.ev_cv:
            self.ev_cv.notify_all()
        self.kick_cv.notify_all()

    # ---------- participants ----------
    def parts(self):
        p = {"p": {}, "a": {}, "pl": {}, "al": {}}
        for r in self.q("SELECT * FROM parts"):
            k = r["key"]
            if k.startswith("p:"):
                p["p"][k[2:]] = r["charge"]
                if r["last"] is not None:
                    p["pl"][k[2:]] = r["last"]
            else:
                key = tuple(k[2:].split("\x1f", 1))
                p["a"][key] = r["charge"]
                if r["last"] is not None:
                    p["al"][key] = r["last"]
        return p

    def _charge(self, key, add=0.0, last=None, floor=None):
        r = self.q1("SELECT charge,last FROM parts WHERE key=?", key)
        c = (r["charge"] if r else 0.0) + add
        if floor is not None:
            c = max(c, floor)
        self.x("INSERT OR REPLACE INTO parts VALUES(?,?,?)", key, c,
               last if last is not None else (r["last"] if r else None))

    def _baseline(self, project, agent):
        """A new or returning participant starts at the current active baseline (never below its own debt)."""
        act = self.q("SELECT DISTINCT project, agent FROM jobs WHERE status IN ('queued','held','running')")
        if not any(r["project"] == project for r in act):
            others = [self.q1("SELECT charge FROM parts WHERE key=?", "p:" + r["project"]) for r in act]
            vals = [o["charge"] for o in others if o]
            self._charge("p:" + project, floor=min(vals) if vals else None)
        agent = team(agent)
        if not any(r["project"] == project and team(r["agent"]) == agent for r in act):
            others = [self.q1("SELECT charge FROM parts WHERE key=?", f"a:{project}\x1f{team(r['agent'])}")
                      for r in act if r["project"] == project]
            vals = [o["charge"] for o in others if o]
            self._charge(f"a:{project}\x1f{agent}", floor=min(vals) if vals else None)

    # ---------- permissions ----------
    def is_operator(self, actor):
        ops = self.cfg["operators"]
        return "*" in ops or actor in ops

    def authorize(self, job, actor):
        """An agent controls its own jobs and its team's (parent:lane); operators control everything."""
        if team(actor) != team(job["agent"]) and not self.is_operator(actor):
            raise Err("forbidden", f"{job['id']} belongs to agent {job['agent']!r}; you are acting as {actor!r}. "
                      f"If you submitted it, act with the agent name you submitted with (MCP: agent, CLI: "
                      f"--actor or ANVIL_AGENT). Other agents' jobs are only for operators (config.json)", 403)

    # ---------- submit ----------
    def submit(self, d, actor=None):
        for k in ("project", "agent", "name", "cmd"):
            if not d.get(k):
                raise Err("invalid", f"missing {k}")
        cmd = d["cmd"]
        if not isinstance(cmd, list) or not all(isinstance(c, str) for c in cmd):
            raise Err("invalid", "cmd must be a list of strings")
        if not all(isinstance(d[k], str) for k in ("project", "agent", "name")):
            raise Err("invalid", "project, agent and name must be strings")
        timeout = self.limit(d.get("timeout_seconds"), self.cfg["default_timeout_seconds"])
        est = d.get("estimated_seconds")
        est = None if est is None else number(est, "estimated_seconds", 0)
        now = time.time()
        sb = d.get("start_before")
        if d.get("start_within_seconds") is not None:
            sb = now + number(d["start_within_seconds"], "start_within_seconds", 0)
        elif sb is not None:
            sb = number(sb, "start_before")
            if sb < 1e9:
                raise Err("invalid", "start_before is an absolute time (epoch seconds); "
                                     "use start_within_seconds for a relative deadline")
            if sb <= now:
                raise Err("invalid", "start_before is already past")
        env = clean_env(d.get("env"))
        note = (d.get("note") or "")
        if len(note) > NOTE_MAX:
            raise Err("invalid", f"note longer than {NOTE_MAX} chars")
        cwd = d.get("cwd")
        if not isinstance(cwd, str) or not os.path.isabs(cwd):
            raise Err("invalid", "cwd must be an absolute path")
        if not os.path.isdir(cwd):
            raise Err("invalid", f"cwd does not exist: {cwd}")
        exe, path = cmd[0] if cmd else "", env.get("PATH") or os.environ.get("PATH", "")
        found = (os.access(os.path.join(cwd, exe), os.X_OK) if "/" in exe else shutil.which(exe, path=path))
        if not found:  # fail now, not after the queue wait
            raise Err("command_not_found", f"{exe!r} not found or not executable (PATH as submitted)")
        watch = d.get("watch") or []
        if isinstance(watch, str):
            watch = [watch]
        if not isinstance(watch, list) or not all(isinstance(w, str) and w for w in watch):
            raise Err("invalid", "watch must be a list of paths")
        for w in watch:
            if not os.path.exists(w if os.path.isabs(w) else os.path.join(cwd, w)):
                raise Err("invalid", f"watch path does not exist: {w}")
        retry = clean_retry(d.get("retry"))
        # keys are per agent: two agents picking the same natural key don't get each other's job
        skey = f"{d['agent']}\x1f{d['submission_key']}" if d.get("submission_key") else None
        # git work happens before taking the service lock, so it never stalls other callers
        ident = snapshot_identity(cmd, cwd, watch) if d.get("check_drift", True) else {}
        with self.L:
            if skey:
                r = self.q1("SELECT id FROM jobs WHERE skey=?", skey)
                if r:
                    return dict(self.status(r["id"]), duplicate=True)
            now = time.time()
            with self.tx():
                self._baseline(d["project"], d["agent"])
                cur = self.x("""INSERT INTO jobs(project,agent,name,note,est,status,timeout,start_before,cmd,cwd,env,
                    identity,skey,rerun_of,retry,next_at,submitted,order_key,elig_since)
                    VALUES(?,?,?,?,?,'queued',?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                             d["project"], d["agent"], d["name"][:120], note or None,
                             est, timeout, sb,
                             json.dumps(cmd), cwd, json.dumps(env), json.dumps(ident),
                             skey, d.get("rerun_of"),
                             json.dumps(retry) if retry else None, now, now, now, now)
                seq = cur.lastrowid
                jid = f"j{seq}"
                self.x("UPDATE jobs SET id=? WHERE seq=?", jid, seq)
                j = self.job(jid)
                self.action(jid, actor or d["agent"], "submit")
                self.event("submitted", j)
                self._positions()
            self._wake()
            return self.status(jid)

    # ---------- views ----------
    def elapsed(self, j):
        if j["status"] == "running" and self.slot and self.slot["job"] == j["id"]:
            return time.monotonic() - self.slot["t0"]
        if j["started"] and j["finished"]:
            return j["finished"] - j["started"]
        if j["started"] and j["status"] == "running":
            return time.time() - j["started"]
        return None

    def why(self, j):
        if j["status"] == "held":
            return j["hold_reason"] or "held"
        if j["status"] != "queued":
            return None
        if j["next_at"] and j["next_at"] > time.time():
            return f"retry backoff ({int(j['next_at'] - time.time())}s)"
        if self.kv("paused", False):
            return "scheduling paused"
        if self.slot:
            return f"GPU busy ({self.slot['job']})"
        return "starting"

    def status(self, jid, j=None):
        j = j or self.job(jid)
        d = {"id": j["id"], "project": j["project"], "agent": j["agent"], "name": j["name"],
             "status": j["status"], "rev": j["rev"], "position": j["position"],
             "timeout_s": j["timeout"], "est_s": j["est"]}
        if j["note"]:
            d["note"] = j["note"] if len(j["note"]) <= 160 else j["note"][:157] + "…"
        if j["sub"]:
            d["sub"] = j["sub"]
        w = self.why(j)
        if w:
            d["why"] = w
        if j["status"] == "held":  # a held job never starts on its own
            if self.owner_must_act(j):
                d["action_required"] = ("release (with accept_drift to run the current source) or cancel"
                                        if (j["hold_reason"] or "").startswith("drift") else "release or cancel")
            else:
                d["waiting_on"] = "someone else held it; it starts when they release it. Nothing to do"
        if j["status"] == "queued" and j["position"]:
            d["approx_wait_s"] = j["est_wait"]
        el = self.elapsed(j)
        if el is not None:
            d["elapsed_s"] = round(el)
        if j["attempt"] > 1 or j["retry"]:
            d["attempt"] = j["attempt"]
        if j["status"] in FINAL:
            d.update(exit_code=j["exit_code"], reason=j["reason"])
            if j["status"] in ATTENTION:
                d["acked"] = bool(j["acked"])
            if j["signal"]:
                d["signal"] = j["signal"]
        if j["progress"]:
            with contextlib.suppress(ValueError):  # rows from before progress was capped may be cut mid-JSON
                d["progress"] = json.loads(j["progress"])
        if j["run_next_at"]:
            d["run_next"] = {"by": j["run_next_by"], "reason": j["run_next_reason"]}
        if j["last_at"]:
            d["run_last"] = True
        if j["rerun_of"]:
            d["rerun_of"] = j["rerun_of"]
        return d

    @staticmethod
    def owner_must_act(j):
        """Holds the job's agent should resolve: drift, or its own hold. A hold by the person or another
        operator is theirs to release; the agent should leave it alone."""
        r = j["hold_reason"] or ""
        if r.startswith("drift"):
            return True
        by = r[len("held by "):].split(":", 1)[0] if r.startswith("held by ") else ""
        return team(by) == team(j["agent"]) or by == "anvil"

    def detail(self, jid):
        with self.L:
            j = self.job(jid)
            d = self.status(jid, j)
            ident = json.loads(j["identity"] or "{}")
            d.update(note=j["note"], cmd=json.loads(j["cmd"]), cwd=j["cwd"],
                     env_keys=sorted(json.loads(j["env"] or "{}")), identity=ident,
                     submitted=j["submitted"], started=j["started"], finished=j["finished"],
                     start_before=j["start_before"], retry=json.loads(j["retry"]) if j["retry"] else None,
                     term_s=j["term_s"], cleanup_s=j["cleanup_s"], acked=bool(j["acked"]),
                     logs_removed=bool(j["logs_removed"]))
            d["attempts"] = [dict(r) for r in self.q(
                "SELECT n,started,finished,exit_code,signal,status,reason,timeout,term_s,cleanup_s FROM attempts "
                "WHERE job_id=? ORDER BY n", jid)]
            d["history"] = [{"ts": r["ts"], "actor": r["actor"], "action": r["action"],
                             "detail": json.loads(r["detail"]) if r["detail"] else None}
                            for r in self.q("SELECT * FROM actions WHERE job_id=? ORDER BY id", jid)]
            rd = self.home / "results" / jid
            d["results"] = sorted(str(p.relative_to(rd)) for p in rd.rglob("*") if p.is_file())[:50] \
                if rd.exists() else []
            d["results_dir"] = str(rd)
            return d

    def list(self, project=None, agent=None, status=None, limit=50, after=None):
        limit = max(1, min(int(limit or 50), 200))
        w, a = [], []
        for col, v in (("project", project), ("agent", agent)):
            if v:
                w.append(f"{col}=?")
                a.append(v)
        if status:
            st = status.split(",")
            w.append(f"status IN ({','.join('?' * len(st))})")
            a += st
        if after:
            w.append("seq<?")
            a.append(int(str(after).lstrip("j")))
        w.append("hidden=0")
        sql = "SELECT * FROM jobs WHERE " + " AND ".join(w) + " ORDER BY seq DESC LIMIT ?"
        with self.L:
            rows = self.q(sql, *a, limit + 1)
            items = [self.status(r["id"], r) for r in rows[:limit]]
        return {"jobs": items, "next": rows[limit - 1]["id"] if len(rows) > limit else None}

    def gpu_state(self):
        if self.slot and self.slot.get("stuck"):
            return "attention"
        if self.q1(f"SELECT 1 FROM jobs WHERE (status IN {ATTENTION} AND acked=0 AND hidden=0) "
                   "OR (status='held' AND hold_reason LIKE 'drift%') LIMIT 1"):
            return "attention"
        if self.slot:
            return "running"
        return "paused" if self.kv("paused", False) else "idle"

    def attention(self):
        """Why the GPU says "Needs Attention", across all projects: what to look at and how to clear it."""
        out = []
        if self.slot and self.slot.get("stuck"):
            out.append({"id": self.slot["job"], "why": "processes survive SIGKILL", "clear": "check the processes"})
        for r in self.q(f"SELECT * FROM jobs WHERE (status IN {ATTENTION} AND acked=0 AND hidden=0) "
                        "OR (status='held' AND hold_reason LIKE 'drift%') ORDER BY seq DESC LIMIT 20"):
            held = r["status"] == "held"
            out.append({"id": r["id"], "name": r["name"], "agent": r["agent"], "project": r["project"],
                        "status": r["status"], "why": r["hold_reason"] if held else (r["reason"] or r["status"]),
                        "clear": "release or cancel" if held else "dismiss (ack)"})
        return out

    def overview(self, project=None, agent=None, history=8):
        with self.L:
            f, a = "", []
            if project:
                f += " AND project=?"
                a.append(project)
            if agent:
                f += " AND agent=?"
                a.append(agent)
            # the running job is always shown: a filtered view must not hide what holds the GPU
            queue = self.q(f"SELECT * FROM jobs WHERE status='running' OR (status IN ('queued','held'){f}) "
                           "ORDER BY status!='running', position IS NULL, position, seq", *a)
            recent = self.q(f"SELECT * FROM jobs WHERE status IN {FINAL} AND hidden=0{f} ORDER BY finished DESC LIMIT ?",
                            *a, int(history))
            names = self.q("SELECT DISTINCT project, agent FROM jobs WHERE submitted > ?", time.time() - 7 * 86400)
            return {"gpu": self.gpu_state(), "paused": self.kv("paused", False),
                    "active": self.slot["job"] if self.slot else None,
                    "queue": [self.status(r["id"], r) for r in queue],
                    "recent": [self.status(r["id"], r) for r in recent],
                    "outside": self.outside, "cursor": self.cursor(), "attention": self.attention(),
                    "projects": sorted({r["project"] for r in names}),
                    "agents": sorted({r["agent"] for r in names})}

    # ---------- events / wait ----------
    def events(self, cursor=0, job=None, agent=None, project=None, limit=100):
        w, a = ["id>?"], [int(cursor or 0)]
        for col, v in (("job_id", job), ("agent", agent), ("project", project)):
            if v:
                w.append(f"({col}=? OR kind IN ('queue','paused','resumed'))" if col != "job_id" else f"{col}=?")
                a.append(v)
        rows = self.q(f"SELECT * FROM events WHERE {' AND '.join(w)} ORDER BY id LIMIT ?", *a, int(limit))
        return [{"id": r["id"], "ts": round(r["ts"], 1), "kind": r["kind"], "job": r["job_id"],
                 **(json.loads(r["data"]) if r["data"] else {})} for r in rows]

    def wait(self, job=None, revision=None, cursor=None, agent=None, project=None, timeout=30, until_done=False,
             gone=None):
        """Bounded wait (<=600s per call; the CLI repeats it). With job: returns when that job's record changes
        (until_done: when it finishes or is held). Without job, until_done: returns the agent's finished jobs, and
        jobs newly held, that no wait has returned yet, or none_pending when it has nothing left to wait for. A
        held job never finishes on its own, so it comes back at once with action_required. With cursor: returns
        new events. Returns {'unchanged': True, ...} on timeout.
        gone: returns True once the caller has disconnected; then nothing is marked delivered, so a result is
        never consumed by a client that won't see it."""
        timeout = max(0.0, min(number(30 if timeout in (None, "") else timeout, "timeout", 0), 600))
        deadline = time.monotonic() + timeout
        while True:
            if gone and gone():
                return None
            with self.L:
                if job:
                    j = self.job(job)
                    done = j["status"] in FINAL
                    changed = revision is None or j["rev"] != int(revision)
                    # a hold its agent must resolve comes back once (until the job changes again)
                    act = j["status"] == "held" and self.owner_must_act(j) and (changed or j["reported"] == 0)
                    if (changed and not until_done) or (until_done and (done or act)):
                        if done or act:
                            self.x("UPDATE jobs SET reported=? WHERE id=?", 1 if done else 2, job)
                        return self.status(job, j)
                    cur = self.cursor()
                elif until_done:
                    if not (agent or project):  # otherwise one agent's wait would consume everyone's results
                        raise Err("invalid", "waiting for any finished job needs an agent (or project)")
                    w, a = [], []
                    for col, v in (("agent", agent), ("project", project)):
                        if v:
                            w.append(f"{col}=?")
                            a.append(v)
                    fin = ','.join('?' * len(FINAL))
                    # Unreported results count even if the person removed or cleared the job meanwhile.
                    rows = self.q(f"SELECT * FROM jobs WHERE {' AND '.join(w)} AND "
                                  f"((status IN ({fin}) AND reported!=1) OR (status NOT IN ({fin}) AND hidden=0)) "
                                  "ORDER BY finished", *a, *FINAL, *FINAL)
                    finished = [r["id"] for r in rows if r["status"] in FINAL]
                    held = [r for r in rows if r["status"] == "held"]
                    act = [r["id"] for r in held if r["reported"] == 0 and self.owner_must_act(r)]
                    if finished or act:
                        for ids, v in ((finished, 1), (act, 2)):
                            if ids:
                                self.x(f"UPDATE jobs SET reported={v} WHERE id IN ({','.join('?' * len(ids))})", *ids)
                        out = {"jobs": [self.status(i) for i in finished + act],
                               "pending": len(rows) - len(finished)}
                        if act:
                            out["action_required"] = act
                        return out
                    if len(held) == len(rows):  # nothing will finish until someone releases a hold
                        out = {"jobs": [], "pending": 0, "none_pending": True}
                        if held:
                            out["held"] = [{"id": r["id"], "why": r["hold_reason"]} for r in held]
                        return out
                    cur = self.cursor()
                else:
                    ev = self.events(cursor if cursor is not None else self.cursor(), None, agent, project)
                    if ev:
                        return {"events": ev, "cursor": ev[-1]["id"]}
                    cur = cursor if cursor is not None else self.cursor()
                    cursor = cur
            left = deadline - time.monotonic()
            if left <= 0:
                out = {"unchanged": True, "cursor": cur}
                if job and j["status"] == "held":
                    out["held"] = {"why": j["hold_reason"], **({"action_required": True} if self.owner_must_act(j)
                                                               else {"waiting_on": "someone else's hold"})}
                if job:
                    out.update(id=job, rev=j["rev"], status=j["status"])
                    st = self.status(job, j)
                    est = st.get("est_s")
                    if est is not None:  # nudge short polling loops toward one wait that covers the job
                        rest = max(0, est - st.get("elapsed_s", 0)) if j["status"] == "running" \
                            else (st.get("approx_wait_s") or 0) + est
                        out["approx_left_s"] = round(rest)
                        out["hint"] = (f"nothing changed; about {round(rest)}s left by the estimates. Wait once "
                                       f"with no timeout (MCP: call gpu_wait again right away), in the background if you can, "
                                       f"instead of polling")
                return out
            with self.ev_cv:
                self.ev_cv.wait(min(left, 5))

    # ---------- controls ----------
    def control(self, jid, op, actor, **kw):
        fn = getattr(self, "_c_" + op)
        params = inspect.signature(fn).parameters
        if not any(p.kind == p.VAR_KEYWORD for p in params.values()):
            allowed = [n for n in params if n not in ("j", "actor")]
            bad = [k for k, v in kw.items() if k not in allowed and v not in (None, False, "")]
            if bad:
                raise Err("invalid", f"{op} doesn't take {', '.join(bad)}"
                                     + (f"; it takes {', '.join(allowed)}" if allowed else ""))
            kw = {k: v for k, v in kw.items() if k in allowed}
        with self.L:
            j = self.job(jid)
            self.authorize(j, actor)
            with self.tx():
                out = getattr(self, "_c_" + op)(j, actor, **kw)
                self._positions()
            self._wake()
            return out or self.status(jid)

    def _c_cancel(self, j, actor, reason=None):
        if j["status"] in ("queued", "held"):
            self._finish_unstarted(j, "canceled", reason or "canceled")
            self.action(j["id"], actor, "cancel", reason)
        elif j["status"] == "running":
            if not self.slot or self.slot["job"] != j["id"]:
                raise Err("conflict", "running job not owned by this service instance")
            self.slot["stop"] = self.slot["stop"] or "cancel"
            self.action(j["id"], actor, "cancel", reason)
            self.upd(j["id"], sub="stopping")
            self.event("stopping", j, by=actor)
        else:
            raise Err("conflict", f"job is {j['status']}")

    def _finish_unstarted(self, j, status, reason):
        self.upd(j["id"], status=status, reason=reason, finished=time.time(), position=None, est_wait=None,
                 elig_since=None, run_next_at=None)
        self.event("finished", self.job(j["id"]), status=status, reason=reason)

    def _c_hold(self, j, actor, reason=None):
        if j["status"] != "queued":
            raise Err("conflict", f"only queued jobs can be held (job is {j['status']})")
        now = time.time()
        acc = j["wait_accum"] + max(0.0, now - (j["elig_since"] or now))
        self.upd(j["id"], status="held", hold_reason=f"held by {actor}" + (f": {reason}" if reason else ""),
                 wait_accum=acc, elig_since=None, position=None, est_wait=None)
        self.action(j["id"], actor, "hold", reason)
        self.event("held", j, by=actor)

    def _c_release(self, j, actor, accept_drift=False):
        if j["status"] != "held":
            raise Err("conflict", f"job is {j['status']}, not held")
        f = {}
        if (j["hold_reason"] or "").startswith("drift"):
            if not accept_drift:
                raise Err("drift", f"{j['hold_reason']}; release with accept_drift to run on the current source")
            f["identity"] = json.dumps(snapshot_identity(json.loads(j["cmd"]), j["cwd"],
                                                         json.loads(j["identity"] or "{}").get("watch")))
        self.upd(j["id"], status="queued", hold_reason=None, elig_since=time.time(), reported=0, **f)
        self.action(j["id"], actor, "release", {"accept_drift": True} if accept_drift else None)
        self.event("released", j, by=actor)

    def _c_reorder(self, j, actor, before=None, after=None, first=False, last=False):
        if j["status"] not in ("queued", "held"):
            raise Err("conflict", "only pending jobs can be reordered")
        mine = self.q("SELECT id, order_key FROM jobs WHERE project=? AND agent=? AND status IN ('queued','held') "
                      "AND id!=? ORDER BY order_key, seq", j["project"], j["agent"], j["id"])
        keys = [r["order_key"] for r in mine]
        ids = [r["id"] for r in mine]
        ref = before or after
        if ref:
            if ref not in ids:
                raise Err("invalid", f"{ref} is not a pending job of {j['agent']} in {j['project']}; "
                                     "cross-agent priority uses run_next")
            i = ids.index(ref) + (1 if after else 0)
        else:
            i = 0 if first else len(ids)
        lo = keys[i - 1] if i > 0 else (keys[0] - 2 if keys else 0)
        hi = keys[i] if i < len(keys) else (keys[-1] + 2 if keys else 2)
        self.upd(j["id"], order_key=(lo + hi) / 2)
        self.action(j["id"], actor, "reorder", {"before": before, "after": after, "first": first, "last": last})

    def _c_run_next(self, j, actor, reason=None):
        if not self.is_operator(actor):
            raise Err("forbidden", "run_next requires an operator (config.json operators)", 403)
        if not reason:
            raise Err("invalid", "run_next requires a reason")
        if j["status"] != "queued":
            raise Err("conflict", f"job is {j['status']}")
        self.upd(j["id"], run_next_at=time.time(), run_next_by=actor, run_next_reason=reason)
        self.action(j["id"], actor, "run_next", reason)
        self.event("run_next", j, by=actor, reason=reason)

    def _c_move(self, j, actor, before=None, after=None, last=False):
        """Operator: put a queued job at a chosen place in the whole queue, across projects and agents. Fair order
        can't express a hand-made one, so every queued job up to and including it is pinned in that order
        (run-next, visible to everyone); the jobs after it keep their fair order. Placing it after every other
        queued job (or last) moves it to the back instead, pinning nothing."""
        if not self.is_operator(actor):
            raise Err("forbidden", "moving across agents requires an operator (config.json operators)", 403)
        if j["status"] != "queued":
            raise Err("conflict", f"only queued jobs can be moved (job is {j['status']})")
        rows = self.q("SELECT id, run_next_at FROM jobs WHERE status='queued' AND id!=? "
                      "ORDER BY position IS NULL, position, seq", j["id"])
        order = [r["id"] for r in rows]
        ref = before or after
        if not last:
            if not ref:
                raise Err("invalid", "move needs before, after or last")
            if ref not in order:
                raise Err("invalid", f"{ref} is not another queued job; place the job relative to a queued one")
            i = order.index(ref) + (1 if after else 0)
            last = i == len(order)
        if last:
            self.upd(j["id"], run_next_at=None, run_next_by=None, run_next_reason=None, last_at=time.time())
            self.action(j["id"], actor, "move", {"last": True})
            self.event("moved", j, by=actor, to="last")
            return
        pinned = order[:i] + [j["id"]]
        base = min([r["run_next_at"] for r in rows if r["run_next_at"]] + [time.time()]) - len(pinned)
        for k, jid in enumerate(pinned):
            self.upd(jid, run_next_at=base + k, run_next_by=actor, run_next_reason="ordered by hand", last_at=None)
        self.action(j["id"], actor, "move", {"before": before, "after": after, "pinned": pinned})
        self.event("run_next", j, by=actor, reason="ordered by hand")

    def _c_unpin(self, j, actor):
        """Undo run-next or move-to-back: the job goes back to its fair place."""
        if not (j["run_next_at"] or j["last_at"]):
            raise Err("conflict", f"{j['id']} isn't pinned")
        if j["run_next_at"] and not self.is_operator(actor):
            raise Err("forbidden", "run-next was set by an operator; only an operator can undo it", 403)
        self.upd(j["id"], run_next_at=None, run_next_by=None, run_next_reason=None, last_at=None)
        self.action(j["id"], actor, "unpin")
        self.event("unpinned", j, by=actor)

    def limit(self, v, default=None):
        """A job's hard time limit in seconds, or None for no limit (given as 0 or "none")."""
        if v is None:
            v = default
        if v is None or v in (0, "0", "none", ""):
            s = None
        else:
            try:
                s = float(v)
            except (TypeError, ValueError):
                raise Err("invalid", "time limit must be seconds, or 0/\"none\" for no limit")
            if s <= 0:
                raise Err("invalid", "time limit must be positive, or 0/\"none\" for no limit")
        mx = self.cfg["max_timeout_seconds"]
        if mx and (s is None or s > mx):
            raise Err("timeout_too_large", f"time limit must be at most the service maximum {mx}s")
        return s

    def _c_timeout(self, j, actor, seconds=None):
        if seconds is None:
            raise Err("invalid", "missing seconds (0 or \"none\" for no limit)")
        s = self.limit(seconds)
        if j["status"] == "running":
            el = self.elapsed(j) or 0
            if s is not None and s <= el:
                raise Err("invalid", f"limit must exceed elapsed {el:.0f}s (limit is absolute from start)")
            if self.slot and self.slot["job"] == j["id"]:
                if self.slot["stop"]:
                    raise Err("conflict", f"job is already stopping ({self.slot['stop']})")
                self.slot["limit"] = s
        elif j["status"] not in ("queued", "held"):
            raise Err("conflict", f"job is {j['status']}")
        self.upd(j["id"], timeout=s)
        self.action(j["id"], actor, "timeout", {"from": j["timeout"], "to": s})
        self.event("timeout_updated", j, to=s, by=actor)

    def _c_remove(self, j, actor):
        """Remove a job from views: a pending job is canceled first; a running job must be stopped first.
        Logs are deleted; the compact receipt stays in the database."""
        if j["status"] == "running":
            raise Err("conflict", "stop the running job first")
        if j["status"] in ("queued", "held"):
            self._finish_unstarted(j, "canceled", "removed")
        shutil.rmtree(self.home / "logs" / j["id"], ignore_errors=True)
        self.upd(j["id"], hidden=1, acked=1, logs_removed=1)
        self.action(j["id"], actor, "remove")
        self.event("removed", j, by=actor)

    def clear_finished(self, actor, project=None, agent=None, queue=False):
        """Remove every finished job (optionally one project/agent) from views; queue: cancel and remove every
        queued and held job instead."""
        with self.L:
            f, a = "", []
            for col, v in (("project", project), ("agent", agent)):
                if v:
                    f += f" AND {col}=?"
                    a.append(v)
            states = ("queued", "held") if queue else FINAL
            rows = self.q(f"SELECT * FROM jobs WHERE status IN ({','.join('?' * len(states))}) AND hidden=0{f}",
                          *states, *a)
            if not self.is_operator(actor):  # an agent clears its own (and its team's) jobs
                rows = [r for r in rows if team(r["agent"]) == team(actor)]
            with self.tx():
                for r in rows:
                    self._c_remove(r, actor)
                self._positions()
            self._wake()
        return {"removed": len(rows)}

    def _c_ack(self, j, actor):
        self.upd(j["id"], acked=1)
        self.action(j["id"], actor, "ack")

    def _c_rm_logs(self, j, actor):
        if j["status"] not in FINAL:
            raise Err("conflict", "job not finished")
        shutil.rmtree(self.home / "logs" / j["id"], ignore_errors=True)
        self.upd(j["id"], logs_removed=1, acked=1)
        self.action(j["id"], actor, "rm_logs")

    def _c_rerun(self, j, actor, **over):
        if j["status"] not in FINAL:
            raise Err("conflict", "rerun applies to finished jobs; use cancel first")
        d = {"project": j["project"], "agent": j["agent"], "name": j["name"], "note": j["note"],
             "estimated_seconds": j["est"], "timeout_seconds": "none" if j["timeout"] is None else j["timeout"],
             "cmd": json.loads(j["cmd"]),
             "cwd": j["cwd"], "env": json.loads(j["env"] or "{}"),
             "retry": json.loads(j["retry"]) if j["retry"] else None, "rerun_of": j["id"],
             "watch": json.loads(j["identity"] or "{}").get("watch")}
        d.update({k: v for k, v in over.items() if v is not None})
        out = self.submit(d, actor)
        self.action(j["id"], actor, "rerun", out["id"])
        return out

    def set_paused(self, paused, actor):
        if not self.is_operator(actor):
            raise Err("forbidden", "pause/resume requires an operator", 403)
        with self.L:
            with self.tx():
                self.set_kv("paused", bool(paused))
                self.action(None, actor, "pause" if paused else "resume")
                self.event("paused" if paused else "resumed", None, by=actor)
            self._wake()
        return {"paused": bool(paused)}

    def logs(self, jid, tail=4000, since=None, attempt=None):
        j = self.job(jid)
        n = int(attempt or j["attempt"] or 0)
        if j["logs_removed"]:
            return {"id": jid, "attempt": n, "data": "", "removed": True}
        p = self.home / "logs" / jid / f"{n}.log"
        if not n or not p.exists():
            return {"id": jid, "attempt": n, "data": "", "cursor": 0}
        size = p.stat().st_size
        cap = max(1, min(int(tail or 4000), 65536))
        start = min(max(0, int(since)), size) if since is not None else max(0, size - cap)
        end = min(size, start + cap)
        with open(p, "rb") as f:
            f.seek(start)
            data = f.read(end - start)
        return {"id": jid, "attempt": n, "data": data.decode("utf-8", "replace"), "cursor": end,
                "truncated_before": start > 0, "more": end < size}

    def hooks_for(self, agent):
        out = [f"db:{r['id']}" for r in self.q("SELECT id FROM hooks WHERE agent=? OR agent='*'", agent)]
        for i, h in enumerate(self.cfg["hooks"]):
            if "*" in h.get("agents", ["*"]) or agent in h.get("agents", []):
                out.append(f"cfg:{i}")
        return out

    def add_hook(self, agent, command):
        with self.L:
            self.x("DELETE FROM hooks WHERE agent=? AND command=?", agent, command)
            hid = self.x("INSERT INTO hooks(agent,command) VALUES(?,?)", agent, command).lastrowid
        return {"hook": hid, "agent": agent}

    def del_hook(self, hid):
        with self.L:
            self.x("DELETE FROM hooks WHERE id=?", int(hid))
        return {"deleted": int(hid)}

    # ---------- scheduling ----------
    def _eligible(self, now):
        return self.q("SELECT * FROM jobs WHERE status='queued' AND (next_at IS NULL OR next_at<=?)", now)

    def _sched_jobs(self, rows, now):
        return [{"id": r["id"], "seq": r["seq"], "project": r["project"], "agent": team(r["agent"]),
                 "order_key": r["order_key"], "run_next_at": r["run_next_at"], "est": r["est"], "last_at": r["last_at"],
                 "wait": r["wait_accum"] + max(0.0, now - (r["elig_since"] or now)),
                 "wait0": r["wait_accum"] + max(0.0, now - (r["elig_since"] or now))} for r in rows]

    def _positions(self):
        now = time.time()
        rows = self._eligible(now)
        parts = self.parts()
        rem = 0.0
        if self.slot:
            rj = self.job(self.slot["job"])
            el = time.monotonic() - self.slot["t0"]
            rem = max(0.0, rj["est"] - el) if rj["est"] is not None else None
            for key, k in (("p", rj["project"]), ("a", (rj["project"], team(rj["agent"])))):
                parts[key][k] = parts[key].get(k, 0.0) + el
        order = sched.project_order(self._sched_jobs(rows, now), parts, self.cfg["overdue_after_seconds"], rem,
                                    self.cfg["fairness"])
        changed = False
        newpos = {jid: (i + 1, w) for i, (jid, w) in enumerate(order)}
        for r in self.q("SELECT id, position, est_wait FROM jobs WHERE status IN ('queued','held')"):
            pos, w = newpos.get(r["id"], (None, None))
            if r["position"] != pos or r["est_wait"] != w:
                self.x("UPDATE jobs SET position=?, est_wait=? WHERE id=?", pos, w, r["id"])
                changed = changed or r["position"] != pos
        if changed:
            self.event("queue", None, order=[jid for jid, _ in order][:20])

    def _tick(self):
        now = time.time()
        with self.tx():
            for r in self.q("SELECT * FROM jobs WHERE status IN ('queued','held') AND start_before IS NOT NULL "
                            "AND start_before<? AND attempt=0", now):
                self._finish_unstarted(r, "canceled", "queue_deadline")
            exp = self.cfg["held_expire_seconds"]
            if exp:
                for r in self.q("SELECT j.*, (SELECT MAX(ts) FROM actions a WHERE a.job_id=j.id AND a.action='hold') "
                                "held_at FROM jobs j WHERE status='held'"):
                    if r["held_at"] and r["held_at"] < now - exp:
                        self._finish_unstarted(r, "canceled", f"hold_expired: {r['hold_reason']}")
            self._positions()
        if self.slot or self.kv("paused", False) or self.stopping:
            return
        while True:
            rows = self._eligible(now)
            j, why = sched.pick(self._sched_jobs(rows, now), self.parts(), self.cfg["overdue_after_seconds"],
                                 self.cfg["fairness"])
            if not j:
                return
            r = self.job(j["id"])
            ident = json.loads(r["identity"] or "{}")
            if ident:
                rev = r["rev"]
                self.L.release()  # git can take seconds on a big repo; don't stall every other caller
                try:
                    cur = current_identity(ident, json.loads(r["cmd"]), r["cwd"])
                    dr = drift(ident, cur, r["cwd"])
                finally:
                    self.L.acquire()
                r = self.job(j["id"])
                if r["status"] != "queued" or r["rev"] != rev or self.slot or self.kv("paused", False) \
                        or self.stopping:
                    if self.slot or self.kv("paused", False) or self.stopping:
                        return
                    continue  # it changed meanwhile (canceled, held, reordered): pick again
                if dr:
                    with self.tx():
                        self._c_hold(r, "anvil", None)
                        self.upd(r["id"], hold_reason=f"drift: {dr}")
                        self.event("action_needed", r, why=f"drift: {dr}")
                        self._positions()
                    self._wake()
                    continue
            self._launch(r, why)
            return

    def _launch(self, j, why):
        jid, n = j["id"], j["attempt"] + 1
        ld = self.home / "logs" / jid
        ld.mkdir(parents=True, exist_ok=True)
        rd = self.home / "results" / jid
        rd.mkdir(parents=True, exist_ok=True)
        cmd = json.loads(j["cmd"])
        env = {k: os.environ[k] for k in ("PATH", "HOME", "USER", "LOGNAME", "LANG", "TMPDIR", "SHELL")
               if k in os.environ}
        env.update(PYTHONUNBUFFERED="1", PYTHONFAULTHANDLER="1")  # live logs; tracebacks on crashes
        env.update(json.loads(j["env"] or "{}"))
        env.update(ANVIL_JOB_ID=jid, ANVIL_ATTEMPT=str(n), ANVIL_RESULT_DIR=str(rd),
                   ANVIL_PROGRESS_FILE=str(ld / "progress.json"))
        exitf = ld / f"{n}.exit"
        exitf.unlink(missing_ok=True)
        now = time.time()
        log = open(ld / f"{n}.log", "ab")
        try:
            proc = subprocess.Popen([sys.executable, "-I", WRAP, str(exitf), *cmd], cwd=j["cwd"], env=env,
                                    stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                    start_new_session=True)
        except (OSError, ValueError, TypeError) as e:
            log.close()
            with self.tx():
                self.x("INSERT OR REPLACE INTO attempts(job_id,n,started,finished,status,reason,timeout) "
                       "VALUES(?,?,?,?,?,?,?)", jid, n, now, now, "failed", f"launch_error: {e}", j["timeout"])
                self.upd(jid, status="failed", attempt=n, started=now, finished=now, reason=f"launch_error: {e}",
                         position=None)
                self.event("finished", self.job(jid), status="failed", reason="launch_error")
            self._wake()
            return
        log.close()
        self.slot = {"job": jid, "proc": proc, "pgid": proc.pid, "t0": time.monotonic(), "acquired": now,
                     "limit": j["timeout"], "stop": None, "n": n}
        with self.tx():
            acc = j["wait_accum"] + max(0.0, now - (j["elig_since"] or now))
            self.x("INSERT OR REPLACE INTO attempts(job_id,n,started,pgid,status,timeout) VALUES(?,?,?,?,?,?)",
                   jid, n, now, proc.pid, "running", j["timeout"])
            self.upd(jid, status="running", sub=None, attempt=n, started=now, finished=None, pgid=proc.pid,
                     position=None, est_wait=None, elig_since=None, wait_accum=acc, run_next_at=None, last_at=None,
                     exit_code=None, signal=None, reason=None, progress=None)
            self._charge("p:" + j["project"], last=now)
            self._charge(f"a:{j['project']}\x1f{team(j['agent'])}", last=now)
            self.event("started", j, attempt=n, timeout_s=j["timeout"], turn=why)
            self._positions()
        self._wake()
        threading.Thread(target=self._monitor, args=(self.slot,), daemon=True).start()

    # ---------- execution ----------
    def _monitor(self, s):
        proc, pgid = s["proc"], s["pgid"]

        def alive():
            if proc is not None:
                return proc.poll() is None or group_alive(pgid)
            return group_alive(pgid)

        def leader_alive():
            return proc.poll() is None if proc is not None else group_alive(pgid)

        pf = self.home / "logs" / s["job"] / "progress.json"
        last_prog, last_prog_t = None, 0.0
        while leader_alive():
            if s["stop"] is None and s["limit"] is not None and time.monotonic() - s["t0"] >= s["limit"]:
                s["fired"], s["stop"] = s["limit"], "timeout"
            if s["stop"]:
                break
            if time.monotonic() - last_prog_t > 10 and pf.exists():
                last_prog_t = time.monotonic()
                try:
                    prog = json.loads(pf.read_text()[:4000])
                    if prog != last_prog:
                        last_prog = prog
                        if not isinstance(prog, dict):
                            prog = {"message": str(prog)}
                        frac = prog.get("fraction")
                        keep = {"message": str(prog.get("message", ""))[:200]}
                        if isinstance(frac, (int, float)) and not isinstance(frac, bool) and math.isfinite(frac):
                            keep["fraction"] = frac
                        with self.L:
                            with self.tx():
                                self.upd(s["job"], progress=json.dumps(keep))
                                self.event("progress", self.job(s["job"]))
                            self._wake()
                except (ValueError, OSError):
                    pass
            time.sleep(0.25)
        term_s = cleanup_s = None
        if alive():  # stop requested, or leader exited leaving owned processes behind
            if s["stop"] is None:
                s["stop_note"] = "orphans_cleaned"
            with self.L, self.tx():
                self.upd(s["job"], sub="stopping")
                self.event("stopping", self.job(s["job"]), why=s["stop"] or "orphans")
            with self.L:
                self._wake()
            if s["stop"] == "timeout":
                self._sample_stacks(s)
            t = time.monotonic()
            try:
                os.killpg(pgid, signal.SIGTERM)
            except OSError:
                pass
            while alive() and time.monotonic() - t < self.cfg["term_grace_seconds"]:
                time.sleep(0.1)
            term_s = time.monotonic() - t
            if alive():
                with self.L, self.tx():
                    self.upd(s["job"], sub="cleanup")
                try:
                    os.killpg(pgid, signal.SIGKILL)
                except OSError:
                    pass
            while alive():
                if time.monotonic() - t > 15 and not s.get("stuck"):
                    s["stuck"] = True
                    with self.L, self.tx():
                        self.upd(s["job"], sub="cleanup_stuck")
                        self.event("action_needed", self.job(s["job"]), why="processes survive SIGKILL")
                    with self.L:
                        self._wake()
                time.sleep(0.2)
            cleanup_s = time.monotonic() - t
        self._finalize(s, term_s, cleanup_s)

    def _sample_stacks(self, s):
        """Record where a timed-out job was spending its time (macOS `sample`, every process in the job's group
        except the wrapper), and point to it from the job's log."""
        secs = self.cfg["timeout_sample_seconds"]
        if not secs or not shutil.which("sample"):
            return
        pids = [p for p in (_sh(["pgrep", "-g", str(s["pgid"])], "/").decode().split()) if p != str(s["pgid"])][:4]
        ld = self.home / "logs" / s["job"]
        files = [ld / f"{s['n']}.stacks-{p}.txt" for p in pids]
        procs = [subprocess.Popen(["sample", p, str(int(secs)), "-file", str(f)], stdout=subprocess.DEVNULL,
                                  stderr=subprocess.DEVNULL) for p, f in zip(pids, files)]
        for pr in procs:
            try:
                pr.wait(timeout=secs + 20)
            except subprocess.TimeoutExpired:
                pr.kill()
        kept = [str(f) for f in files if f.exists()]
        if kept:
            with open(ld / f"{s['n']}.log", "a") as log:
                log.write(f"\n[anvil] time limit reached; stack samples of the running processes: {', '.join(kept)}\n")

    def _finalize(self, s, term_s, cleanup_s):
        """Record the outcome. Whatever goes wrong, the job ends and the GPU slot is freed: a stuck slot would
        stop every agent's work."""
        try:
            self._finalize_job(s, term_s, cleanup_s)
        except Exception as e:
            print(f"anvil finalize error for {s['job']}: {e!r}", file=sys.stderr, flush=True)
            with self.L:
                with contextlib.suppress(Exception), self.tx():
                    self.upd(s["job"], status="failed", sub=None, reason=f"internal error: {e!r}"[:200],
                             finished=time.time(), pgid=None)
                    self.event("finished", self.job(s["job"]), status="failed", reason="internal error")
                if self.slot is s:
                    self.slot = None
                with contextlib.suppress(Exception):
                    self._positions()
                self._wake()

    def _finalize_job(self, s, term_s, cleanup_s):
        jid, n = s["job"], s["n"]
        now = time.time()
        exitf = self.home / "logs" / jid / f"{n}.exit"
        rec = {}
        try:
            rec = json.loads(exitf.read_text())
        except (OSError, ValueError):
            pass
        code, sig = rec.get("exit_code"), rec.get("signal")
        if not rec and s.get("proc") is not None and s["proc"].returncode is not None:
            rc = s["proc"].returncode
            code, sig = (rc, None) if rc >= 0 else (None, -rc)
        stop = s["stop"]
        if stop == "cancel":
            status, reason = "canceled", "canceled"
        elif stop == "timeout":
            status, reason = "timed_out", f"limit {s.get('fired') or s['limit'] or 0:g}s reached"
        elif s.get("survivor") and not rec:
            status, reason = "interrupted", "service restarted; outcome unknown"
        elif code == 0:
            status, reason = "succeeded", None
        else:
            status = "failed"
            reason = f"signal {sig}" if sig else f"exit {code}" if code is not None else "unknown exit"
        if s.get("survivor") and rec and status in ("succeeded", "failed"):
            reason = (reason + "; " if reason else "") + "finished while service restarted"
        if s.get("stop_note"):
            reason = (reason + "; " if reason else "") + s["stop_note"]
        with self.L:
            j = self.job(jid)
            occ = now - s["acquired"]
            with self.tx():
                self._charge("p:" + j["project"], occ)
                self._charge(f"a:{j['project']}\x1f{team(j['agent'])}", occ)
                self.x("UPDATE attempts SET finished=?, exit_code=?, signal=?, status=?, reason=?, timeout=?, "
                       "term_s=?, cleanup_s=? WHERE job_id=? AND n=?", now, code, sig, status, reason, s["limit"],
                       term_s, cleanup_s, jid, n)
                retry = json.loads(j["retry"]) if j["retry"] else None
                if retry and self._retryable(retry, status, code, sig) and n < int(retry.get("max_attempts", 1)):
                    back = float(retry.get("backoff_seconds", 30)) * (2 ** (n - 1))
                    self.upd(jid, status="queued", sub=None, exit_code=code, signal=sig, reason=reason,
                             next_at=now + back, elig_since=now + back, finished=None, pgid=None)
                    self.event("retry_scheduled", j, attempt=n, status=status, reason=reason, backoff_s=back)
                else:
                    self.upd(jid, status=status, sub=None, exit_code=code, signal=sig, reason=reason,
                             finished=now, term_s=term_s, cleanup_s=cleanup_s, pgid=None)
                    self.event("finished", self.job(jid), status=status, exit_code=code, reason=reason)
                if self.slot is s:
                    self.slot = None
                self._positions()
            self._wake()

    @staticmethod
    def _retryable(retry, status, code, sig):
        if status == "failed" and code is not None and code in retry.get("exit_codes", []):
            return True
        on = retry.get("reasons", [])
        return (status == "timed_out" and "timed_out" in on) or (status == "failed" and sig and "signal" in on)

    # ---------- restart reconciliation ----------
    def _ours(self, r):
        """Is this running row's process group still our wrapper for this attempt? After a reboot the pgid may
        belong to anything (a shell, a daemon); adopting it would block the queue and later signal it."""
        exitf = self.home / "logs" / r["id"] / f"{r['attempt']}.exit"
        if not r["pgid"] or exitf.exists():
            return False
        args = _sh(["ps", "-o", "args=", "-p", str(r["pgid"])], "/").decode(errors="replace")
        return "wrap.py" in args and str(exitf) in args  # the wrapper's argv names this attempt's exit file

    def reconcile(self):
        with self.L:
            for r in self.q("SELECT * FROM jobs WHERE status='running'"):
                s = {"job": r["id"], "proc": None, "pgid": r["pgid"], "n": r["attempt"],
                     "acquired": r["started"] or time.time(),
                     "t0": time.monotonic() - (time.time() - (r["started"] or time.time())),
                     "limit": r["timeout"], "stop": None, "survivor": True}
                if self._ours(r) and not self.slot:
                    self.slot = s
                    with self.tx():
                        self.upd(r["id"], sub="reconciling")
                        self.event("reconciling", r)
                    threading.Thread(target=self._monitor, args=(s,), daemon=True).start()
                else:
                    self._finalize(s, None, None)

    # ---------- background loops ----------
    def run_scheduler(self):
        while not self.stopping:
            with self.L:
                try:
                    self._tick()
                    now = time.time()
                    nxt = [r["t"] for r in self.q(
                        "SELECT MIN(next_at) t FROM jobs WHERE status='queued' AND next_at>? UNION ALL "
                        "SELECT MIN(start_before) FROM jobs WHERE status IN ('queued','held') AND start_before>?",
                        now, now) if isinstance(r["t"], (int, float))]
                    delay = max(0.05, min([30.0] + [t - now for t in nxt]))
                except Exception as e:  # never let the loop die; surface the problem
                    print(f"anvil scheduler error: {e!r}", file=sys.stderr, flush=True)
                    delay = 5.0
                self.kick_cv.wait(delay)

    def run_hooks(self):
        while not self.stopping:
            time.sleep(1)
            try:
                self._deliver_due()
            except Exception as e:  # a bad hook entry must not end delivery for everyone
                print(f"anvil hook error: {e!r}", file=sys.stderr, flush=True)

    def _deliver_due(self):
        with self.L:
            due = self.q("SELECT * FROM deliveries WHERE done=0 AND next_at<=? LIMIT 20", time.time())
        for d in due:
            with self.L:
                ev = self.q1("SELECT * FROM events WHERE id=?", d["event_id"])
                j = self.q1("SELECT * FROM jobs WHERE id=?", ev["job_id"]) if ev else None
                cmd = self._hook_cmd(d["hook"])
                st = self.status(j["id"], j) if j else {}
            if not (ev and j and cmd):
                with self.L:
                    self.x("UPDATE deliveries SET done=1, err='hook or event gone' WHERE event_id=? AND hook=?",
                           d["event_id"], d["hook"])
                continue
            receipt = {"event": ev["id"], "kind": ev["kind"], **{k: v for k, v in st.items()
                       if k in ("id", "project", "agent", "name", "status", "exit_code", "reason", "elapsed_s",
                                "why")}}
            env = dict(os.environ, ANVIL_EVENT_ID=str(ev["id"]), ANVIL_JOB_ID=j["id"], ANVIL_AGENT=j["agent"])
            ok, err = self._run_hook(cmd, receipt, env)
            with self.L:
                tries = d["tries"] + 1
                done = 1 if ok or tries >= 6 else 0
                self.x("UPDATE deliveries SET tries=?, done=?, err=?, next_at=? WHERE event_id=? AND hook=?",
                       tries, done, None if ok else err, time.time() + 2 ** tries, d["event_id"], d["hook"])

    def _run_hook(self, cmd, receipt, env):
        """Run one hook in its own process group; on timeout the whole group is killed (a hook that starts an
        agent turn may take a while: hook_timeout_seconds)."""
        try:
            p = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.PIPE, env=env, start_new_session=True)
        except OSError as e:
            return False, str(e)[:200]
        try:
            _, err = p.communicate(json.dumps(receipt).encode(), timeout=self.cfg["hook_timeout_seconds"])
        except subprocess.TimeoutExpired:
            with contextlib.suppress(OSError):
                os.killpg(p.pid, signal.SIGKILL)
            p.communicate()
            return False, "timed out"
        return p.returncode == 0, err.decode(errors="replace")[-200:] or f"exit {p.returncode}"

    def _hook_cmd(self, key):
        kind, i = key.split(":")
        if kind == "db":
            r = self.q1("SELECT command FROM hooks WHERE id=?", int(i))
            return r["command"] if r else None
        hs = self.cfg["hooks"]
        return hs[int(i)].get("command") if int(i) < len(hs) else None

    def run_probe(self):
        pats = self.cfg["outside_gpu_patterns"]
        while not self.stopping:
            try:
                out = subprocess.run(["ps", "-axo", "pid=,pgid=,args="], capture_output=True, timeout=5).stdout
                s = self.slot
                mine = {os.getpid(), s["pgid"] if s else -1}
                found = []
                for line in out.decode(errors="replace").splitlines():
                    parts = line.split(None, 2)
                    if len(parts) < 3:
                        continue
                    pid, pgid, args = int(parts[0]), int(parts[1]), parts[2]
                    if pid in mine or pgid in mine or "anvil" in args or "ps -axo" in args:
                        continue
                    argv = args.split()
                    # program, script and module args only: paths elsewhere in the args are not evidence
                    head = " ".join([os.path.basename(argv[0])] + [x for x in argv[1:4] if not x.startswith("/")]
                                    + [os.path.basename(x) for x in argv[1:3] if x.startswith("/")])
                    if any(p in head for p in pats):
                        found.append({"pid": pid, "cmd": args[:100]})
                if found != self.outside:
                    self.outside = found[:10]
                    with self.L, self.tx():
                        self.event("outside", None, count=len(found))
                    with self.L:
                        self._wake()
            except Exception:
                pass
            time.sleep(self.cfg["probe_interval_seconds"])

    def run_retention(self):
        while not self.stopping:
            try:
                self.prune()
            except Exception as e:
                print(f"anvil retention error: {e!r}", file=sys.stderr, flush=True)
            time.sleep(600)

    def prune(self):
        ret = self.cfg["log_retention"]
        cutoff = time.time() - ret["max_age_days"] * 86400
        with self.L:
            rows = self.q(f"SELECT id, finished, status, acked FROM jobs WHERE status IN {FINAL} AND logs_removed=0 "
                          "ORDER BY finished")
            self.x("DELETE FROM events WHERE ts<? AND id<(SELECT MAX(id) FROM events)", time.time() - 30 * 86400)
        sizes = {r["id"]: sum(p.stat().st_size for p in (self.home / "logs" / r["id"]).glob("*")
                              if p.is_file()) if (self.home / "logs" / r["id"]).exists() else 0 for r in rows}
        total = sum(sizes.values())
        limit = ret["max_total_mb"] * 1024 * 1024
        for r in rows:
            protected = r["status"] in ATTENTION and not r["acked"]
            if protected or not ((r["finished"] or 0) < cutoff or total > limit):
                continue
            shutil.rmtree(self.home / "logs" / r["id"], ignore_errors=True)
            total -= sizes[r["id"]]
            with self.L:
                self.x("UPDATE jobs SET logs_removed=1 WHERE id=?", r["id"])

    def start(self):
        self.reconcile()
        for fn in (self.run_scheduler, self.run_hooks, self.run_probe, self.run_retention):
            threading.Thread(target=fn, daemon=True).start()
