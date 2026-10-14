"""Anvil core: durable store, fair scheduler, process runner, events, hooks, retention."""
import hashlib
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

from . import config, sched

WRAP = str(Path(__file__).with_name("wrap.py"))
FINAL = ("succeeded", "failed", "canceled", "timed_out", "interrupted")
ATTENTION = ("failed", "timed_out", "interrupted")
NOTE_MAX = 500

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs(
 seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE, project TEXT, agent TEXT, name TEXT, note TEXT,
 est REAL, status TEXT, sub TEXT, timeout REAL, start_before REAL, cmd TEXT, cwd TEXT, env TEXT,
 identity TEXT, skey TEXT UNIQUE, rerun_of TEXT, retry TEXT, attempt INTEGER DEFAULT 0, next_at REAL,
 hold_reason TEXT, rev INTEGER DEFAULT 1, submitted REAL, started REAL, finished REAL, exit_code INTEGER,
 signal INTEGER, reason TEXT, progress TEXT, order_key REAL, wait_accum REAL DEFAULT 0, elig_since REAL,
 run_next_at REAL, run_next_by TEXT, run_next_reason TEXT, position INTEGER, est_wait REAL, pgid INTEGER,
 acked INTEGER DEFAULT 0, logs_removed INTEGER DEFAULT 0, term_s REAL, cleanup_s REAL,
 hidden INTEGER DEFAULT 0);  -- hidden: removed from views; the compact receipt stays
CREATE INDEX IF NOT EXISTS jobs_status ON jobs(status);
CREATE TABLE IF NOT EXISTS attempts(job_id TEXT, n INTEGER, started REAL, finished REAL, pgid INTEGER,
 exit_code INTEGER, signal INTEGER, status TEXT, reason TEXT, timeout REAL, term_s REAL, cleanup_s REAL,
 PRIMARY KEY(job_id, n));
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, kind TEXT, job_id TEXT,
 project TEXT, agent TEXT, data TEXT);
CREATE TABLE IF NOT EXISTS actions(id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, job_id TEXT, actor TEXT,
 action TEXT, detail TEXT);
CREATE TABLE IF NOT EXISTS parts(key TEXT PRIMARY KEY, charge REAL DEFAULT 0, last REAL);
CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS hooks(id INTEGER PRIMARY KEY AUTOINCREMENT, agent TEXT, command TEXT);
CREATE TABLE IF NOT EXISTS deliveries(event_id INTEGER, hook TEXT, tries INTEGER DEFAULT 0, next_at REAL,
 done INTEGER DEFAULT 0, err TEXT, PRIMARY KEY(event_id, hook));
"""


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


def snapshot_identity(cmd, cwd):
    """Checkout/build identity: git HEAD + hash of uncommitted changes + executable/script stats."""
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


def drift(old, new):
    out = []
    if old.get("git_head") != new.get("git_head"):
        out.append(f"git HEAD {str(old.get('git_head'))[:8]}→{str(new.get('git_head'))[:8]}")
    elif old.get("git_changes") != new.get("git_changes"):
        out.append("uncommitted changes differ")
    for f, v in (old.get("files") or {}).items():
        if (new.get("files") or {}).get(f) != v:
            out.append(f"{os.path.basename(f)} changed")
    return "; ".join(out)


class Service:
    def __init__(self):
        self.cfg = config.load()
        self.home = config.home()
        (self.home / "logs").mkdir(exist_ok=True)
        (self.home / "results").mkdir(exist_ok=True)
        self.db = sqlite3.connect(self.home / "anvil.db", check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript(SCHEMA)
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
                svc._depth = getattr(svc, "_depth", 0) + 1
                if svc._depth == 1:
                    svc.db.execute("BEGIN IMMEDIATE")

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
        if not any(r["project"] == project and r["agent"] == agent for r in act):
            others = [self.q1("SELECT charge FROM parts WHERE key=?", f"a:{project}\x1f{r['agent']}")
                      for r in act if r["project"] == project]
            vals = [o["charge"] for o in others if o]
            self._charge(f"a:{project}\x1f{agent}", floor=min(vals) if vals else None)

    # ---------- permissions ----------
    def is_operator(self, actor):
        ops = self.cfg["operators"]
        return "*" in ops or actor in ops or actor.split(":")[0] in ops

    def authorize(self, job, actor):
        if actor != job["agent"] and not self.is_operator(actor):
            raise Err("forbidden", f"{actor} may only control its own jobs (operators: config.json)", 403)

    # ---------- submit ----------
    def submit(self, d, actor=None):
        for k in ("project", "agent", "name", "cmd"):
            if not d.get(k):
                raise Err("invalid", f"missing {k}")
        cmd = d["cmd"]
        if not isinstance(cmd, list) or not all(isinstance(c, str) for c in cmd):
            raise Err("invalid", "cmd must be a list of strings")
        timeout = self.limit(d.get("timeout_seconds"), self.cfg["default_timeout_seconds"])
        est = d.get("estimated_seconds")
        if est is not None and float(est) < 0:
            raise Err("invalid", "estimated_seconds must be >= 0")
        note = (d.get("note") or "")
        if len(note) > NOTE_MAX:
            raise Err("invalid", f"note longer than {NOTE_MAX} chars")
        cwd = d.get("cwd") or os.getcwd()
        if not os.path.isdir(cwd):
            raise Err("invalid", f"cwd does not exist: {cwd}")
        exe, path = cmd[0] if cmd else "", (d.get("env") or {}).get("PATH") or os.environ.get("PATH", "")
        found = (os.access(os.path.join(cwd, exe), os.X_OK) if "/" in exe else shutil.which(exe, path=path))
        if not found:  # fail now, not after the queue wait
            raise Err("command_not_found", f"{exe!r} not found or not executable (PATH as submitted)")
        retry = d.get("retry")
        if retry:
            if int(retry.get("max_attempts", 1)) < 1:
                raise Err("invalid", "retry.max_attempts must be >= 1")
        with self.L:
            if d.get("submission_key"):
                r = self.q1("SELECT id FROM jobs WHERE skey=?", d["submission_key"])
                if r:
                    return dict(self.status(r["id"]), duplicate=True)
            now = time.time()
            ident = snapshot_identity(cmd, cwd) if d.get("check_drift", True) else {}
            with self.tx():
                self._baseline(d["project"], d["agent"])
                cur = self.x("""INSERT INTO jobs(project,agent,name,note,est,status,timeout,start_before,cmd,cwd,env,
                    identity,skey,rerun_of,retry,next_at,submitted,order_key,elig_since)
                    VALUES(?,?,?,?,?,'queued',?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                             d["project"], d["agent"], d["name"][:120], note or None,
                             float(est) if est is not None else None, timeout, d.get("start_before"),
                             json.dumps(cmd), cwd, json.dumps(d.get("env") or {}), json.dumps(ident),
                             d.get("submission_key"), d.get("rerun_of"),
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
        if j["status"] == "queued" and j["position"]:
            d["approx_wait_s"] = j["est_wait"]
        el = self.elapsed(j)
        if el is not None:
            d["elapsed_s"] = round(el)
        if j["attempt"] > 1 or j["retry"]:
            d["attempt"] = j["attempt"]
        if j["status"] in FINAL:
            d.update(exit_code=j["exit_code"], reason=j["reason"])
            if j["signal"]:
                d["signal"] = j["signal"]
        if j["progress"]:
            d["progress"] = json.loads(j["progress"])
        if j["run_next_at"]:
            d["run_next"] = {"by": j["run_next_by"], "reason": j["run_next_reason"]}
        if j["rerun_of"]:
            d["rerun_of"] = j["rerun_of"]
        return d

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

    def overview(self, project=None, agent=None, history=8):
        with self.L:
            f, a = "", []
            if project:
                f += " AND project=?"
                a.append(project)
            if agent:
                f += " AND agent=?"
                a.append(agent)
            queue = self.q(f"SELECT * FROM jobs WHERE status IN ('running','queued','held'){f} "
                           "ORDER BY status!='running', position IS NULL, position, seq", *a)
            recent = self.q(f"SELECT * FROM jobs WHERE status IN {FINAL} AND hidden=0{f} ORDER BY finished DESC LIMIT ?",
                            *a, int(history))
            names = self.q("SELECT DISTINCT project, agent FROM jobs WHERE submitted > ?", time.time() - 7 * 86400)
            return {"gpu": self.gpu_state(), "paused": self.kv("paused", False),
                    "active": self.slot["job"] if self.slot else None,
                    "queue": [self.status(r["id"], r) for r in queue],
                    "recent": [self.status(r["id"], r) for r in recent],
                    "outside": self.outside, "cursor": self.cursor(),
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

    def wait(self, job=None, revision=None, cursor=None, agent=None, project=None, timeout=30, until_done=False):
        """Bounded wait. With job+revision: returns when that job's record changes. With cursor: returns new
        events. Returns {'unchanged': True, ...} on timeout."""
        timeout = max(0.0, min(float(timeout or 30), 600))
        deadline = time.monotonic() + timeout
        while True:
            with self.L:
                if job:
                    j = self.job(job)
                    done = j["status"] in FINAL
                    changed = revision is None or j["rev"] != int(revision)
                    if (changed and not until_done) or (until_done and done):
                        return self.status(job, j)
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
                if job:
                    out.update(id=job, rev=j["rev"], status=j["status"])
                return out
            with self.ev_cv:
                self.ev_cv.wait(min(left, 5))

    # ---------- controls ----------
    def control(self, jid, op, actor, **kw):
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
            f["identity"] = json.dumps(snapshot_identity(json.loads(j["cmd"]), j["cwd"]))
        self.upd(j["id"], status="queued", hold_reason=None, elig_since=time.time(), **f)
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

    def clear_finished(self, actor, project=None, agent=None):
        """Remove every finished job (optionally one project/agent) from views."""
        with self.L:
            f, a = "", []
            for col, v in (("project", project), ("agent", agent)):
                if v:
                    f += f" AND {col}=?"
                    a.append(v)
            rows = self.q(f"SELECT * FROM jobs WHERE status IN {FINAL} AND hidden=0{f}", *a)
            for r in rows:
                self.authorize(r, actor)
            with self.tx():
                for r in rows:
                    self._c_remove(r, actor)
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
             "retry": json.loads(j["retry"]) if j["retry"] else None, "rerun_of": j["id"]}
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
        return [{"id": r["id"], "seq": r["seq"], "project": r["project"], "agent": r["agent"],
                 "order_key": r["order_key"], "run_next_at": r["run_next_at"], "est": r["est"],
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
            for key, k in (("p", rj["project"]), ("a", (rj["project"], rj["agent"]))):
                parts[key][k] = parts[key].get(k, 0.0) + el
        order = sched.project_order(self._sched_jobs(rows, now), parts, self.cfg["overdue_after_seconds"], rem)
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
            self._positions()
        if self.slot or self.kv("paused", False) or self.stopping:
            return
        while True:
            rows = self._eligible(now)
            j, why = sched.pick(self._sched_jobs(rows, now), self.parts(), self.cfg["overdue_after_seconds"])
            if not j:
                return
            r = self.job(j["id"])
            ident = json.loads(r["identity"] or "{}")
            if ident:
                dr = drift(ident, snapshot_identity(json.loads(r["cmd"]), r["cwd"]))
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
        except OSError as e:
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
                     position=None, est_wait=None, elig_since=None, wait_accum=acc, run_next_at=None,
                     exit_code=None, signal=None, reason=None, progress=None)
            self._charge("p:" + j["project"], last=now)
            self._charge(f"a:{j['project']}\x1f{j['agent']}", last=now)
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
                s["stop"] = "timeout"
            if s["stop"]:
                break
            if time.monotonic() - last_prog_t > 10 and pf.exists():
                last_prog_t = time.monotonic()
                try:
                    prog = json.loads(pf.read_text()[:500])
                    if prog != last_prog:
                        last_prog = prog
                        with self.L:
                            with self.tx():
                                self.upd(s["job"], progress=json.dumps(prog)[:300])
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
            t = time.monotonic()
            try:
                os.killpg(pgid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            while alive() and time.monotonic() - t < self.cfg["term_grace_seconds"]:
                time.sleep(0.1)
            term_s = time.monotonic() - t
            if alive():
                with self.L, self.tx():
                    self.upd(s["job"], sub="cleanup")
                try:
                    os.killpg(pgid, signal.SIGKILL)
                except ProcessLookupError:
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

    def _finalize(self, s, term_s, cleanup_s):
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
            status, reason = "timed_out", f"limit {s['limit']:g}s reached"
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
                self._charge(f"a:{j['project']}\x1f{j['agent']}", occ)
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
    def reconcile(self):
        with self.L:
            for r in self.q("SELECT * FROM jobs WHERE status='running'"):
                s = {"job": r["id"], "proc": None, "pgid": r["pgid"], "n": r["attempt"], "acquired": time.time(),
                     "t0": time.monotonic() - (time.time() - (r["started"] or time.time())),
                     "limit": r["timeout"], "stop": None, "survivor": True}
                if r["pgid"] and group_alive(r["pgid"]) and not self.slot:
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
                except Exception as e:  # never let the loop die; surface the problem
                    print(f"anvil scheduler error: {e!r}", file=sys.stderr, flush=True)
                now = time.time()
                nxt = [r["t"] for r in self.q(
                    "SELECT MIN(next_at) t FROM jobs WHERE status='queued' AND next_at>? UNION ALL "
                    "SELECT MIN(start_before) FROM jobs WHERE status IN ('queued','held') AND start_before>?",
                    now, now) if r["t"]]
                self.kick_cv.wait(max(0.05, min([30.0] + [t - now for t in nxt])))

    def run_hooks(self):
        while not self.stopping:
            time.sleep(1)
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
                try:
                    r = subprocess.run(cmd, shell=True, input=json.dumps(receipt).encode(), env=env,
                                       capture_output=True, timeout=15)
                    ok, err = r.returncode == 0, r.stderr.decode()[-200:] or f"exit {r.returncode}"
                except (OSError, subprocess.TimeoutExpired) as e:
                    ok, err = False, str(e)[:200]
                with self.L:
                    tries = d["tries"] + 1
                    done = 1 if ok or tries >= 6 else 0
                    self.x("UPDATE deliveries SET tries=?, done=?, err=?, next_at=? WHERE event_id=? AND hook=?",
                           tries, done, None if ok else err, time.time() + 2 ** tries, d["event_id"], d["hook"])

    def _hook_cmd(self, key):
        kind, i = key.split(":")
        if kind == "db":
            r = self.q1("SELECT command FROM hooks WHERE id=?", int(i))
            return r["command"] if r else None
        hs = self.cfg["hooks"]
        return hs[int(i)]["command"] if int(i) < len(hs) else None

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
