"""anvil — CLI for the local GPU queue. Prints compact JSON (the agent contract) unless noted."""
import argparse
import http.client
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from . import config

ENV_CAPTURE = ("PATH", "VIRTUAL_ENV", "CONDA_PREFIX", "CONDA_DEFAULT_ENV", "PYTHONPATH")
ROOT = Path(__file__).parent.parent
AGENTS = Path.home() / "Library/LaunchAgents"
APP = config.APP


SANDBOXED = {"error": "permission_denied",
             "message": "the agent sandbox blocks Anvil's local service; rerun this anvil command with "
                        "permission to run outside the sandbox"}


class ApiError(Exception):
    def __init__(self, obj):
        self.obj = obj


class UnixConn(http.client.HTTPConnection):
    def __init__(self, timeout):
        super().__init__("anvil", timeout=timeout)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(str(config.sock()))


def api(method, path, body=None, timeout=10, autostart=True):
    c = UnixConn(timeout)
    try:
        c.request(method, path, json.dumps(body) if body is not None else None,
                  {"Content-Type": "application/json"})
        r = c.getresponse()
        data = json.loads(r.read() or b"{}")
    except (FileNotFoundError, ConnectionRefusedError) as e:
        if autostart:
            start_daemon()
            return api(method, path, body, timeout, autostart=False)
        raise ApiError({"error": "unavailable", "message": f"anvil service not running: {e}"})
    except PermissionError as e:
        raise ApiError(SANDBOXED | {"message": f"{SANDBOXED['message']} ({e})"})
    except OSError as e:
        raise ApiError({"error": "unavailable", "message": f"anvil service unreachable: {e}"})
    finally:
        c.close()
    if r.status >= 400:
        raise ApiError(data)
    return data


def start_daemon():
    log = open(config.home() / "service.log", "ab")
    subprocess.Popen([sys.executable, "-m", "anvil", "serve"], stdout=log, stderr=log, stdin=subprocess.DEVNULL,
                     start_new_session=True, cwd=str(ROOT))
    for _ in range(50):  # wait until the service accepts connections (a stale socket file may still exist)
        time.sleep(0.1)
        s = socket.socket(socket.AF_UNIX)
        try:
            s.connect(str(config.sock()))
            return
        except OSError:
            pass
        finally:
            s.close()


def qs(**kw):
    from urllib.parse import urlencode
    kw = {k: v for k, v in kw.items() if v is not None and v is not False}
    return ("?" + urlencode({k: (1 if v is True else v) for k, v in kw.items()})) if kw else ""


def out(obj):
    print(json.dumps(obj, separators=(",", ":"), ensure_ascii=False))


def actor(a):
    return a.actor or os.environ.get("ANVIL_AGENT") or "human"


def default_project(cwd):
    top = subprocess.run(["git", "rev-parse", "--show-toplevel"], cwd=cwd, capture_output=True, text=True).stdout
    return Path(top.strip() or cwd).name


def parse_deadline(v):
    if v is None:
        return None
    if v.startswith("+"):
        return time.time() + float(v[1:])
    return datetime.fromisoformat(v).timestamp()


# ---------------- human views ----------------
def fmt_s(s):
    if s is None:
        return "—"
    s = int(s)
    return f"{s // 3600}h{s % 3600 // 60:02d}m" if s >= 3600 else f"{s // 60}m{s % 60:02d}s" if s >= 60 else f"{s}s"


def render(o, stale=False, width=None):
    width = width or shutil.get_terminal_size((110, 30)).columns
    B, D, R, G, Y, X = "\033[1m", "\033[2m", "\033[31m", "\033[32m", "\033[33m", "\033[0m"
    dot = {"idle": D + "○ idle", "running": G + "● running", "paused": Y + "❚❚ paused",
           "attention": R + "▲ needs attention"}[o["gpu"]] + X
    lines = [f"{B}Anvil{X}  GPU {dot}   scheduling {'paused' if o['paused'] else 'running'}"
             + (f"   {R}STALE — service unreachable{X}" if stale else "")]
    if o["outside"]:
        lines.append(f"{Y}outside GPU work (best-effort): " +
                     ", ".join(f"{p['pid']} {p['cmd'][:40]}" for p in o["outside"][:3]) + X)
    hdr = f"{'ID':<7}{'PROJECT':<14}{'AGENT':<14}{'NAME':<22}{'STATUS':<11}{'POS':<5}{'ELAPSED/LIMIT':<16}{'EST*':<8}WAITING"
    lines += ["", B + hdr[:width] + X]
    for j in o["queue"]:
        st = j["status"] + ("·" + j["sub"] if j.get("sub") else "")
        cap = fmt_s(j["timeout_s"]) if j["timeout_s"] else "none"
        lim = f"{fmt_s(j.get('elapsed_s'))}/{cap}" if j["status"] == "running" else f"—/{cap}"
        why = j.get("why") or (j.get("progress") or {}).get("message", "")
        if j.get("approx_wait_s") is not None:
            why += f" ~{fmt_s(j['approx_wait_s'])}"
        row = (f"{j['id']:<7}{j['project'][:13]:<14}{j['agent'][:13]:<14}{j['name'][:21]:<22}{st[:10]:<11}"
               f"{str(j['position'] or ''):<5}{lim:<16}{fmt_s(j['est_s']):<8}{why}")
        lines.append((G if j["status"] == "running" else D if j["status"] == "held" else "") + row[:width] + X)
        if j.get("note"):
            lines.append(D + "       " + j["note"][: width - 8] + X)
    if not o["queue"]:
        lines.append(D + "  queue empty" + X)
    if o["recent"]:
        lines += ["", B + "Recent" + X]
        for j in o["recent"]:
            c = G if j["status"] == "succeeded" else R if j["status"] in ("failed", "timed_out", "interrupted") else D
            lines.append(f"{j['id']:<7}{j['project'][:13]:<14}{j['agent'][:13]:<14}{j['name'][:21]:<22}"
                         f"{c}{j['status']:<11}{X}{fmt_s(j.get('elapsed_s')):<8}{(j.get('reason') or '')[:40]}")
    lines.append(D + "\n* estimates are agent-provided and advisory" + X)
    return "\n".join(lines)


def cmd_top(a):
    q = qs(project=a.project, agent=a.agent)
    o, stale = None, False
    sys.stdout.write("\033[?25l")
    try:
        while True:
            try:
                o, stale = api("GET", "/api/overview" + q, autostart=False), False
            except ApiError:
                stale = True
            if o:
                sys.stdout.write("\033[H\033[2J" + render(o, stale) + "\n")
                sys.stdout.flush()
            try:
                api("GET", "/api/wait" + qs(cursor=o["cursor"] if o else None, timeout=1), timeout=5,
                    autostart=False)
            except ApiError:
                time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        sys.stdout.write("\033[?25h")


# ---------------- commands ----------------
def cmd_submit(a):
    cwd = os.path.abspath(a.cwd or os.getcwd())
    cmd = a.cmd[1:] if a.cmd and a.cmd[0] == "--" else a.cmd
    if not cmd:
        raise ApiError({"error": "invalid", "message": "missing command after --"})
    env = {k: os.environ[k] for k in ENV_CAPTURE if k in os.environ}
    for k in a.pass_env or []:
        if k in os.environ:
            env[k] = os.environ[k]
    for kv in a.env or []:
        k, _, v = kv.partition("=")
        env[k] = v
    exe = shutil.which(cmd[0], path=env.get("PATH")) if "/" not in cmd[0] else None
    if exe:
        cmd = [exe] + cmd[1:]
    retry = None
    if a.max_attempts and a.max_attempts > 1:
        retry = {"max_attempts": a.max_attempts, "backoff_seconds": a.backoff,
                 "exit_codes": [int(c) for c in (a.retry_codes or "").split(",") if c],
                 "reasons": [r for r in (a.retry_on or "").split(",") if r]}
    body = {"project": a.project or default_project(cwd), "agent": a.agent or os.environ.get("ANVIL_AGENT")
            or os.environ.get("USER", "human"), "name": a.name or Path(cmd[-1]).name, "note": a.note,
            "estimated_seconds": a.estimate, "timeout_seconds": a.timeout, "start_before": parse_deadline(a.start_before),
            "cmd": cmd, "cwd": cwd, "env": env, "submission_key": a.key, "retry": retry,
            "check_drift": not a.no_drift_check}
    body["actor"] = a.actor or body["agent"]
    out(api("POST", "/api/submit", body))


def time_limit(v):  # named for argparse's "invalid time_limit value" message
    return "none" if v.lower() in ("0", "none") else float(v)


def control(op, **extra):
    def f(a):
        body = {"actor": actor(a), **{k: getattr(a, k) for k in extra if getattr(a, k, None) is not None}}
        out(api("POST", f"/api/jobs/{a.id}/{op}", body))
    return f


def cmd_wait(a):
    deadline = time.monotonic() + a.timeout
    while True:
        left = max(1, deadline - time.monotonic())
        r = api("GET", "/api/wait" + qs(job=a.id, revision=a.rev, cursor=a.cursor, agent=a.agent,
                                        timeout=min(left, 300), until_done=a.until_done), timeout=min(left, 300) + 10)
        if not r.get("unchanged") or time.monotonic() >= deadline:
            return out(r)


def cmd_logs(a):
    r = api("GET", f"/api/jobs/{a.id}/logs" + qs(tail=a.tail, since=a.since, attempt=a.attempt))
    if a.raw:
        sys.stdout.write(r["data"])
    else:
        out(r)


def build_app():
    """Compile the native menu bar app (Swift/AppKit, no packages) into ~/Applications/Anvil.app."""
    macos = APP / "Contents/MacOS"
    macos.mkdir(parents=True, exist_ok=True)
    (APP / "Contents/Resources").mkdir(exist_ok=True)
    (APP / "Contents/Info.plist").write_text(f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>CFBundleIdentifier</key><string>dev.anvil.app</string><key>CFBundleName</key><string>Anvil</string>
<key>CFBundleExecutable</key><string>Anvil</string><key>CFBundlePackageType</key><string>APPL</string>
<key>CFBundleShortVersionString</key><string>1.0</string><key>LSMinimumSystemVersion</key><string>14.0</string>
<key>LSUIElement</key><true/>
</dict></plist>
""")
    # How the app starts the service when it isn't running.
    (APP / "Contents/Resources/service.json").write_text(json.dumps(
        {"python": sys.executable, "root": str(ROOT), "home": str(config.home())}))
    r = subprocess.run(["xcrun", "swiftc", "-O", "-parse-as-library", "-o", str(macos / "Anvil"),
                        str(Path(__file__).with_name("Anvil.swift"))], capture_output=True, text=True)
    if r.returncode:
        raise ApiError({"error": "build_failed", "message": r.stderr[-1500:]})
    subprocess.run(["codesign", "-s", "-", "--force", str(APP)], capture_output=True)


def cmd_app(a):
    if a.build or not (APP / "Contents/MacOS/Anvil").exists():
        build_app()
    subprocess.run(["open", str(APP)])
    out({"app": str(APP)})


def remove_login_items():
    """Older versions started the service and app at login; Anvil now starts only when something uses it."""
    for label in ("dev.anvil.app", "dev.anvil.queue"):
        p = AGENTS / f"{label}.plist"
        subprocess.run(["launchctl", "bootout", f"gui/{os.getuid()}", str(p)], capture_output=True)
        p.unlink(missing_ok=True)


# Agent apps that get the anvil-mcp server: (remove command, add command) for each app's own CLI.
MCP_CLIENTS = {"claude": (["claude", "mcp", "remove", "--scope", "user", "anvil"],
                          ["claude", "mcp", "add", "--scope", "user", "anvil", "--"]),
               "codex": (["codex", "mcp", "remove", "anvil"], ["codex", "mcp", "add", "anvil", "--"])}


def register_mcp(add):
    """Register (or remove) anvil-mcp in Claude Code and Codex where they are installed."""
    exe = shutil.which("anvil-mcp") or str(Path(sys.executable).with_name("anvil-mcp"))
    done = {}
    for name, (rm, mk) in MCP_CLIENTS.items():
        if not shutil.which(name):
            done[name] = "not installed"
            continue
        subprocess.run(rm, capture_output=True)  # re-adding refreshes the path after a reinstall
        r = subprocess.run(mk + [exe], capture_output=True, text=True) if add else None
        done[name] = (r.returncode == 0 or (r.stderr or r.stdout).strip()[-300:]) if add else "removed"
    return done


def cmd_install(a):
    build_app()
    out({"app": str(APP), "mcp": register_mcp(True)})


def cmd_uninstall(a):
    remove_login_items()
    register_mcp(False)
    subprocess.run(["osascript", "-e", 'quit app id "dev.anvil.app"'], capture_output=True)
    shutil.rmtree(APP, ignore_errors=True)
    out({"uninstalled": True, "removed": str(APP)})


def main(argv=None):
    remove_login_items()
    p = argparse.ArgumentParser(prog="anvil", description="Local fair GPU queue. Output is compact JSON.")
    p.add_argument("--actor", help="who is acting (default $ANVIL_AGENT or 'human')")
    sp = p.add_subparsers(dest="cmd_name", required=True)

    def sub(name, fn, help):
        s = sp.add_parser(name, help=help)
        s.set_defaults(fn=fn)
        s.add_argument("--actor", default=argparse.SUPPRESS)
        return s

    s = sub("submit", cmd_submit, "queue a GPU job: anvil submit -n NAME [opts] -- CMD...")
    s.add_argument("-n", "--name")
    s.add_argument("-p", "--project", help="default: git repo / directory name")
    s.add_argument("-a", "--agent", help="stable agent id (default $ANVIL_AGENT)")
    s.add_argument("--note")
    s.add_argument("-e", "--estimate", type=float, help="estimated run seconds (advisory)")
    s.add_argument("-t", "--timeout", type=time_limit, help="hard time limit, seconds from launch "
                   "(default: none, or the config default; 0 or none = no limit)")
    s.add_argument("--start-before", help="cancel if not started by: +SECONDS or ISO time")
    s.add_argument("--key", help="submission key; a repeat returns the original job")
    s.add_argument("--cwd")
    s.add_argument("--env", action="append", metavar="K=V")
    s.add_argument("--pass-env", action="append", metavar="K")
    s.add_argument("--max-attempts", type=int)
    s.add_argument("--retry-codes", help="comma list of retryable exit codes")
    s.add_argument("--retry-on", help="comma list: timed_out,signal")
    s.add_argument("--backoff", type=float, default=30)
    s.add_argument("--no-drift-check", action="store_true")
    s.add_argument("cmd", nargs=argparse.REMAINDER)

    s = sub("status", lambda a: out(api("GET", f"/api/jobs/{a.id}" if a.id else "/api/overview")),
            "one job, compact; without an id, the whole queue")
    s.add_argument("id", nargs="?")
    s = sub("show", lambda a: out(api("GET", f"/api/jobs/{a.id}/detail")), "full record: command, attempts, history")
    s.add_argument("id")
    s = sub("list", lambda a: out(api("GET", "/api/jobs" + qs(project=a.project, agent=a.agent, status=a.status,
                                                              limit=a.limit, after=a.after))), "list jobs (paginated)")
    for f in ("--project", "--agent", "--status", "--after"):
        s.add_argument(f)
    s.add_argument("--limit", type=int, default=20)
    s = sub("wait", cmd_wait, "bounded wait for a change")
    s.add_argument("id", nargs="?")
    s.add_argument("--rev", type=int, help="return when the job's rev differs")
    s.add_argument("--until-done", action="store_true")
    s.add_argument("--cursor", type=int, help="event cursor (without id)")
    s.add_argument("--agent")
    s.add_argument("--timeout", type=float, default=60)
    s = sub("events", lambda a: out(api("GET", "/api/events" + qs(cursor=a.cursor, agent=a.agent, job=a.job))),
            "events after a cursor (missed-event recovery)")
    s.add_argument("--cursor", type=int, default=0)
    s.add_argument("--agent")
    s.add_argument("--job")
    s = sub("logs", cmd_logs, "bounded log tail or bytes since a cursor")
    s.add_argument("id")
    s.add_argument("--tail", type=int, default=4000)
    s.add_argument("--since", type=int)
    s.add_argument("--attempt", type=int)
    s.add_argument("--raw", action="store_true", help="print text only")
    for name, extra, h in (("cancel", ("reason",), "cancel queued or running job"),
                           ("hold", ("reason",), "hold a queued job"),
                           ("release", ("accept_drift",), "release a held job"),
                           ("ack", (), "acknowledge a failure (unprotects its logs)"),
                           ("rm-logs", (), "delete a finished job's logs"),
                           ("remove", (), "remove a job from views (cancels it if pending)")):
        s = sub(name, control(name.replace("-", "_"), **dict.fromkeys(extra)), h)
        s.add_argument("id")
        if "reason" in extra:
            s.add_argument("--reason")
        if "accept_drift" in extra:
            s.add_argument("--accept-drift", action="store_true", default=None)
    s = sub("reorder", control("reorder", before=1, after=1, first=1, last=1), "reorder within your own jobs")
    s.add_argument("id")
    g = s.add_mutually_exclusive_group(required=True)
    g.add_argument("--before")
    g.add_argument("--after")
    g.add_argument("--first", action="store_true", default=None)
    g.add_argument("--last", action="store_true", default=None)
    s = sub("run-next", control("run_next", reason=1), "operator override: start this job next")
    s.add_argument("id")
    s.add_argument("--reason", required=True)
    s = sub("timeout", control("timeout", seconds=1), "set limit (running: absolute from start)")
    s.add_argument("id")
    s.add_argument("seconds", type=time_limit)
    s = sub("rerun", control("rerun", timeout_seconds=1), "rerun a finished job as a new linked job")
    s.add_argument("id")
    s.add_argument("--timeout", dest="timeout_seconds", type=time_limit)
    s = sub("clear", lambda a: out(api("POST", "/api/clear", {"actor": actor(a), "project": a.project,
                                                              "agent": a.agent})), "remove all finished jobs from views")
    s.add_argument("--project")
    s.add_argument("--agent")
    sub("pause", lambda a: out(api("POST", "/api/pause", {"actor": actor(a)})), "stop new starts")
    sub("resume", lambda a: out(api("POST", "/api/resume", {"actor": actor(a)})), "resume starts")
    s = sub("hook", lambda a: out(api("POST", "/api/hooks", {"agent": a.agent, "command": a.command, "actor": actor(a)})
                                  if a.command else api("POST", "/api/hooks/delete", {"id": a.rm})),
            "wake hook: command receives a JSON receipt on stdin")
    s.add_argument("--agent")
    s.add_argument("--command")
    s.add_argument("--rm", type=int)
    s = sub("view", lambda a: print(render(api("GET", "/api/overview" + qs(project=a.project, agent=a.agent)))),
            "human overview (text)")
    s.add_argument("--project")
    s.add_argument("--agent")
    s = sub("top", cmd_top, "live terminal view")
    s.add_argument("--project")
    s.add_argument("--agent")
    s = sub("app", cmd_app, "open the menu bar app (builds it on first use)")
    s.add_argument("--build", action="store_true", help="rebuild from anvil/Anvil.swift")
    sub("quickstart", lambda a: print(Path(__file__).with_name("QUICKSTART.md").read_text()), "agent quick-start")
    sub("serve", lambda a: __import__("anvil.server").server.serve(), "run the service in the foreground")
    sub("install", cmd_install, "build the menu bar app; add anvil-mcp to Claude Code and Codex")
    sub("uninstall", cmd_uninstall, "quit and remove the menu bar app and the MCP entries")

    args = sys.argv[1:] if argv is None else argv
    if not args or args[0] in ("-h", "--help", "help"):  # the guide is the help: an agent told "use anvil" starts here
        print(Path(__file__).with_name("QUICKSTART.md").read_text())
        print("Commands (anvil <command> -h for flags):", ", ".join(sp.choices))
        return
    a = p.parse_args(args)
    try:
        a.fn(a)
    except ApiError as e:
        out(e.obj)
        sys.exit(1)
    except PermissionError as e:  # e.g. creating $ANVIL_HOME or the service log from inside a sandbox
        out(SANDBOXED | {"message": f"{SANDBOXED['message']} ({e})"})
        sys.exit(1)
