"""Thin MCP (stdio) adapter over the same HTTP contract as the CLI."""
import json
import os
import shutil
import socket
import sys
import threading

from .cli import ENV_CAPTURE, ApiError, api, default_name, default_project, qs

# Who this MCP session is when a call doesn't say: $ANVIL_AGENT, else one name per client process, so every tool
# in a session (submit, wait, control) agrees and two sessions never share an identity by accident.
AGENT = os.environ.get("ANVIL_AGENT") or f"agent-{os.getppid()}"
# MCP clients cut tool calls off (Claude Code at 300s), so one wait stays under that; ANVIL_MCP_WAIT raises it.
WAIT = min(float(os.environ.get("ANVIL_MCP_WAIT", 240)), 600)
GUIDE = ("Anvil: the GPU queue on this computer. Run every GPU or heavy compute job through gpu_submit, "
         f"never directly. Don't poll: after submitting, keep working, then call gpu_wait(until_done) (up to "
         f"{WAIT:g}s per call) and repeat only while it returns unchanged. If it returns action_required, a job is "
         "held and won't start until you release or cancel it. Use one agent name for every tool (default: one per "
         "session). The CLI equivalent and full guide: `anvil help`.")
S, I, N, B, A = ({"type": "string"}, {"type": "integer"}, {"type": "number"}, {"type": "boolean"},
                 {"type": "array", "items": {"type": "string"}})
AGENT_DESC = {"type": "string", "description": f"who is acting (default {AGENT}); sub-agents: parent:lane"}


def tool(name, desc, props, req=()):
    return {"name": name, "description": desc,
            "inputSchema": {"type": "object", "properties": props, "required": list(req)}}


TOOLS = [
    tool("gpu_submit", "Queue a GPU job; returns at once with id, status, position, rev. cwd: absolute. Runs with this "
         "session's PATH/venv (cmd[0] is resolved on it) plus env. timeout_seconds: optional hard limit from launch "
         "(default none; 0 or \"none\" = no limit; the estimate never extends it). submission_key makes retries "
         "idempotent (per agent). start_within_seconds: cancel if not started by then. watch: paths (relative to "
         "cwd) the job reads; if their content changes before it starts, it is held (default: any tracked file in "
         "the repo, plus tracked files named in cmd). Python output is unbuffered. Then do other work; use gpu_wait.",
         {"name": S, "cmd": A, "cwd": S, "project": S, "agent": AGENT_DESC, "note": S, "estimated_seconds": N,
          "timeout_seconds": {"type": ["number", "string"]}, "submission_key": S,
          "env": {"type": "object", "additionalProperties": {"type": "string"}}, "start_within_seconds": N,
          "watch": A,
          "retry": {"type": "object", "description": "{max_attempts, backoff_seconds, exit_codes[], reasons[timed_out|signal]}"}},
         ("cmd", "cwd")),
    tool("gpu_status", "One job, compact (<1KB). status: queued|held|running|succeeded|failed|canceled|timed_out|"
         "interrupted. succeeded = exit 0 only. 'why' explains waiting; action_required says what a hold needs from "
         "you; approx_wait_s is uncertain.",
         {"id": S}, ("id",)),
    tool("gpu_detail", "One job in full: command, cwd, attempts, control history (who held, moved, canceled), "
         "results_dir and its files.", {"id": S}, ("id",)),
    tool("gpu_list", "List jobs, newest first; filter project/agent/status (comma list); page with 'after'=next.",
         {"project": S, "agent": S, "status": S, "limit": I, "after": S}),
    tool("gpu_wait", f"Bounded wait (<={WAIT:g}s). With id+rev: returns the job when its record changes "
         "(until_done: when final, or held in a way you must resolve). Without id, until_done: returns your "
         "finished jobs, and newly held ones, that no wait has returned yet, or none_pending (with 'held' if only "
         "held jobs remain). action_required: the job won't start until you release or cancel it (why says the "
         "reason, e.g. drift). A hold by the person is theirs: leave it. Returns {unchanged:true} on timeout "
         f"(default {WAIT:g}s with until_done): call again, don't poll status or logs in between.",
         {"id": S, "rev": I, "until_done": B, "cursor": I, "agent": AGENT_DESC, "timeout": N}),
    tool("gpu_logs", "Bounded log tail (bytes) or bytes since cursor. Returns data, cursor, truncated_before, more.",
         {"id": S, "tail": I, "since": I, "attempt": I}, ("id",)),
    tool("gpu_control", "Control one of your jobs. op: cancel [reason] (running: TERM, 5s, KILL, cleanup verified) | "
         "hold [reason] | release [accept_drift: run the changed source] | reorder before|after|first|last (among "
         "your own pending jobs) | unpin (undo run-next or move-to-back) | timeout seconds (0/\"none\" = no limit; "
         "running: absolute from start, > elapsed) | rerun [timeout_seconds] | ack (acknowledge a failure) | "
         "remove (drop from views; cancels if pending). Operators only: run_next reason | move before|after|last.",
         {"id": S, "op": S, "agent": AGENT_DESC, "reason": S, "before": S, "after": S, "first": B, "last": B,
          "seconds": {"type": ["number", "string"]}, "timeout_seconds": {"type": ["number", "string"]},
          "accept_drift": B}, ("id", "op")),
    tool("gpu_overview", "GPU idle/running/paused/attention, active job, queue order, recent results.",
         {"project": S, "agent": S}),
]


def call(name, a, on_conn=None):
    agent = a.get("agent") or AGENT  # the acting agent; for gpu_list/gpu_overview, `agent` is a filter instead
    actor = a.pop("actor", None) or agent
    if name in ("gpu_submit", "gpu_wait", "gpu_control"):
        a.pop("agent", None)
    if name == "gpu_submit":
        cwd = os.path.abspath(os.path.expanduser(a["cwd"]))
        env = {k: os.environ[k] for k in ENV_CAPTURE if k in os.environ}
        env.update(a.get("env") or {})
        cmd = list(a["cmd"])
        if cmd and "/" not in cmd[0]:  # resolve on the PATH the job will run with, as the CLI does
            cmd[0] = shutil.which(cmd[0], path=env.get("PATH")) or cmd[0]
        body = {**a, "cwd": cwd, "env": env, "cmd": cmd, "agent": agent, "actor": actor}
        body.setdefault("project", default_project(cwd))
        body.setdefault("name", default_name(cmd) if cmd else "job")
        return api("POST", "/api/submit", body)
    if name == "gpu_status":
        return api("GET", f"/api/jobs/{a['id']}")
    if name == "gpu_detail":
        return api("GET", f"/api/jobs/{a['id']}/detail")
    if name == "gpu_list":
        return api("GET", "/api/jobs" + qs(**a))
    if name == "gpu_wait":
        t = a.get("timeout")
        t = min(float(WAIT if t is None and a.get("until_done") else 60 if t is None else t), WAIT)
        who = agent if not a.get("id") else None
        return api("GET", "/api/wait" + qs(job=a.get("id"), revision=a.get("rev"), cursor=a.get("cursor"),
                                           agent=who, timeout=t, until_done=a.get("until_done")),
                   timeout=t + 10, on_conn=on_conn)
    if name == "gpu_logs":
        return api("GET", f"/api/jobs/{a.pop('id')}/logs" + qs(**a))
    if name == "gpu_control":
        jid, op = a.pop("id"), a.pop("op")
        return api("POST", f"/api/jobs/{jid}/{op}", {**a, "actor": actor})
    if name == "gpu_overview":
        return api("GET", "/api/overview" + qs(**a))
    raise ApiError({"error": "unknown_tool", "message": f"no tool {name!r}"})


out_lock = threading.Lock()
calls = {}  # request id -> open service connection, so a canceled call can be aborted


def send(obj):
    with out_lock:
        print(json.dumps(obj), flush=True)


def run_tool(mid, p):
    try:
        r, err = call(p["name"], dict(p.get("arguments") or {}), on_conn=lambda c: track(mid, c)), False
    except ApiError as e:
        r, err = e.obj, True
    except KeyError as e:
        r, err = {"error": "invalid", "message": f"missing argument {e}"}, True
    except Exception as e:  # one bad call must never take the server (and every tool) down
        r, err = {"error": "internal", "message": f"{type(e).__name__}: {e}"[:300]}, True
    if calls.pop(mid, None) is False:  # canceled meanwhile: the client no longer expects an answer
        return
    send({"jsonrpc": "2.0", "id": mid,
          "result": {"content": [{"type": "text", "text": json.dumps(r, separators=(",", ":"))}], "isError": err}})


def track(mid, c):
    if calls.get(mid) is False:  # canceled before the request went out
        raise ApiError({"error": "canceled", "message": "canceled by the client"})
    calls[mid] = c


def cancel(mid):
    c = calls.get(mid)
    calls[mid] = False
    if c and getattr(c, "sock", None):  # closing makes the service drop the wait without consuming results
        try:
            c.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


def handle(msg):
    mid, method = msg.get("id"), msg.get("method")
    if method == "tools/call":  # each call on its own thread: a long gpu_wait never blocks other tools
        threading.Thread(target=run_tool, args=(mid, msg.get("params") or {}), daemon=True).start()
        return
    if method == "notifications/cancelled":
        cancel((msg.get("params") or {}).get("requestId"))
        return
    if method == "initialize":
        res = {"protocolVersion": (msg.get("params") or {}).get("protocolVersion", "2025-06-18"),
               "capabilities": {"tools": {}}, "serverInfo": {"name": "anvil", "version": "1.0.0"},
               "instructions": GUIDE}
    elif method == "tools/list":
        res = {"tools": TOOLS}
    elif method == "ping":
        res = {}
    elif mid is None:
        return
    else:
        send({"jsonrpc": "2.0", "id": mid, "error": {"code": -32601, "message": "unknown method"}})
        return
    send({"jsonrpc": "2.0", "id": mid, "result": res})


def main():
    for line in sys.stdin:
        if not line.strip():
            continue
        try:
            msg = json.loads(line)
        except ValueError:
            send({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "parse error"}})
            continue
        for m in msg if isinstance(msg, list) else [msg]:
            try:
                if isinstance(m, dict):
                    handle(m)
            except Exception as e:
                if isinstance(m, dict) and m.get("id") is not None:
                    send({"jsonrpc": "2.0", "id": m["id"], "error": {"code": -32603, "message": str(e)[:200]}})


if __name__ == "__main__":
    main()
