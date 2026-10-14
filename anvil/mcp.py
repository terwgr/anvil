"""Thin MCP (stdio) adapter over the same HTTP contract as the CLI."""
import json
import os
import sys

from .cli import ApiError, api, qs

AGENT = os.environ.get("ANVIL_AGENT")
GUIDE = ("Anvil: the GPU queue on this computer. Run every GPU or heavy compute job through gpu_submit, "
         "never directly. Don't poll: after submitting, keep working, then call gpu_wait(until_done, timeout 600) "
         "and repeat only while it returns unchanged. The CLI equivalent and full guide: `anvil help`.")
S, I, N, B, A = ({"type": "string"}, {"type": "integer"}, {"type": "number"}, {"type": "boolean"},
                 {"type": "array", "items": {"type": "string"}})


def tool(name, desc, props, req=()):
    return {"name": name, "description": desc,
            "inputSchema": {"type": "object", "properties": props, "required": list(req)}}


TOOLS = [
    tool("gpu_submit", "Queue a GPU job; returns at once with id, status, position, rev. Runs in cwd with captured "
         "PATH/venv. timeout_seconds: optional hard limit from launch (default none; 0 or \"none\" = no limit; "
         "estimate never extends it). submission_key makes "
         "retries idempotent. Then do other work; use gpu_wait.",
         {"name": S, "cmd": A, "cwd": S, "project": S, "agent": S, "note": S, "estimated_seconds": N,
          "timeout_seconds": {"type": ["number", "string"]}, "submission_key": S, "env": {"type": "object"}, "start_before": N,
          "retry": {"type": "object", "description": "{max_attempts, backoff_seconds, exit_codes[], reasons[timed_out|signal]}"}},
         ("name", "cmd", "cwd")),
    tool("gpu_status", "One job, compact (<1KB). status: queued|held|running|succeeded|failed|canceled|timed_out|"
         "interrupted. succeeded = exit 0 only. 'why' explains waiting; approx_wait_s is uncertain.",
         {"id": S}, ("id",)),
    tool("gpu_list", "List jobs, newest first; filter project/agent/status (comma list); page with 'after'=next.",
         {"project": S, "agent": S, "status": S, "limit": I, "after": S}),
    tool("gpu_wait", "Bounded wait (<=600s). With id+rev: returns the job when its record changes "
         "(until_done: only when final). Without id, until_done: returns your (agent) finished jobs that no wait has "
         "returned yet, or none_pending. With cursor: returns new events. Returns {unchanged:true} on timeout "
         "(default 600s with until_done): call again, don't poll status or logs in between.",
         {"id": S, "rev": I, "until_done": B, "cursor": I, "agent": S, "timeout": N}),
    tool("gpu_logs", "Bounded log tail (bytes) or bytes since cursor. Returns data, cursor, truncated_before, more.",
         {"id": S, "tail": I, "since": I, "attempt": I}, ("id",)),
    tool("gpu_control", "Control a job. op: cancel (running: TERM, 5s, KILL, cleanup verified) | hold | release "
         "(accept_drift to run changed source) | reorder (before/after/first/last among your own jobs) | run_next "
         "(operator only, needs reason) | timeout (seconds, or 0/\"none\" for no limit; running = absolute from start, > elapsed) | rerun | ack.",
         {"id": S, "op": S, "reason": S, "before": S, "after": S, "first": B, "last": B, "seconds": {"type": ["number", "string"]},
          "accept_drift": B}, ("id", "op")),
    tool("gpu_overview", "GPU idle/running/paused/attention, active job, queue order, recent results.",
         {"project": S, "agent": S}),
]


def call(name, a):
    actor = a.pop("actor", None) or AGENT or "agent"
    if name == "gpu_submit":
        a.setdefault("agent", AGENT or "agent")
        a.setdefault("project", os.path.basename(a["cwd"].rstrip("/")))
        return api("POST", "/api/submit", {**a, "actor": actor})
    if name == "gpu_status":
        return api("GET", f"/api/jobs/{a['id']}")
    if name == "gpu_list":
        return api("GET", "/api/jobs" + qs(**a))
    if name == "gpu_wait":
        t = min(float(a.get("timeout", 600 if a.get("until_done") else 60)), 600)
        agent = a.get("agent") or (AGENT if not a.get("id") else None)
        return api("GET", "/api/wait" + qs(job=a.get("id"), revision=a.get("rev"), cursor=a.get("cursor"),
                                           agent=agent, timeout=t, until_done=a.get("until_done")),
                   timeout=t + 10)
    if name == "gpu_logs":
        return api("GET", f"/api/jobs/{a.pop('id')}/logs" + qs(**a))
    if name == "gpu_control":
        jid, op = a.pop("id"), a.pop("op")
        return api("POST", f"/api/jobs/{jid}/{op}", {**a, "actor": actor})
    if name == "gpu_overview":
        return api("GET", "/api/overview" + qs(**a))
    raise ApiError({"error": "unknown_tool"})


def main():
    for line in sys.stdin:
        if not line.strip():
            continue
        msg = json.loads(line)
        mid, method = msg.get("id"), msg.get("method")
        if method == "initialize":
            res = {"protocolVersion": msg["params"].get("protocolVersion", "2025-06-18"),
                   "capabilities": {"tools": {}}, "serverInfo": {"name": "anvil", "version": "1.0.0"},
                   "instructions": GUIDE}
        elif method == "tools/list":
            res = {"tools": TOOLS}
        elif method == "tools/call":
            p = msg["params"]
            try:
                r, err = call(p["name"], dict(p.get("arguments") or {})), False
            except ApiError as e:
                r, err = e.obj, True
            res = {"content": [{"type": "text", "text": json.dumps(r, separators=(",", ":"))}], "isError": err}
        elif method == "ping":
            res = {}
        elif mid is None:
            continue
        else:
            print(json.dumps({"jsonrpc": "2.0", "id": mid, "error": {"code": -32601, "message": "unknown method"}}),
                  flush=True)
            continue
        print(json.dumps({"jsonrpc": "2.0", "id": mid, "result": res}), flush=True)


if __name__ == "__main__":
    main()
