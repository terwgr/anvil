"""HTTP API over a private Unix socket ($ANVIL_HOME/anvil.sock, mode 0600). No TCP listener."""
import contextlib
import json
import re
import sys
import os
import fcntl
import select
import socket
import traceback
import socketserver
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

from . import config
from .service import Err, Service

OPS = ("cancel", "hold", "release", "reorder", "move", "unpin", "run_next", "timeout", "ack", "rm_logs", "rerun", "remove")


class Handler(BaseHTTPRequestHandler):
    svc: Service = None
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def gone(self):
        """The client hung up (a killed `anvil wait`, a canceled MCP call): readable with nothing to read."""
        try:
            r, _, _ = select.select([self.connection], [], [], 0)
            return bool(r) and self.connection.recv(1, socket.MSG_PEEK) == b""
        except OSError:
            return True

    def send(self, obj, code=200, ctype="application/json"):
        body = obj if isinstance(obj, bytes) else json.dumps(obj, separators=(",", ":")).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def handle_one(self, method):
        u = urlparse(self.path)
        qs = {k: v[-1] for k, v in parse_qs(u.query).items()}
        path = u.path.rstrip("/") or "/"
        s = self.svc
        try:
            if method == "POST":
                n = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(n) or b"{}") if n else {}
                if not isinstance(body, dict):
                    raise Err("invalid", "request body must be a JSON object")
                actor = body.pop("actor", None) or "unknown"  # never an operator by omission
                if path == "/api/submit":
                    return self.send(s.submit(body, actor))
                if path in ("/api/pause", "/api/resume"):
                    return self.send(s.set_paused(path.endswith("pause"), actor))
                if path == "/api/clear":
                    return self.send(s.clear_finished(actor, body.get("project"), body.get("agent"), bool(body.get("queue"))))
                if path == "/api/hooks":
                    return self.send(s.add_hook(body.get("agent") or actor, body["command"]))
                if path == "/api/hooks/delete":
                    return self.send(s.del_hook(body["id"]))
                m = re.fullmatch(r"/api/jobs/(\w+)/(\w+)", path)
                if m and m[2] in OPS:
                    return self.send(s.control(m[1], m[2], actor, **body))
                raise Err("not_found", "unknown endpoint", 404)
            if path == "/api/health":
                return self.send({"ok": True, "pid": os.getpid(), "since": s.started_at})
            if path == "/api/overview":
                return self.send(s.overview(qs.get("project"), qs.get("agent"), qs.get("history", 8)))
            if path == "/api/jobs":
                return self.send(s.list(qs.get("project"), qs.get("agent"), qs.get("status"), qs.get("limit", 50),
                                        qs.get("after")))
            if path == "/api/wait":
                r = s.wait(qs.get("job"), qs.get("revision"), qs.get("cursor"), qs.get("agent"),
                           qs.get("project"), qs.get("timeout", 30), qs.get("until_done") == "1", gone=self.gone)
                if r is None:  # nobody to answer; nothing was marked delivered
                    self.close_connection = True
                    return
                return self.send(r)
            if path == "/api/events":
                with s.L:
                    ev = s.events(qs.get("cursor", 0), qs.get("job"), qs.get("agent"), qs.get("project"),
                                  min(int(qs.get("limit", 100)), 500))
                    return self.send({"events": ev, "cursor": ev[-1]["id"] if ev else int(qs.get("cursor", 0))})
            m = re.fullmatch(r"/api/jobs/(\w+)(/detail|/logs)?", path)
            if m:
                with s.L:
                    if m[2] == "/detail":
                        return self.send(s.detail(m[1]))
                    if m[2] == "/logs":
                        return self.send(s.logs(m[1], qs.get("tail", 4000), qs.get("since"), qs.get("attempt")))
                    return self.send(s.status(m[1]))
            raise Err("not_found", "unknown endpoint", 404)
        except Err as e:
            self.send({"error": e.code, "message": e.message}, e.http)
        except KeyError as e:
            self.send({"error": "invalid", "message": f"missing {e}"}, 400)
        except (ValueError, TypeError) as e:
            self.send({"error": "invalid", "message": str(e)[:200]}, 400)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
        except Exception as e:  # an answer, not a dropped connection that reads as "service unreachable"
            traceback.print_exc(file=sys.stderr)
            with contextlib.suppress(OSError):
                self.send({"error": "internal", "message": f"{type(e).__name__}: {e}"[:300]}, 500)

    def do_GET(self):
        self.handle_one("GET")

    def do_POST(self):
        self.handle_one("POST")


class Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True


def serve():
    path = str(config.sock())
    # One service per ANVIL_HOME: the lock is held for the life of the process and released by the OS on exit.
    lock = open(config.home() / "anvil.lock", "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.exit(f"anvil: already running at {path}")
    if os.path.exists(path):
        os.unlink(path)  # stale socket from a previous run
    svc = Service()
    Handler.svc = svc
    old = os.umask(0o177)
    httpd = Server(path, Handler)
    os.umask(old)
    svc.start()
    print(f"anvil serving {path}", flush=True)
    config.show_app()  # the menu bar app appears whenever the service runs, however it was started
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        svc.stopping = True  # running jobs keep running; the next start reconciles them
        os.unlink(path)
