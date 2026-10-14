"""Process-group leader for one job attempt. Records the child's exit status to a file so the
outcome survives a service restart. Standalone: imports nothing from the package.
Usage: python -I wrap.py <exit_file> <cmd> [args...]"""
import json
import os
import signal
import subprocess
import sys
import time

exit_file, cmd = sys.argv[1], sys.argv[2:]
# TERM goes to the whole group; the wrapper outlives the child long enough to record its status.
signal.signal(signal.SIGTERM, signal.SIG_IGN)
signal.signal(signal.SIGINT, signal.SIG_IGN)
try:
    child = subprocess.Popen(cmd, restore_signals=True,
                             preexec_fn=lambda: (signal.signal(signal.SIGTERM, signal.SIG_DFL),
                                                 signal.signal(signal.SIGINT, signal.SIG_DFL)))
    rc = child.wait()
except OSError as e:
    print(f"anvil: cannot launch {cmd[0]!r}: {e}", file=sys.stderr, flush=True)
    rc = 127
rec = {"exit_code": rc if rc >= 0 else None, "signal": -rc if rc < 0 else None, "at": time.time()}
tmp = exit_file + ".tmp"
with open(tmp, "w") as f:
    json.dump(rec, f)
os.replace(tmp, exit_file)
sys.exit(rc if rc >= 0 else 128 - rc)
