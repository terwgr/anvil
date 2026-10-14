"""Paths and configuration. Config lives in $ANVIL_HOME/config.json (default ~/.anvil)."""
import hashlib
import json
import os
import subprocess
from pathlib import Path

DEFAULTS = {
    "default_timeout_seconds": 300,
    "max_timeout_seconds": None,          # optional service maximum; oversized requests are rejected
    "overdue_after_seconds": 600,
    "term_grace_seconds": 5,
    # Actors allowed to control other agents' jobs, pause/resume, and use run-next.
    # Agents always control their own jobs. "*" allows everyone.
    "operators": ["human", "panel"],
    "log_retention": {"max_total_mb": 2048, "max_age_days": 14},
    # Best-effort detection of GPU work started outside Anvil (matched against process args).
    "outside_gpu_patterns": ["mlx_lm", "mlx_whisper", "torchrun", "ollama", "llama-server", "llama-cli",
                             "whisper", "ComfyUI", "comfyui", "stable-diffusion", "LM Studio"],
    "probe_interval_seconds": 15,
    # Global hooks: [{"command": "...", "agents": ["*"]}]; receipt JSON is passed on stdin.
    "hooks": [],
}


APP = Path.home() / "Applications/Anvil.app"


def home() -> Path:
    p = Path(os.environ.get("ANVIL_HOME", Path.home() / ".anvil"))
    if not p.is_dir():  # mkdir on an existing dir fails with EPERM (not EEXIST) inside agent sandboxes
        p.mkdir(parents=True, exist_ok=True)
    return p


def load() -> dict:
    cfg = json.loads(json.dumps(DEFAULTS))
    f = home() / "config.json"
    if f.exists():
        user = json.loads(f.read_text())
        for k, v in user.items():
            if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                cfg[k].update(v)
            else:
                cfg[k] = v
    return cfg


def sock() -> Path:
    """Socket path. macOS limits AF_UNIX paths to 104 bytes; deep homes fall back to a short /tmp name
    (same rule in anvil/Anvil.swift)."""
    p = home() / "anvil.sock"
    if len(str(p)) <= 100:
        return p
    return Path(f"/tmp/anvil-{os.getuid()}-{hashlib.sha1(str(home()).encode()).hexdigest()[:10]}.sock")


def show_app():
    """Open the menu bar app in the background if it is built for this ANVIL_HOME. No-op when already open."""
    try:
        built_for = json.loads((APP / "Contents/Resources/service.json").read_text())["home"]
    except (OSError, ValueError, KeyError):
        return
    if Path(built_for) == home():
        subprocess.Popen(["open", "-g", str(APP)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
