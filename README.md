# Anvil

A local, fair GPU queue for one Mac, shared by all projects and agents. One job runs at a time;
agents submit, keep working, and wake on completion. Python 3.11+ standard library only — no packages.
Requires macOS on Apple Silicon and the Xcode command-line tools (`xcode-select --install`) to build the menu bar app.

<p align="center"><img src="docs/anvil-logo.png" width="120" alt="Anvil logo"></p>

## Install

```bash
uv tool install git+https://github.com/terwgr/anvil
```
```bash
anvil install
```

The first command puts `anvil` and `anvil-mcp` on your PATH. The second builds the menu bar app into
`~/Applications/Anvil.app` (Swift/AppKit, compiled locally with Xcode's `swiftc`) and adds the
`anvil-mcp` server to Claude Code and Codex if they are installed. Nothing starts at login: the first
`anvil` command or MCP call starts the service, and the menu bar app appears with it.
`anvil uninstall` quits and removes the app and the MCP entries.

To add the MCP server yourself (or to another client, as a stdio server running `anvil-mcp`):

```bash
claude mcp add --scope user anvil -- anvil-mcp
```
```bash
codex mcp add anvil -- anvil-mcp
```

## Use

```bash
anvil run -n train -e 120 -t 900 -- python3 train.py
```
queues the job, waits until it is done and prints its receipt (outcome, log tail, result files);
`anvil submit` takes the same flags and returns at once.
- Menu bar: a small anvil — outline when idle, filled while running, with pause bars when new starts are
  paused and "!" when something needs attention. Click it for the panel: one line per job — running job,
  queue, recent results.
  - Drag queued rows anywhere: within one agent's jobs it reorders them and fair sharing continues; elsewhere
    the jobs above the drop point are pinned in that order (recorded, visible as "next"), so the job lands
    where you drop it. The running job's bar fills against the agent's estimate and turns orange past it.
  - Hover a row to see its id and agent; the tooltip has status, reason, limits, note. Right-click ▸ Unpin
    undoes a "next" or "last" placement.
  - "Needs Attention" lists its causes (failures not yet dismissed, drift holds, a stuck cleanup) under the
    header, with Dismiss for failures. Stopping a running job and the Clear actions ask first.
  - Hover a row for × (remove: cancels a queued job; stop for the running one); right-click for every action;
    click to expand the note, log tail, time limit and buttons. ⋯ ▸ Clear Finished empties the recent list;
    ⋯ ▸ Clear Queue… cancels every queued and held job (in the current filter), after a confirmation.
  - Drag the panel's edges to resize; it remembers the size and otherwise fits its content.
  - The pin keeps it open while you work elsewhere; otherwise Esc, clicking elsewhere or the icon closes it.
  `anvil app` opens it.
- `anvil top` — live terminal view; `anvil view` — one-shot.
- Agents: tell any agent "use Anvil for the GPU". Running `anvil` (or `anvil help`) prints its guide ([anvil/QUICKSTART.md](anvil/QUICKSTART.md)).
- MCP: agents get `gpu_run` (submit and wait in one call), `gpu_submit`, `gpu_wait`, `gpu_logs` and the rest;
  they pass `agent` to name themselves.
- Agents get one answer per finished job, with what they need to act on it (outcome, log tail, result files),
  instead of checking in while it runs; a result whose answer was lost (a killed client) comes back on the
  next wait.

Route each project's GPU entrypoint through `anvil submit`; work started outside Anvil is detected
best-effort and shown, never killed.

## Configuration

The service listens only on a private Unix socket, `~/.anvil/anvil.sock` (owner-only); there is no
network port. `~/.anvil/config.json` (all optional): `default_timeout_seconds` (hard limit for jobs
submitted without one; default none, so jobs run until they finish), `max_timeout_seconds` (if set, every
job needs a limit up to it), `operators` (actors allowed to pause, run-next and control others' jobs;
default `["human","panel"]`, `"*"` for everyone), `fairness` (`"time"`, default: equal GPU time; or
`"turns"`: strict round robin), `held_expire_seconds` (cancel jobs held longer; default 86400, `null` keeps
them), `timeout_sample_seconds` (stack sample before a time-limit kill; default 2, 0 off),
`log_retention` (`max_total_mb`, `max_age_days`), `outside_gpu_patterns` (add your own GPU binaries), `hooks`,
`hook_timeout_seconds` (default 60).
Data, logs and results live in `~/.anvil` (`ANVIL_HOME` overrides).

## Layout

`anvil/service.py` store, scheduler, runner, events · `anvil/sched.py` fairness policy ·
`anvil/server.py` API on the Unix socket · `anvil/Anvil.swift` menu bar app · `anvil/cli.py` CLI + terminal view ·
`anvil/mcp.py` MCP adapter · `anvil/wrap.py` job process-group leader (records exit status across restarts).

## Checks

```bash
python3 -m unittest tests.test_sched tests.test_wait
```
```bash
~/Applications/Anvil.app/Contents/MacOS/Anvil --selftest
```

The first checks the fairness policy, and waits, receipts and delivery against a throwaway service (its own
`ANVIL_HOME`; tiny shell jobs, no GPU). The second drives the real panel against the running service with real
mouse events through the event queue (pin button clicks, row drags) and every control (hold, release, reorder,
run next, limit, pause, stop, rerun, remove, clear, resize), using throwaway jobs in project `anvil-selftest`. It pauses scheduling and runs real jobs, so run it
while no one else needs the GPU.
`--snapshot <png> [--dark] [--expand]` renders the panel and `--icons <png>` the four icon states.

## License

MIT — see [LICENSE](LICENSE).
