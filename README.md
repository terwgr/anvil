# Anvil

A local, fair GPU queue for one Mac, shared by all projects and agents. One job runs at a time;
agents submit, keep working, and wake on completion. Python 3.11+ standard library only — no packages.
Requires macOS on Apple Silicon and the Xcode command-line tools (`xcode-select --install`) to build the menu bar app.

<p align="center"><img src="docs/anvil-logo.png" width="220" alt="Anvil logo"></p>

## Install

```bash
uv tool install git+https://github.com/terwgr/anvil
```
```bash
anvil install
```

The first command puts `anvil` and `anvil-mcp` on your PATH. The second builds the menu bar app into
`~/Applications/Anvil.app` (Swift/AppKit, compiled locally with Xcode's `swiftc`) and starts both the
service and the app at login (`anvil uninstall` removes the login items). Without it, any `anvil` command
or the app starts the service on demand.

## Use

```bash
anvil submit -n train -e 120 -t 900 -- python3 train.py
```
- Menu bar: a small anvil — outline when idle, filled while running, with pause bars when new starts are
  paused and "!" when something needs attention. Click it for the panel: one line per job — running job,
  queue, recent results.
  - Drag queued rows to reorder an agent's own jobs; drag a job to the top to run it next (recorded, visible).
    Projects and agents otherwise share the GPU fairly, so other cross-agent moves are explained, not applied.
  - Hover a row for × (remove: cancels a queued job; stop for the running one); right-click for every action;
    click to expand the note, log tail, time limit and buttons. ⋯ ▸ Clear Finished empties the recent list.
  - Drag the panel's edges to resize; it remembers the size and otherwise fits its content.
  - The pin keeps it open while you work elsewhere; otherwise Esc, clicking elsewhere or the icon closes it.
  `anvil app` opens it.
- `anvil top` — live terminal view; `anvil view` — one-shot.
- Agents: tell any agent "use Anvil for the GPU". Running `anvil` (or `anvil help`) prints its guide ([anvil/QUICKSTART.md](anvil/QUICKSTART.md)).
- MCP: add `anvil-mcp` as a stdio server with `ANVIL_AGENT=<id>` in its environment.

Route each project's GPU entrypoint through `anvil submit`; work started outside Anvil is detected
best-effort and shown, never killed.

## Configuration

The service listens only on a private Unix socket, `~/.anvil/anvil.sock` (owner-only); there is no
network port. `~/.anvil/config.json` (all optional): `default_timeout_seconds` (300), `max_timeout_seconds`,
`operators` (actors allowed to pause, run-next and control others' jobs; default `["human","panel"]`,
`"*"` for everyone), `log_retention` (`max_total_mb`, `max_age_days`), `outside_gpu_patterns`, `hooks`.
Data, logs and results live in `~/.anvil` (`ANVIL_HOME` overrides).

## Layout

`anvil/service.py` store, scheduler, runner, events · `anvil/sched.py` fairness policy ·
`anvil/server.py` API on the Unix socket · `anvil/Anvil.swift` menu bar app · `anvil/cli.py` CLI + terminal view ·
`anvil/mcp.py` MCP adapter · `anvil/wrap.py` job process-group leader (records exit status across restarts).
## Checks

```bash
python3 -m unittest tests.test_sched
```
```bash
~/Applications/Anvil.app/Contents/MacOS/Anvil --selftest
```

The first checks the fairness policy. The second drives the real panel against the running service with real
mouse events through the event queue (pin button clicks, row drags) and every control (hold, release, reorder,
run next, limit, pause, stop, rerun, remove, clear, resize), using throwaway jobs in project `anvil-selftest`.
`--snapshot <png> [--dark] [--expand]` renders the panel and `--icons <png>` the four icon states.

## License

MIT — see [LICENSE](LICENSE).
