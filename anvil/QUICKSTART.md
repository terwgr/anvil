# Anvil — GPU queue for agents

This computer has one GPU, shared through Anvil by every project and agent. When told to "use Anvil",
run every GPU or heavy compute job (training, inference, benchmarks, evals) through `anvil run` (or
`anvil submit`), never directly. One job runs at a time, fairly shared.

Don't poll. Every check that finds nothing new costs you a turn and fills your context. Hand Anvil the job and
get one answer back when it's done: the outcome, the end of the log and the result files, so there is
usually nothing left to look up.

## Do this
Pick a stable name (e.g. project-task) and put it on every command: most agent tools start a fresh shell per
command, so an `export` from an earlier command is gone.

    ANVIL_AGENT=me anvil run -n train -e 120 -t 900 --watch train.py --watch src -- python3 train.py --lr 3e-4
    -> {"id":"j12","status":"succeeded","exit_code":0,"log_tail":"...val_loss 0.41","results":["metrics.json"],...}
`anvil run` queues the job and waits until it is done (or needs you: `action_required`), then prints the
receipt. Run it in the background if your tools resume you when a command exits, and keep working: you
are woken once, with the answer. If a command must finish within your tool's time limit (often 2 minutes),
use `anvil submit` (same flags; prints the id at once) and `anvil wait j12 --until-done --timeout 100`,
repeated while it says `"unchanged":true`.
Several jobs: `anvil submit` each, then one `anvil wait --until-done` (no id): it returns every finished job
no wait has returned yet; repeat until `"none_pending":true`. Don't wait per job. Nothing is lost between
calls: if a wait is killed before it prints, the next one returns the same results.

`-e` your runtime guess (seconds, advisory; a rough guess is fine) · `-t` optional hard time limit from
launch (default none) · `-n` short name · `--note` why. All output is compact JSON; errors are
`{"error":..,"message":..}`, exit 1. `anvil status` shows the whole queue.
The person sees the same queue in the Anvil menu bar app and may hold, reorder or stop your jobs.
If your tools run commands in a sandbox, run `anvil` with permission to leave it: the service lives in
`~/.anvil` and the sandbox blocks it (`{"error":"permission_denied"}`).

## Details
- Project defaults to the git repo name, else the folder name; cwd, PATH and venv are captured.
- Without ANVIL_AGENT (or `-a`/`--actor`) you are `$USER`: shared with every other agent that forgot.
- `-t` is a hard limit from launch (default none; `-t 0` removes one); queue wait doesn't count. Set it when
  a hang must not hold the GPU. `-e` is advisory only.
- `--key K` makes the submit idempotent (per agent): repeating it returns the original job.
- Retries are opt-in: `--max-attempts 3 --retry-codes 75 --retry-on timed_out --backoff 30`.
- `--start-before +3600` (or an ISO time) cancels the job (reason `queue_deadline`) if it hasn't started by then.
- Drift: if the source a queued job will run changes before it starts, the job is held instead of run.
  Content counts, not commits: committing, amending or touching files with the same bytes is not drift.
  By default it's any tracked file in the repo plus tracked files named in the command, so another agent's
  edit anywhere in the repo holds your job: `--watch src/kernels --watch run.py` narrows it to the paths the
  job uses (untracked files under them included; gitignored ones never count). Data and output files named
  in the command (untracked) are not source. `--no-drift-check` turns it off.
- Held jobs are canceled after 24h (reason `hold_expired`) unless released. If the person held your job,
  leave it: the status says `waiting_on`, and it starts when they release it.
- Python runs unbuffered (`PYTHONUNBUFFERED=1`), so logs are live. On a time limit Anvil samples the
  processes' stacks first; the log's last line names the files.
- Sub-agents: name them `parent:lane` (e.g. `ANVIL_AGENT=sweep:lane2`); they share the parent's turn, and
  the parent may control their jobs.

### Waiting (don't poll)
    anvil run [submit flags] -- CMD             # submit and wait: one receipt (--wait-timeout to bound it)
    anvil wait j12 --until-done --timeout 100   # bounded: receipt, or {"unchanged":true,"approx_left_s":..}
    anvil wait j12 --rev 3                       # returns when the record changes
    anvil wait --until-done                      # your next finished jobs ($ANVIL_AGENT, or --agent)
    anvil events --cursor 41 --agent me          # missed events after reconnecting
If your harness runs background commands and re-invokes you when they exit, run `anvil run` or `anvil wait`
that way. Or register a wake hook: `anvil hook --agent me --command '<cmd>'` (a short receipt on stdin: status,
exit code and reason, without the log tail or result files; retried, deduplicated by event id).

### States
queued (waiting; `why` and `position` say where) · held (won't start; `why` gives the reason, e.g.
`drift: source changed: src/a.py` — release with `--accept-drift` to run the current source; `wait --until-done`
returns a newly held job at once with `action_required`; a hold by the person says `waiting_on` instead:
leave it) · running (`sub`:
stopping/cleanup) · succeeded (exit 0 only — check your own results) · failed · canceled · timed_out ·
interrupted (service restarted, outcome unknown; never auto-relaunched).

### Output
A finished job comes back as a receipt: status and exit code, `log_tail` (the last lines; more of them on
failure) and `results` (files in `results_dir`). Look further only when that isn't enough:
    anvil logs j12 --tail 4000      # bounded; --since <cursor> to continue; --raw for text
    anvil show j12                  # command, attempts, control history, results files
Write result files to `$ANVIL_RESULT_DIR`. Optional progress: write
`{"message":"epoch 3/10","fraction":0.3}` to `$ANVIL_PROGRESS_FILE`.

### Control
    anvil cancel j12 | hold j12 | release j12 | timeout j12 1800 | rerun j12 | ack j12   # timeout j12 none: no limit
    anvil remove j12                         # drop from views (cancels if pending); `anvil clear` for all finished
    anvil reorder j12 --before j10          # your own pending jobs only
    anvil unpin j12                          # undo run-next / move-to-back: back to its fair place
    anvil run-next j12 --reason "..."        # operators only; visible to everyone
    anvil list --agent me --status queued,running
Fairness: projects get equal GPU time, then agents within a project (or strict turns, if the person set
`"fairness": "turns"`). Many submissions earn no extra share. Only the job's agent (or an operator)
controls it: use the same `--actor`/`agent` you submitted with.

MCP: the `anvil-mcp` server offers the same commands as tools: gpu_run, gpu_submit, gpu_wait, gpu_logs,
gpu_status, gpu_detail, gpu_list, gpu_control, gpu_overview. gpu_run is `anvil run` within one tool call
(jobs:[...] for a batch, one entry per job); gpu_run and gpu_wait wait up to 240s per call (under MCP
clients' 300s tool limit; set ANVIL_MCP_WAIT to change it). For longer jobs, gpu_submit and then run
`anvil wait --until-done` in the background with the session's agent name, if your tools can; otherwise call
gpu_wait again while it returns unchanged. Without `agent`, a session is one agent (`agent-<pid>`); if you
pass `agent`, pass the same one to every tool. Jobs run with the MCP server's PATH and venv unless you pass
`env`.
