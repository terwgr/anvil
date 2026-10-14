# Anvil — GPU queue for agents

This computer has one GPU, shared through Anvil by every project and agent. When told to "use Anvil",
run every GPU or heavy compute job (training, inference, benchmarks, evals) through `anvil submit`, never
directly. One job runs at a time, fairly shared.

Don't poll. Submit, then make one wait that lasts until the job is done: no loops of short waits, no
repeated status or log checks while it runs. Each check that finds nothing new costs you a turn.

## Do this
    export ANVIL_AGENT=<any stable id you pick, e.g. project-task>
    anvil submit -n train -e 120 -t 900 -- python3 train.py --lr 3e-4   # from your project directory
    -> {"id":"j12","status":"queued","position":2,"why":"GPU busy (j11)",...}
    anvil wait j12 --until-done      # one call; returns when it finishes. Background it if your tools allow,
                                     # and keep working meanwhile
    anvil logs j12 --tail 4000       # then check output and your result files
Several jobs: `anvil wait --until-done` (no id) returns your finished jobs that no wait has returned yet;
repeat it until it says `"none_pending":true`. Nothing is missed between calls.

`-e` your runtime guess (seconds, advisory; a rough guess is fine) · `-t` optional hard time limit from launch (default none) ·
`-n` short name · `--note` why. All output is compact JSON; errors are `{"error":..,"message":..}`, exit 1.
The person sees the same queue in the Anvil menu bar app and may hold, reorder or stop your jobs.
If your tools run commands in a sandbox, run `anvil` with permission to leave it: the service lives in
`~/.anvil` and the sandbox blocks it (`{"error":"permission_denied"}`). `anvil status` shows the whole queue.

## Details
- Project defaults to the git repo name, else the folder name; cwd, PATH and venv are captured.
- `-t` is a hard limit from launch (default none; `-t 0` removes one); queue wait doesn't count. Set it when
  a hang must not hold the GPU. `-e` is advisory only.
- `--key K` makes the submit idempotent: repeating it returns the original job.
- Retries are opt-in: `--max-attempts 3 --retry-codes 75 --retry-on timed_out --backoff 30`.
- `--start-before +3600` cancels the job (reason `queue_deadline`) if it hasn't started in time.

### Waiting (don't poll)
    anvil wait j12 --until-done --timeout 600   # bounded: final status, or {"unchanged":true,"approx_left_s":..}
    anvil wait j12 --rev 3                       # returns when the record changes
    anvil wait --until-done                      # your next finished jobs ($ANVIL_AGENT, or --agent)
    anvil events --cursor 41 --agent me          # missed events after reconnecting
If your harness runs background commands and re-invokes you when they exit, run `anvil wait` that way.
Or register a wake hook: `anvil hook --agent me --command '<cmd>'`
(receipt JSON on stdin, retried, deduplicated by event id).

### States
queued (waiting; `why` and `position` say where) · held (won't start; `why` gives the reason, e.g.
`drift: git HEAD a1→b2` — release with `--accept-drift` to run the current source) · running (`sub`:
stopping/cleanup) · succeeded (exit 0 only — check your own results) · failed · canceled · timed_out ·
interrupted (service restarted, outcome unknown; never auto-relaunched).

### Output
    anvil logs j12 --tail 4000      # bounded; --since <cursor> to continue; --raw for text
    anvil show j12                  # command, attempts, control history, results files
Write result files to `$ANVIL_RESULT_DIR`. Optional progress: write
`{"message":"epoch 3/10","fraction":0.3}` to `$ANVIL_PROGRESS_FILE`.

### Control
    anvil cancel j12 | hold j12 | release j12 | timeout j12 1800 | rerun j12 | ack j12
    anvil remove j12                         # drop from views (cancels if pending); `anvil clear` for all finished
    anvil reorder j12 --before j10          # your own pending jobs only
    anvil run-next j12 --reason "..."        # operators only; visible to everyone
    anvil list --agent me --status queued,running
Fairness: projects get equal GPU time, then agents within a project. Many submissions earn no extra
share. Errors: `{"error":"code","message":"..."}` with exit status 1.

MCP: the `anvil-mcp` server offers the same as gpu_submit, gpu_wait, gpu_logs, gpu_status, gpu_list, gpu_control, gpu_overview.
