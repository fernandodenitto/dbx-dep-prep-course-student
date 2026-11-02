---
title: "Reference card: Jobs from the API and the CLI"
section: S12 — Debugging and Deploying
duration: ~6 min read
type: 📄 reference card (Udemy article — no video)
verified: Databricks CLI v1.19.0 (`--help` output and live runs) + docs.databricks.com, 2026-11-01
---

> **📄 Reference card.** No video, nothing to run. Every command below was checked against
> **CLI v1.19.0**. Run `databricks jobs --help`: your binary always beats this page.

Lesson 12.3 repaired a failed run. This card is the whole jobs surface from outside the
UI, which is where production automation actually lives.

---

## The commands

```bash
# defining
databricks jobs create  --json @job.json      # new job
databricks jobs reset   --json @job.json      # replace ALL settings; job_id goes in the JSON
databricks jobs update  --json @patch.json    # patch some settings; job_id goes in the JSON
databricks jobs delete  123

# reading
databricks jobs list
databricks jobs list --name lakeshore-debug-lab
databricks jobs get       123
databricks jobs list-runs --job-id 123
databricks jobs get-run   456
databricks jobs get-run-output 789             # a TASK run id: the task's output or error
databricks jobs export-run     789             # exportable notebook run

# running
databricks jobs run-now    123
databricks jobs submit     --json @run.json    # one-time run, creates NO job
databricks jobs repair-run 456 --rerun-all-failed-tasks
databricks jobs cancel-run 456
databricks jobs cancel-all-runs --job-id 123
```

**The rule that catches everyone:** the id is positional, and `--json` replaces it. Pass both and the
CLI says `when --json flag is specified, no positional arguments are allowed`. So parameter overrides
carry the id inside the JSON:

```bash
databricks jobs run-now --json '{"job_id": 123, "job_parameters": {"top_n": "-1"}}'
databricks jobs repair-run --json '{"run_id": 456, "rerun_all_failed_tasks": true,
                                    "job_parameters": {"top_n": "5"}}'
```

`list-runs --job-id` and `cancel-all-runs --job-id` are real flags, because those two take no positional
id.

---

## Pipelines, the same way

```bash
databricks pipelines list-pipelines --filter "name LIKE '%lakeshore%'"
databricks pipelines start-update <pipeline-id>
databricks pipelines list-updates <pipeline-id> --max-results 5
databricks pipelines get-update <pipeline-id> <update-id>
databricks pipelines list-pipeline-events <pipeline-id> --max-results 20
databricks pipelines list-pipeline-events <pipeline-id> --filter "level='ERROR'"
```

---

## Triggers, in the job's JSON

```json
{"schedule": {"quartz_cron_expression": "0 0 6 * * ?", "timezone_id": "UTC"}}
{"trigger": {"pause_status": "UNPAUSED", "file_arrival": {"url": "/Volumes/lakeshore/landing/files/topics/"}}}
{"trigger": {"pause_status": "UNPAUSED", "table_update": {"table_names": ["lakeshore.silver.bus_orders"]}}}
{"continuous": {"pause_status": "UNPAUSED"}}
```

A bundle's development mode pauses all of them, which is the usual cause of "my job never ran".

---

## The three distinctions the exam tests

### `reset` vs `update`

| | Replaces | Use for |
|---|---|---|
| `reset` | **All settings.** Anything absent from the payload is **cleared** | Declarative "this is the job now" |
| `update` | Only the fields you send | Surgical changes |

**Trap:** `reset` with a partial payload silently drops schedules, notifications and tasks.
It is the most destructive command in this list, and its name does not suggest it.

### `run-now` vs `submit`

| | Needs an existing job | Leaves a job behind |
|---|---|---|
| `run-now` | Yes | — |
| `submit` | No | **No** |

`submit` creates a **one-time run**. It is how every notebook in this course was validated,
and why the workspace has no leftover validation jobs. When a question says "run this
workload once without registering anything", the answer is `submit`.

### `repair-run` vs `run-now`

`repair-run` re-runs **only the unsuccessful tasks and their dependents**, reusing the successful
ones, with the **current** job and task settings. `run-now` starts a fresh run of everything.

The docs support repair only for jobs with **two or more tasks**. A failed single-task job gets Run now.

**And the caveat from lesson 12.3:** *"Lakeflow Jobs doesn't make tasks idempotent, so if a
task wrote part of its output before it failed, re-running it can duplicate that data."*
Repair is cheap, not free: it is only safe on idempotent tasks.

From the second repair of a run on, the request must carry `latest_repair_id`, from the previous repair's
response or from `jobs get-run <id> --include-history`. And `rerun_tasks` means exactly those tasks: add
`rerun_dependent_tasks`, or use `rerun_all_failed_tasks`, or a skipped dependent stays skipped.

---

## Scripting it

```bash
databricks jobs list -o json | jq -r '.[] | "\(.job_id)\t\(.settings.name)"'

# the run id of the most recent run of a job
databricks jobs list-runs --job-id 123 -o json | jq '.[0].run_id'

# each task of a run: its own run id, its attempt, its state
databricks jobs get-run 456 -o json | jq -c '.tasks[] | {task_key, run_id, attempt_number, state: .state.result_state}'
```

`-o json` on any command is what turns the CLI into automation. Everything the UI does is
available here, and a handful of things — like `submit` — are barely reachable any other
way.

---

## API versions

The CLI speaks the current Jobs API for you, which is the main argument for using it rather
than hand-rolling REST calls: **you do not have to track the API version**. When you do
call REST directly, pin the version in the path and read the release notes before bumping
it — task and trigger schemas have changed between versions.

---

## Where this fits against DABs

| Tool | Good for |
|---|---|
| **DABs** | The **desired state** of a project: jobs, pipelines, permissions, per environment |
| **`jobs` CLI / API** | **Operating** what exists: run, repair, cancel, inspect |

**Trap:** "how should a team manage job definitions across environments" is a DABs
question, not a `jobs create` one. Scripting `jobs create` in CI is the answer people give
before they know about bundles — it works, and it has no state, no diff and no teardown.

Use `bundle deploy` to define, `jobs` to operate.

---

## Secrets never go in the payload

The Jobs CLI help says it outright: *"You should never hard code secrets or store them in
plain text."* A job JSON checked into git with a token in it is a finding, not a style
issue. See the secrets card in Section 10.

---

## Exam traps

- **`reset` replaces everything**; `update` patches. Partial payload to `reset` deletes
  settings.
- **`submit` creates no job**; `run-now` needs one.
- **`repair-run` skips successful tasks** but can duplicate output from a partially-written
  failed task.
- **DABs defines, the jobs CLI operates.**
- **`get-run-output` takes a task run id**: it's how you read a task's result or error from outside
  the UI.
- **`--json` replaces the positional id**: the id goes inside the JSON.

## Resources

- Jobs CLI and API: https://docs.databricks.com/aws/en/jobs/
- Repair runs: https://docs.databricks.com/aws/en/jobs/repair-job-failures
- Triggers: https://docs.databricks.com/aws/en/jobs/triggers
- `jobs` CLI commands: https://docs.databricks.com/aws/en/dev-tools/cli/reference/jobs-commands
- Python SDK: https://docs.databricks.com/aws/en/dev-tools/sdk-python
- CLI overview: https://docs.databricks.com/aws/en/dev-tools/cli/
