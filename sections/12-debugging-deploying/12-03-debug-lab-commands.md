# Lesson 12.3: debug lab commands

Run these from a local clone of this repo, in `sections/12-debugging-deploying/`, with the CLI profile
from Section 1. Checked on Databricks CLI v1.19.0 on 2026-11-01. The lab's tasks read
`lakeshore.gold.fact_order_items` and write two small tables, `gold.debug_lab_revenue` and
`gold.debug_lab_top`. `jq` only pulls fields out of the JSON.

## Create the job

```bash
ME=$(databricks current-user me -o json | jq -r .userName)
REPO=/Users/$ME/dbx-dep-prep-course-student      # your Git folder
sed "s#__REPO__#$REPO#g" 12-03-debug-lab.job.json > /tmp/debug-lab.json
JOB=$(databricks jobs create --json @/tmp/debug-lab.json -o json | jq -r .job_id)
```

`rank_top` sets `disable_auto_optimization`. Serverless auto-optimization retries failed tasks by default,
and the docs recommend leaving it on. We turn it off on this one task because a bad parameter doesn't
heal on retry, and the extra attempt would muddy the run history you're about to read.

## Act 1: break it with a parameter, repair with an override

```bash
RUN=$(databricks jobs run-now --no-wait --json "{\"job_id\": $JOB, \"job_parameters\": {\"top_n\": \"-1\"}}" -o json | jq -r .run_id)
databricks jobs get-run $RUN -o json | jq -c '{life: .state.life_cycle_state, result: .state.result_state}'
```

Repeat that second line until it stops saying `RUNNING`, then read the tasks:

```bash
databricks jobs get-run $RUN -o json | jq -c '.tasks[] | {task_key, run_id, attempt_number, state: .state.result_state}'
TASK=$(databricks jobs get-run $RUN -o json | jq -r '[.tasks[] | select(.task_key=="rank_top")] | max_by(.attempt_number) | .run_id')
databricks jobs get-run-output $TASK -o json | jq -r .error
```

Now repair the same run. The run id goes inside the JSON: `--json` replaces positional arguments.

```bash
REPAIR=$(databricks jobs repair-run --no-wait --json "{\"run_id\": $RUN, \"rerun_all_failed_tasks\": true, \"job_parameters\": {\"top_n\": \"5\"}}" -o json | jq -r .repair_id)
databricks jobs get-run $RUN --include-history -o json | jq -c '{result: .state.result_state, repairs: [.repair_history[]? | {type, id}], tasks: [.tasks[] | {task_key, attempt_number, state: .state.result_state}]}'
databricks jobs get $JOB -o json | jq -c .settings.parameters
```

`prepare` still shows one attempt, and the job's stored `top_n` is still `5`.

## Act 2: break it in the code, fix it, repair

In your Git folder, open `12-03-debug-target` and change `orderBy("revenue"` to `orderBy("revenue_typo"`.

```bash
RUN2=$(databricks jobs run-now $JOB --no-wait -o json | jq -r .run_id)
```

When it has failed, repair without fixing anything. It fails again, on the same error:

```bash
REPAIR=$(databricks jobs repair-run --no-wait --json "{\"run_id\": $RUN2, \"rerun_tasks\": [\"rank_top\"]}" -o json | jq -r .repair_id)
```

Now change the code back, and repair again. A second repair must name the previous one, and
`rerun_tasks` alone would leave `publish` skipped and the run FAILED, so ask for the dependents too:

```bash
databricks jobs repair-run --no-wait --json "{\"run_id\": $RUN2, \"latest_repair_id\": $REPAIR, \"rerun_tasks\": [\"rank_top\"], \"rerun_dependent_tasks\": true}" -o json
```

Discard your edit in the Git dialog if the file still shows as changed.

## Past the incident: system tables

Records typically arrive within an hour, so run this later, in the SQL editor, with your job id:

```sql
SELECT period_start_time, task_key, result_state, termination_code
FROM system.lakeflow.job_task_run_timeline
WHERE job_id = :job_id
ORDER BY period_start_time DESC
LIMIT 20;
```

Keep the job: `12-04-sdk-monitoring` reads its runs. Lesson 12.4 deletes nothing of it, so delete it when you
finish the section with `databricks jobs delete $JOB`.
