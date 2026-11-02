# Lesson 12.4: REST API and CLI commands

Run these from your local clone (lesson 1.4), in `sections/12-debugging-deploying/`, with the CLI profile
from Section 1. Checked on Databricks CLI v1.19.0 on 2026-11-01. Every command takes your default profile,
or add `--profile <name>`. `jq` only pulls fields out of the JSON.

## Door 1: the UI

**Jobs & Pipelines**, **Create**, **Job**, the **Notebook** tile. Task name `refresh`, **Path** your Git
folder's `sections/12-debugging-deploying/12-04-refresh-top-products`. **Save task**. Name the job
`lakeshore-top-products-ui`. Open the kebab menu beside **Run now** and choose **Switch to code version
(YAML)**: the same job as text. Run it once, then delete it. We build the keeper from code.

## Door 2: the CLI

```bash
ME=$(databricks current-user me -o json | jq -r .userName)
REPO=/Users/$ME/dbx-dep-prep-course-student      # your Git folder
sed "s#__REPO__#$REPO#g" 12-04-top-products.job.json > /tmp/top-products.json

JOB=$(databricks jobs create --json @/tmp/top-products.json -o json | jq -r .job_id)
databricks jobs list --name lakeshore-top-products
databricks jobs get $JOB
databricks jobs run-now $JOB                      # waits, then prints the run
```

## Door 3: raw REST, through the CLI and then with curl

```bash
RUN=$(databricks api post /api/2.2/jobs/run-now --json "{\"job_id\": $JOB, \"job_parameters\": {\"top_n\": \"3\"}}" | jq -r .run_id)
databricks api get "/api/2.2/jobs/runs/get?run_id=$RUN" | jq -c '{life: .state.life_cycle_state, result: .state.result_state}'

HOST=$(databricks auth describe -o json | jq -r .details.host)
TOKEN=$(databricks auth token -o json | jq -r .access_token)     # short-lived OAuth, not a PAT
curl -s -H "Authorization: Bearer $TOKEN" "$HOST/api/2.2/jobs/list?limit=2" | jq '.jobs[].settings.name'
unset TOKEN
```

## Automate it: a file-arrival trigger

The trigger is saved paused, so nothing fires until you're ready. Change `pause_status` to `UNPAUSED` to go
live; a job run then starts when new files land in that folder.

```bash
sed "s/__JOB_ID__/$JOB/" 12-04-add-file-arrival.patch.json > /tmp/patch.json
databricks jobs update --json @/tmp/patch.json
databricks jobs get $JOB -o json | jq -c .settings.trigger
```

`jobs update` takes the job id inside the JSON. Passing it as an argument as well is an error.

## Monitor: jobs

```bash
databricks jobs list-runs --job-id $JOB --limit 3 -o json | jq -c '.[] | {run_id, result: .state.result_state}'
RUN=$(databricks jobs list-runs --job-id $JOB --limit 1 -o json | jq -r '.[0].run_id')
databricks jobs get-run $RUN --include-history
TASK=$(databricks jobs get-run $RUN -o json | jq -r '.tasks | max_by(.attempt_number) | .run_id')
databricks jobs get-run-output $TASK -o json | jq -c .notebook_output
```

## Monitor: pipelines

```bash
PIPE=$(databricks pipelines list-pipelines --filter "name LIKE 'lakeshore-platform-sdp'" -o json | jq -r '.[0].pipeline_id')
databricks pipelines list-updates $PIPE --max-results 3
databricks pipelines list-pipeline-events $PIPE --max-results 5
databricks pipelines list-pipeline-events $PIPE --filter "level='ERROR'"
```

`--filter` takes `level='ERROR'`. An equals sign on `event_type` is rejected: `Operation '=' not allowed on
'event_type'`. Filter that one afterwards with `jq`.

## The SDK

Run the notebook `12-04-sdk-monitoring` in your Git folder. It needs no credentials.

## Clean up

```bash
databricks jobs delete $JOB
```
