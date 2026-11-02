---
title: "Monitoring commands: CLI and REST"
section: S9 — Monitoring and Alerting
type: 📄 reference for lesson 9.4 (run on your laptop)
verified: CLI v1.19.0 on Free Edition, 2026-11-01
---

Every command below ran on the Databricks CLI v1.19.0. You installed and authenticated the CLI in Section 1. Add
`-o json` to any of them when a script reads the output.

## Jobs

```bash
databricks jobs list --name lakeshore-platform-refresh     # find the job id
databricks jobs get <job-id>                               # its full definition
databricks jobs list-runs --job-id <job-id> --limit 5      # run history, newest first
databricks jobs get-run <run-id>                           # states, durations, each task's run id
databricks jobs get-run-output <task-run-id>               # one TASK's output or error
```

`get-run-output` wants the **task's** run id, which `get-run` gives you. With the parent run id of a multi-task job you get
"Retrieving the output of runs with multiple tasks is not supported."

## Pipelines

```bash
databricks pipelines list-pipelines --filter "name LIKE 'lakeshore-platform-sdp'"
databricks pipelines get <pipeline-id>
databricks pipelines list-updates <pipeline-id> --max-results 5
databricks pipelines get-update <pipeline-id> <update-id>
databricks pipelines list-pipeline-events <pipeline-id> --filter "level='ERROR'" --max-results 20
```

Poll the **update's** state: COMPLETED, FAILED or CANCELED. The pipeline's own state goes back to IDLE either way.

## REST, through the CLI

The same endpoints without writing the auth yourself. On a laptop, this is one `curl` with a bearer token away.

```bash
databricks api get /api/2.2/jobs/runs/list --json '{"job_id": <job-id>, "limit": 5}'
```

## Job notifications (lesson 9.4)

Save as `notify.json` with your job id and email, then run `databricks jobs update --json @notify.json`.

```json
{"job_id": 0,
 "new_settings": {
   "email_notifications": {
     "on_failure": ["you@example.com"],
     "on_duration_warning_threshold_exceeded": ["you@example.com"]},
   "health": {"rules": [{"metric": "RUN_DURATION_SECONDS", "op": "GREATER_THAN", "value": 1800}]},
   "notification_settings": {"no_alert_for_skipped_runs": true}}}
```

## Alerts (lesson 9.3)

`my-alert.json` is the file you made in lesson 9.3, at the root of your local clone. Run these from there.

```bash
databricks alerts-v2 create-alert --json @my-alert.json
databricks alerts-v2 list-alerts
databricks alerts-v2 get-alert <alert-id>
databricks alerts-v2 update-alert <alert-id> display_name --json '{"display_name": "new-name"}'
databricks alerts-v2 trash-alert <alert-id>
```

## Resources

- Databricks CLI: https://docs.databricks.com/aws/en/dev-tools/cli/
- Jobs REST API: https://docs.databricks.com/api/workspace/jobs
- Databricks SDK for Python: https://docs.databricks.com/aws/en/dev-tools/sdk-python
