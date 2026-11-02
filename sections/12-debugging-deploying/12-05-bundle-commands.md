# Lesson 12.5: bundle commands

Run these from a local clone of this repo, in `sections/12-debugging-deploying/bundle/`, with the CLI
profile from Section 1. Checked on Databricks CLI v1.19.0 on 2026-11-01.

```bash
git clone https://github.com/fernandodenitto/dbx-dep-prep-course-student
cd dbx-dep-prep-course-student/sections/12-debugging-deploying/bundle
```

## Validate both targets

```bash
databricks bundle validate -t dev
databricks bundle validate -t prod
databricks bundle validate -t prod -o json | jq '.resources.jobs.lakeshore_pro_smoke.schedule'
```

The last line shows a target override after the merge: `pause_status` is `PAUSED`.

## Deploy and run dev

```bash
databricks bundle deploy -t dev
databricks bundle run lakeshore_pro_smoke -t dev
databricks bundle summary -t dev
```

It prints the pytest report, then the last line of the job: `smoke-ok:lakeshore:6 tables`.
The job creates `[dev <you>] lakeshore-pro-smoke`, the pipeline
`[dev <you>] lakeshore-pro-graded-orders`, and the schema `lakeshore.dev_<you>_pro_bundle_dev`.

## Clean up

```bash
databricks bundle destroy -t dev --auto-approve
```

Destroying deletes the job, the pipeline and the schema with its table. Your platform tables are not
in this bundle, so they stay.

## Prod stays described

`bundle validate -t prod` is enough here. Free Edition has one workspace, so a prod deploy would land
beside dev and prove nothing dev didn't. A company deploys prod from CI as a service principal:
lesson 12.8 and `ci/deploy-bundle.github-actions.yml`.
