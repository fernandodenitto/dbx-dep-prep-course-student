---
title: "Reference card: DABs across environments"
section: S12 — Debugging and Deploying
duration: ~7 min read
type: 📄 reference card (Udemy article — no video)
verified: docs.databricks.com + Databricks CLI v1.19.0, 2026-11-01
---

> **📄 Reference card.** No video, nothing to run. Verified against the docs **and the
> CLI (v1.19.0)** on the date in the header. Newer CLIs exist: your binary always wins.
>
> This card assumes the Associate DABs card (commands, file structure, the two modes). It
> covers the layer above: **running the same bundle safely in more than one environment.**

---

## The shape of a multi-target bundle

```yaml
bundle:
  name: lakeshore

variables:
  catalog:
    description: Unity Catalog to deploy into
    default: lakeshore_dev
  deploy_sp:
    description: Application ID of the service principal that runs production

include:
  - resources/*.yml            # forget this and you deploy nothing

targets:
  dev:
    mode: development
    default: true

  staging:
    mode: production
    variables:
      catalog: lakeshore_staging
    run_as:
      service_principal_name: ${var.deploy_sp}
    workspace:
      root_path: /Workspace/.bundle/${bundle.name}/${bundle.target}

  prod:
    mode: production
    git:
      branch: main
    variables:
      catalog: lakeshore_prod
    run_as:
      service_principal_name: ${var.deploy_sp}
    permissions:
      - level: CAN_VIEW
        group_name: data-engineers
    workspace:
      root_path: /Workspace/.bundle/${bundle.name}/${bundle.target}
```

One definition of the job. Three environments. The differences live entirely in `targets`.

---

## Variables, for values that differ

```yaml
variables:
  catalog:
    description: Unity Catalog to deploy into
    default: lakeshore_dev
```

Referenced as `${var.catalog}`, overridden per target, or on the command line:

```bash
databricks bundle deploy -t prod --var "catalog=lakeshore_prod"
```

Built-in substitutions worth knowing: `${bundle.name}`, `${bundle.target}`,
`${workspace.current_user.userName}`, `${workspace.current_user.short_name}`,
`${workspace.file_path}`, `${resources.pipelines.<key>.id}`.

---

## Target overrides, for resources that differ

A variable changes a value. A target can also change the shape of a resource: the `resources`
mapping under a target **joins** the top-level definition, and on a conflict the target wins. Jobs are
matched by their key and tasks by `task_key`. This is in the course's bundle:

```yaml
targets:
  prod:
    resources:
      jobs:
        lakeshore_pro_smoke:
          schedule:
            pause_status: PAUSED
          tasks:
            - task_key: smoke
              max_retries: 1
```

To see the merged result, run `databricks bundle validate -t prod -o json`. It prints every target's
final configuration, so you can check an override before you deploy it.

**Trap:** duplicating a resource file per environment is the wrong answer to every "how do you deploy
to dev and prod" question. One definition, with variables and target overrides for what differs. Copies
drift, and the drift is discovered in production.

---

## `run_as` and `permissions`

```yaml
run_as:
  service_principal_name: ${var.deploy_sp}
permissions:
  - level: CAN_VIEW
    group_name: data-engineers
```

`run_as` is the identity the deployed resources **execute as**. `permissions` is who can
see and operate them.

**The docs recommend a service principal for production.** If you don't use one, the docs say
production mode validates that `run_as` and `permissions` are specified, and that `artifact_path`,
`file_path`, `root_path` and `state_path` aren't overridden to a specific user. A production job under
one engineer's home folder breaks the day they leave.

What the CLI actually did when we tried it (`bundle validate -t prod`, v1.19.0, not using a service
principal):

| `run_as` + `permissions` | `workspace.root_path` | Result |
|---|---|---|
| neither | missing | **Error:** must set `workspace.root_path` |
| neither | set | Validation OK |
| both | missing | Recommendation to set `workspace.root_path` |
| both | set | Validation OK |

So the live check is softer than the docs' wording, and it can change between versions. Set all three
anyway, as the course's bundle does: the mode's point is one shared copy with declared identities.

---

## Git branch enforcement

```yaml
targets:
  prod:
    git:
      branch: main
```

Production mode **validates that the current branch matches**, and it does so at deploy time. In our
test `bundle validate -t prod` passed on a feature branch and `bundle deploy -t prod` refused it.
`--force` overrides the check, and needing `--force` is a signal, not a solution.

---

## The deployment loop, and where CI fits

```bash
databricks bundle validate -t prod        # config is parseable and complete
databricks bundle plan     -t prod        # what would change
databricks bundle deploy   -t prod        # apply
databricks bundle run my_job -t prod      # trigger
databricks bundle summary  -t prod        # what is deployed, with URLs
databricks bundle destroy  -t staging     # tear down an environment
```

**Validate every target in CI, not just the one you use daily.** This course learned that the hard
way: our prod target declared no `root_path` and no identities, it had **never validated**, and nobody
knew, because every deploy went to dev. A CI step that runs `validate` for dev, staging *and* prod
costs seconds. See `bundle/ci/deploy-bundle.github-actions.yml`.

---

## Adopting resources that already exist

```bash
databricks bundle generate job --existing-job-id 123 --key my_job
databricks bundle deployment bind my_job 123
```

`generate` writes YAML from a job someone built in the UI. `bind` links a bundle resource
to an existing one so the next deploy **manages** it instead of creating a duplicate.

**Trap:** deploying a bundle that defines a job with the same name as an unbound existing
job produces **two jobs**, not one. `bind` is the fix, and the exam likes this scenario.

---

## What development mode changes, restated

Because it explains most "it worked in dev" surprises:

- Resources are prefixed `[dev <short_name>]`
- **Schedules and triggers are paused** — override with `pause_status: UNPAUSED`
- Pipelines are marked `development: true`
- Concurrent runs allowed, deployment lock disabled
- `--cluster-id` may override the cluster

Production mode reverses all of it and refuses the cluster override.

---

## Exam traps

- **One definition + variables**, never a copy of the file per environment.
- **Variables change values, target overrides change resources**, joined by key and `task_key`.
- **Production mode wants declared identities**: per the docs, `run_as` and `permissions` when you
  don't deploy as a service principal. The answer the exam wants is a service principal.
- **Prod checks the Git branch at deploy**, when the target sets `git.branch`.
- **`validate` on one target proves nothing about another.**
- **Unbound duplicates:** `bundle deployment bind` adopts an existing resource instead of
  creating a second one.
- **Development mode pauses schedules** — the usual cause of "my job never ran".

## Resources

- Bundle configuration: https://docs.databricks.com/aws/en/dev-tools/bundles/settings
- Target overrides: https://docs.databricks.com/aws/en/dev-tools/bundles/overrides
- Run identity: https://docs.databricks.com/aws/en/dev-tools/bundles/run-as
- Deployment modes: https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes
- Bundles overview: https://docs.databricks.com/aws/en/dev-tools/bundles/
