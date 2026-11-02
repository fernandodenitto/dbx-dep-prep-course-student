# Lesson 12.8: Git folder commands

Run these in a terminal, with the CLI profile from Section 1. Checked on Databricks CLI v1.19.0 on
2026-11-01. `jq` only pulls one field out of the JSON. Without it, read the JSON and paste the value.

```bash
ME=$(databricks current-user me -o json | jq -r .userName)
URL=https://github.com/fernandodenitto/dbx-dep-prep-course-student
SIM=/Users/$ME/lakeshore-prod-sim
```

## Your own Git folder: which branch, which commit

```bash
databricks repos get /Users/$ME/dbx-dep-prep-course-student
databricks repos update /Users/$ME/dbx-dep-prep-course-student --branch main
```

`update` pulls the latest commit of that branch, or switches to it. A branch that isn't on the remote
fails with `Remote branch does not exist`. `--tag v1.2.0` pins a tag instead.

## A pretend production folder

A real one sits outside user folders and an admin creates it. This one sits in your home folder,
because you're the only person here, but the commands are the same.

```bash
databricks repos create $URL gitHub --path $SIM
databricks repos get $SIM -o json | jq '{branch, head_commit_id}'
databricks repos update $SIM --branch main

REPO_ID=$(databricks repos get $SIM -o json | jq -r .id)
databricks repos get-permission-levels $REPO_ID
databricks repos get-permissions $REPO_ID
```

To sync it from a job instead, run `12-08-sync-git-folder` with `git_folder` set to the value of `$SIM`.

## Clean up

```bash
databricks repos delete $REPO_ID
```

## Don't trust `repos list`

`databricks repos list` is deprecated. Its help says it misses repos with the Git CLI enabled, and the
folder above is one: `repos get` shows `"git_cli_enabled": true`, and `repos list` returned nothing
for it. Ask for a folder by path with `repos get`.

## The CI samples

`bundle/ci/deploy-bundle.github-actions.yml` deploys the bundle, and
`bundle/ci/sync-git-folder.github-actions.yml` syncs a production Git folder. Both authenticate with
workload identity federation, so they cannot run against Free Edition: the federation policy is an
account-level object. Lesson 12.8 explains the path.
