---
title: "Reference card: secrets"
section: S10 — Security and Compliance
duration: ~5 min read
type: 📄 reference card (Udemy article — no video)
verified: Databricks CLI v1.19.0 (`--help` output) + docs.databricks.com, 2026-11-01
---

> **📄 Reference card.** No video, nothing to run. Beyond the exam: the October guide doesn't
> name secrets. Commands read from **the installed CLI, v1.19.0**.

Lesson 10.6 stored a credential properly. This card is the full surface, including the ACL
model that most material skips.

---

## Scopes and secrets

```bash
databricks secrets create-scope lakeshore
databricks secrets put-secret lakeshore api_token          # prompts for the value
databricks secrets list-scopes
databricks secrets list-secrets lakeshore      # keys only — never values
databricks secrets delete-secret lakeshore api_token
databricks secrets delete-scope lakeshore
```

A **scope** is a named container; secrets live inside it as key/value pairs. The creator
gets `MANAGE` on the scope automatically. Scope names are alphanumeric plus `-`, `_`, `@`
and `.`, up to 128 characters, unique per workspace.

**Databricks-backed scopes** store values in an encrypted database owned and managed by
Databricks. That is the default and the one you use here.

**`list-secrets` returns keys, never values.** There is no CLI path that dumps every secret
in a scope, and that is deliberate.

---

## ACLs — the part usually missed

```bash
databricks secrets put-acl    lakeshore data-engineers READ
databricks secrets list-acls  lakeshore
databricks secrets get-acl    lakeshore data-engineers
databricks secrets delete-acl lakeshore data-engineers
```

| Permission | Allows |
|---|---|
| `READ` | Read secret values and list keys |
| `WRITE` | `READ`, plus create and delete secrets |
| `MANAGE` | `WRITE`, plus manage the ACLs themselves |

Grant to **groups**, not to individuals — the same rule as Unity Catalog.

**Note the model difference:** scope ACLs are **not** Unity Catalog grants. It's a separate,
older permission system with its own three levels.

---

## Two kinds of secrets

| | Secret scope | Unity Catalog secret |
|---|---|---|
| Lives at | `scope` + `key`, per workspace | `catalog.schema.secret`, in the metastore |
| Permissions | scope ACLs: READ / WRITE / MANAGE | Unity Catalog privileges |
| CLI | `databricks secrets` | `databricks secrets-uc` |
| Status | the long-standing option, used in this course | GA since 3 August 2026 |

Reach for the governed object when you can, for the same reason as everywhere else in Unity
Catalog: owner, grants, audit. This course's hands-on stays on scopes, for a measured reason:
on Free Edition's serverless compute the API created a Unity Catalog secret, but `DROP SECRET`
in SQL answered `UC_SECRETS_NOT_ENABLED` (2026-11-01). The docs' federation `secret()` example
uses scopes too.

---

## Reading a secret

```python
token = dbutils.secrets.get(scope="lakeshore", key="api_token")
```

```bash
databricks secrets get-secret lakeshore api_token
```

---

## Redaction, and its limit

Printing a variable that holds a secret retrieved with `dbutils.secrets.get()` displays
`[REDACTED]`. Databricks tracks the literal value through notebook output.

The docs are equally clear about the limit: redaction *"does not prevent deliberate and
arbitrary transformations of a secret literal."* Print it one character at a time, or
base64 it first, and it will appear in plain text.

**So redaction is a guardrail against accident, not a security control against intent.**
An exam answer treating it as protection from a malicious user is wrong.

---

## Where secrets belong

| Situation | Do |
|---|---|
| Credential in notebook code | `dbutils.secrets.get(...)` |
| Credential in a job definition JSON | **Never inline it** — reference a secret |
| Credential in a bundle YAML in git | Never. Use a variable that resolves to a secret |
| Reading external data regularly | Prefer a **Unity Catalog connection or service credential** over a raw secret |

That last row matters for the Professional exam: Unity Catalog governs *access to external
systems* through connections, storage credentials and service credentials — objects with
owners, grants and audit trails. A secret in a scope has none of that. Reach for the
governed object first; use a secret when there isn't one.

---

## Traps

- **`list-secrets` never returns values.**
- **Scope ACLs are their own system** — `READ` / `WRITE` / `MANAGE`, not UC privileges. Unity
  Catalog secrets are the governed alternative.
- **Redaction is defeated by transformation.** It stops accidents, not attackers.
- **The creator of a scope gets `MANAGE`** automatically.
- **Prefer UC connections and credentials** over raw secrets when the option exists.

## Resources

- Secret management: https://docs.databricks.com/aws/en/security/secrets/
- Secrets utility: https://docs.databricks.com/aws/en/dev-tools/databricks-utils
