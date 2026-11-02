---
title: "Reference card: studying with Lakenaut and the official docs"
section: S13 — Exam Strategy
duration: ~4 min read
type: 📄 reference card (Udemy article — no video)
exam-domains: all (support material)
verified: lakenaut.dev links checked 2026-11-01
---

> **📄 Reference card.** No video, nothing to run. It's the map of where to read more after a
> lesson, and what to trust when two sources disagree.

## What Lakenaut is

[Lakenaut](https://lakenaut.dev/) is a free study site about Databricks, one page per concept,
organized by learning path and by certification. **I built it**, so weigh this recommendation
accordingly. It's an independent project, not affiliated with Databricks, and you don't need it to
pass: what the exam asks is in this course and in the docs.

## Which source for what

| You want | Go to |
|---|---|
| What a feature actually does, its limits, its exact syntax | [docs.databricks.com](https://docs.databricks.com/). **This is the authority** |
| What's on the exam, and the official sample questions | the [current exam guide](https://www.databricks.com/sites/default/files/2026-10/Databricks-Certified-Data-Engineer-Professional-Exam-Guide-9-Oct-2026.pdf) |
| A second explanation of a concept, in plain words | a Lakenaut concept page |
| To see how concepts connect | the [concept graph](https://lakenaut.dev/graph/) and [learning paths](https://lakenaut.dev/paths/) |
| To follow the exam domain by domain | the [Professional roadmap](https://lakenaut.dev/roadmaps/de-professional/) |
| "What is this thing called now?" | [renames](https://lakenaut.dev/naming/) |
| "Is this safe to build on, or still in preview?" | [previews](https://lakenaut.dev/preview/) |
| "When does this stop being supported?" | [end of support](https://lakenaut.dev/lifecycle/) |
| What changed recently | [updates](https://lakenaut.dev/updates/) |

**When sources disagree, the docs and the current exam guide win.** That includes this course and
Lakenaut. This exam changed on 9 October 2026, and the exam page lagged behind its own guide, so
check that any roadmap you follow names the same guide version you're preparing for.

## How to use it with the course

1. Watch the lesson and run the notebook. Hands-on first.
2. If something didn't land, read its concept page below for a second angle.
3. Open the doc links in that lesson's Resources. The exam allows no aids, but the docs are how you learn the limits it asks about.
4. Before the practice exam, walk the roadmap and read the concepts you can't explain out loud.

## Concept pages by section

| Section | Topic | Concept pages |
|---|---|---|
| 2 | Data modeling | [medallion-architecture](https://lakenaut.dev/concepts/medallion-architecture/) · [gold-layer-objects](https://lakenaut.dev/concepts/gold-layer-objects/) · [metric-views](https://lakenaut.dev/concepts/metric-views/) · [data-layout-partitioning-zorder](https://lakenaut.dev/concepts/data-layout-partitioning-zorder/) · [liquid-clustering](https://lakenaut.dev/concepts/liquid-clustering/) · [iceberg-interoperability](https://lakenaut.dev/concepts/iceberg-interoperability/) |
| 3 | Advanced streaming | [structured-streaming-basics](https://lakenaut.dev/concepts/structured-streaming-basics/) · [streaming-triggers](https://lakenaut.dev/concepts/streaming-triggers/) · [streaming-watermarks-state](https://lakenaut.dev/concepts/streaming-watermarks-state/) · [foreachbatch](https://lakenaut.dev/concepts/foreachbatch/) · [kafka-streaming](https://lakenaut.dev/concepts/kafka-streaming/) · [data-quality-overview](https://lakenaut.dev/concepts/data-quality-overview/) |
| 4 | CDC, CDF, stream joins | [merge-upsert](https://lakenaut.dev/concepts/merge-upsert/) · [change-data-feed](https://lakenaut.dev/concepts/change-data-feed/) · [pipelines-auto-cdc](https://lakenaut.dev/concepts/pipelines-auto-cdc/) · [table-history-and-checkpoints](https://lakenaut.dev/concepts/table-history-and-checkpoints/) |
| 5 | Python and SQL processing | [sql-window-functions](https://lakenaut.dev/concepts/sql-window-functions/) · [sql-joins-and-sets](https://lakenaut.dev/concepts/sql-joins-and-sets/) · [udfs-and-alternatives](https://lakenaut.dev/concepts/udfs-and-alternatives/) · [semi-structured-data](https://lakenaut.dev/concepts/semi-structured-data/) · [ai-functions-sql](https://lakenaut.dev/concepts/ai-functions-sql/) · [python-in-notebooks](https://lakenaut.dev/concepts/python-in-notebooks/) |
| 6 | Ingestion and acquisition | [ingestion-patterns](https://lakenaut.dev/concepts/ingestion-patterns/) · [auto-loader](https://lakenaut.dev/concepts/auto-loader/) · [copy-into](https://lakenaut.dev/concepts/copy-into/) · [lakeflow-connect](https://lakenaut.dev/concepts/lakeflow-connect/) · [iceberg-interoperability](https://lakenaut.dev/concepts/iceberg-interoperability/) · [kafka-streaming](https://lakenaut.dev/concepts/kafka-streaming/) |
| 7 | Cost and performance | [delta-optimize-vacuum](https://lakenaut.dev/concepts/delta-optimize-vacuum/) · [liquid-clustering](https://lakenaut.dev/concepts/liquid-clustering/) · [predictive-optimization](https://lakenaut.dev/concepts/predictive-optimization/) · [deletion-vectors](https://lakenaut.dev/concepts/deletion-vectors/) · [query-profile](https://lakenaut.dev/concepts/query-profile/) · [sql-query-caching](https://lakenaut.dev/concepts/sql-query-caching/) · [spark-ui-bottlenecks](https://lakenaut.dev/concepts/spark-ui-bottlenecks/) · [cost-attribution-and-budgets](https://lakenaut.dev/concepts/cost-attribution-and-budgets/) |
| 8 | Production pipelines and jobs | [pipelines-overview](https://lakenaut.dev/concepts/pipelines-overview/) · [pipelines-expectations](https://lakenaut.dev/concepts/pipelines-expectations/) · [pipelines-auto-cdc](https://lakenaut.dev/concepts/pipelines-auto-cdc/) · [pipelines-sql-vs-python](https://lakenaut.dev/concepts/pipelines-sql-vs-python/) · [pipelines-sinks](https://lakenaut.dev/concepts/pipelines-sinks/) · [jobs-control-flow](https://lakenaut.dev/concepts/jobs-control-flow/) · [jobs-triggers](https://lakenaut.dev/concepts/jobs-triggers/) · [jobs-repair-runs](https://lakenaut.dev/concepts/jobs-repair-runs/) · [serverless-compute](https://lakenaut.dev/concepts/serverless-compute/) |
| 9 | Monitoring and alerting | [system-tables](https://lakenaut.dev/concepts/system-tables/) · [pipelines-event-log](https://lakenaut.dev/concepts/pipelines-event-log/) · [runs-monitoring](https://lakenaut.dev/concepts/runs-monitoring/) · [alerts-overview](https://lakenaut.dev/concepts/alerts-overview/) · [spark-ui-bottlenecks](https://lakenaut.dev/concepts/spark-ui-bottlenecks/) · [cli-and-sdk](https://lakenaut.dev/concepts/cli-and-sdk/) |
| 10 | Security and compliance | [privileges-grant-revoke](https://lakenaut.dev/concepts/privileges-grant-revoke/) · [row-filters-column-masks](https://lakenaut.dev/concepts/row-filters-column-masks/) · [abac-policies](https://lakenaut.dev/concepts/abac-policies/) · [governed-tags](https://lakenaut.dev/concepts/governed-tags/) · [data-classification](https://lakenaut.dev/concepts/data-classification/) · [secrets-management](https://lakenaut.dev/concepts/secrets-management/) |
| 11 | Governance, sharing, federation | [unity-catalog-overview](https://lakenaut.dev/concepts/unity-catalog-overview/) · [unity-catalog-lineage](https://lakenaut.dev/concepts/unity-catalog-lineage/) · [governed-tags](https://lakenaut.dev/concepts/governed-tags/) · [opensharing-overview](https://lakenaut.dev/concepts/opensharing-overview/) · [marketplace-delta-sharing](https://lakenaut.dev/concepts/marketplace-delta-sharing/) · [lakehouse-federation](https://lakenaut.dev/concepts/lakehouse-federation/) |
| 12 | Debugging and deploying | [cluster-troubleshooting](https://lakenaut.dev/concepts/cluster-troubleshooting/) · [jobs-repair-runs](https://lakenaut.dev/concepts/jobs-repair-runs/) · [bundles-overview](https://lakenaut.dev/concepts/bundles-overview/) · [bundles-variables-targets](https://lakenaut.dev/concepts/bundles-variables-targets/) · [bundles-ci-cd](https://lakenaut.dev/concepts/bundles-ci-cd/) · [git-folders](https://lakenaut.dev/concepts/git-folders/) · [cli-and-sdk](https://lakenaut.dev/concepts/cli-and-sdk/) |

Lakenaut's writing is CC BY-NC-SA 4.0, so link to it rather than copying it.
