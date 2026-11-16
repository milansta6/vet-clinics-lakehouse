# Performance and cost: the choices, in plain language

This project is small (a few thousand rows), so nothing here is needed to make it *fast*.
The point is to show the habits that matter when the same pipeline handles millions of rows
and somebody has to pay the cloud bill.

**The short version**

| Choice | Why |
|---|---|
| Load only new files in Bronze | Do not pay to read old files again every day |
| Process only new Bronze rows in Silver | The daily cost depends on the daily volume, not on the full history |
| MERGE instead of rewriting tables | Only files that hold changed rows are rewritten |
| Partition only large, date-driven tables | Partitions help big tables and hurt small ones |
| OPTIMIZE + Z-ORDER on a schedule | Fewer, larger, sorted files make reads cheaper |
| Small pre-aggregated Gold tables | Power BI reads 1,000 rows instead of scanning the fact table |
| Serverless / job compute, no always-on cluster | Pay only while the pipeline runs |

---

## 1. Incremental processing: the biggest cost saver

Reading data costs compute time. The cheapest data is the data you do not read.

- **Bronze** keeps the name of every loaded file in `_source_file`. A run lists the landing
  folder and reads only files that are not in the table yet. On the second run in this repo
  (`make run-incremental`), 30 new files are read: 14 days of appointments and invoices plus
  two exchange rate files. The 126 files of the first run are skipped.
- **Silver** keeps a watermark per table (`pipeline_watermarks`): the newest `_ingested_at` it
  has already processed. A run only transforms Bronze rows that are newer.
- **Silver writes with MERGE.** Delta only rewrites the data files that contain changed rows.
  A full overwrite would rewrite everything, every day.

**What I would change at scale:** replace the file listing with **Auto Loader** (`cloudFiles`).
It tracks loaded files in a checkpoint and can use storage notifications, so it does not need
to list a folder with millions of files or scan the Bronze table for file names. I did not use
it here because it only exists on Databricks, and this repo must also run on a laptop.

## 2. Partitioning: where it makes sense, and where it does not

Partitioning splits a table into folders by a column value. A query that filters on that
column can skip whole folders. But every partition holds at least one file, and many tiny
files are slow and expensive to list and open.

The usual advice from Databricks: do not partition tables smaller than about 1 TB, and aim
for partitions of at least 1 GB.

| Table | Partitioned by | Reason |
|---|---|---|
| `bronze.appointments`, `bronze.invoices` | `_ingest_date` | They grow every day and never change. One partition per load day makes it cheap to reprocess or delete one day, and to apply retention rules |
| `gold.fact_invoice` | `invoice_year_month` | Reports almost always filter on a period. Monthly (not daily) partitions keep the number of files low |
| Silver tables | not partitioned | They are updated with MERGE on a business key. Partitioning would not help the MERGE and would create many small files. Z-ORDER is used instead |
| Dimensions and aggregates | not partitioned | Far too small |

At this demo's size the partitioning has no measurable effect. It is there to show the
layout I would start with for real volumes.

**Mistakes I avoided on purpose**

- Partitioning by a column with many different values, such as `clinic_id` + day or `pet_id`:
  this creates thousands of tiny files.
- Partitioning a table that is mostly updated by key (Silver): MERGE has to touch many
  partitions anyway.

## 3. OPTIMIZE and Z-ORDER

Daily appends and MERGEs create many small files. Two commands fix this (see
`src/vet_lakehouse/maintenance.py` and `notebooks/05_maintenance.py`):

```sql
OPTIMIZE vet_silver.invoices ZORDER BY (clinic_id, invoice_date);
OPTIMIZE vet_gold.fact_invoice ZORDER BY (clinic_key, date_key);
```

- **OPTIMIZE** rewrites many small files into fewer large ones. Fewer files means fewer
  storage requests and less overhead per query.
- **ZORDER BY** sorts related rows into the same files. Delta stores the minimum and maximum
  value of each column per file, so a query such as `WHERE clinic_id = 'CL007'` can skip
  files that cannot contain that clinic.
- I chose Z-ORDER columns that are used in **filters and joins**: clinic and date. Columns
  that are already partition columns cannot be used (and do not need to be).
- More than two or three Z-ORDER columns makes each one less effective.

**OPTIMIZE is not free:** it rewrites data. That is why it runs in a separate, weekly
maintenance step and not after every load.

**VACUUM** deletes files that are no longer part of the table. The default keeps 7 days,
which is what time travel and long-running readers need. I keep the default.

**What I would use on current Databricks:** *liquid clustering* (`CLUSTER BY`) instead of
partitioning + Z-ORDER for new tables, and *predictive optimization* so Databricks decides
when to run OPTIMIZE and VACUUM. Liquid clustering lets you change the clustering columns
later without rewriting the table. I show the classic approach here because it also works
with open-source Delta Lake on a laptop, and because it is still what many existing
platforms use.

## 4. Gold: full rebuild now, incremental later

Gold is rebuilt completely on every run (`overwrite`). For a few thousand rows this takes
seconds, is always correct, and is easy to understand.

When the fact table gets large, I would:

1. Append/MERGE only the changed invoices into `fact_invoice` (reuse the Silver watermark
   idea or Delta Change Data Feed).
2. Recalculate only the affected days and months in the aggregate tables
   (`replaceWhere` on the date range), or use materialized views in Databricks SQL.

The three aggregate tables exist for cost reasons too: a Power BI visual that reads
`daily_revenue` (about 750 rows) is cheaper than one that scans and joins the fact table.

## 5. Compute

- **Databricks:** the job has no cluster definition, so it runs on **serverless** compute:
  it starts in seconds and is billed only while tasks run. On a workspace without serverless
  I would use a small **job cluster** (cheaper per hour than all-purpose compute) with
  auto-termination, never an always-on interactive cluster.
- **One job, five tasks, one after another.** The data is small; parallel tasks would add
  complexity without saving money.
- **Locally:** `spark.sql.shuffle.partitions` is set to 4 instead of the default 200. With
  tiny data, 200 shuffle partitions mean 200 almost empty tasks per stage. On Databricks I
  leave this to Adaptive Query Execution.
- **Broadcast joins:** small lookup tables (clinics, FX rates) are broadcast in the Silver
  joins, which avoids shuffling the large side.
- **No caching and no Python UDFs.** All logic uses built-in Spark functions, which the
  engine can optimise (and Photon can accelerate). Python UDFs process data row by row and
  are much slower.

## 6. Storage

- **Explicit schemas in Bronze.** No schema inference, so files are read once, not twice.
- **Raw files:** the example Terraform moves the `landing` container to the cool tier after
  30 days and deletes files after one year. Bronze already holds a full copy of the raw data.
- **LRS redundancy** in dev (cheapest). For production I would choose ZRS.
- **The quarantine table grows forever** if nobody looks at it. I would add a retention rule
  (for example 90 days) and an alert on the quarantine share, which the DQ suite already
  measures.

## 7. How I would measure it

Guessing is not tuning. On Databricks I would look at:

- `DESCRIBE DETAIL <table>`: number of files and table size (before and after OPTIMIZE),
- `DESCRIBE HISTORY <table>`: rows and files changed by each MERGE,
- the query profile: how many files were skipped ("files pruned"),
- the `system.billing.usage` table: DBUs per job run, to see the cost trend per day.
