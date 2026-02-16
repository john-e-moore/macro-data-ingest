# TLG State-Level Prices Data Pipeline (BEA PCE) — Codex Prompt

You are ChatGPT Codex running inside Cursor. Create a new repository that implements a **data ingestion + warehousing pipeline** for **BEA PCE by State** with a **Bronze / Silver / Gold** lakehouse pattern on **S3** and a small **Postgres RDS** instance for serving curated tables and views.

This repo must remain **lightweight** while following **data engineering best practices** (idempotency, observability, documentation, tests, reproducibility).

---

## Goal (What this project must do)
Build an end-to-end, repeatable pipeline that:
- Ingests **BEA PCE by State** programmatically (via BEA API; categories configurable)
- Stores **immutable raw source responses** in **S3 Bronze**
- Produces **cleaned/typed standardized datasets** in **S3 Silver**
- Produces **analytics-ready outputs** in **S3 Gold** and loads curated datasets into **Postgres (RDS)**
- Runs on a daily schedule (GitHub Actions): checks for updates and only reprocesses when data changes
- Provides a clear workflow so analysts can later connect BI tools / notebooks to RDS

Assume the repository is dedicated to data engineering (ingest + transforms + load). Downstream dashboards and reporting are out of scope.

---

## Non-goals (Explicitly out of scope for now)
- No BI dashboards required
- No heavy orchestration platform required (use GitHub Actions)
- No streaming (batch only)
- No complex infra-as-code required (a single script for AWS resource creation is sufficient)

---

## Target architecture (High-level)
1. **Scheduler**: GitHub Actions runs daily (and supports manual runs).
2. **Ingestion**:
   - Calls BEA API (API key from secrets).
   - Detects new/revised data (compare hashes / vintages / last-known release metadata).
   - Writes raw payloads to **S3 Bronze** using a partitioned layout:
     - `s3://<bucket>/<env>/bronze/<source>/<dataset>/<extract_date>/...`
   - Writes a manifest entry per run (timestamp, endpoint + params, row counts, hash).
3. **Transformation**:
   - Reads Bronze → produces cleaned datasets in Silver (typed columns, normalized geo/time keys, stable schemas).
   - Produces analytics-ready Gold outputs (Parquet recommended).
4. **Load to RDS (Postgres)**:
   - Loads Gold tables into Postgres with idempotent upserts.
   - Creates/refreshes views for common derivatives (YoY, MoM, rolling averages) where appropriate.
5. **Observability + Quality**:
   - Structured logging
   - Run metadata + checkpoints
   - Basic data quality checks and failure handling

---

## NEW: AWS resource creation script (staging + prod)
Add a script that creates the AWS resources required for both **staging** and **prod** environments.

### Requirements
- The script should be runnable locally after the user populates `.env` with AWS credentials and desired settings.
- It should support **two environments** (`staging`, `prod`) with clean naming conventions and isolation.
- Keep it lightweight: use either **AWS CLI** calls or **boto3** (choose one; keep dependencies minimal).
- It must be safe and repeatable (idempotent where possible). If a resource exists, it should reuse it or exit cleanly.

### Resources to create (minimum)
- **S3 bucket(s)** (either one bucket with env prefixes or separate buckets; your choice)
  - Enable: versioning, encryption (SSE-S3 or SSE-KMS), and sensible public access blocks
  - Optional but preferred: lifecycle policies (e.g., Bronze raw retention longer; logs retention; etc.)
- **IAM roles/policies** for GitHub Actions and for the pipeline runtime
  - Principle of least privilege for S3 + RDS access
- **RDS Postgres instance** (small) OR an RDS cluster if you think it’s justified (default to simple instance)
  - Security group(s) and networking basics
  - Parameter group defaults OK; keep it minimal
- **CloudWatch** log group(s) (or equivalent) for pipeline logs (even if logs also go to stdout)
- Optional but recommended lightweight extras:
  - **SNS topic** (or similar) for failure alerts
  - **S3 bucket for access logs** if you choose to enable it

### Script UX
- Provide a single command like:
  - `python scripts/provision_aws.py --env staging`
  - `python scripts/provision_aws.py --env prod`
- Output a summary of created resources and the values that need to be copied into `.env` / GitHub secrets.
- Include rollback guidance (even if manual).

### After provisioning
Document clearly that the user will:
1) Fill `.env` with AWS credentials and config  
2) Run the provisioning script for staging (then prod later)  
3) Add outputs (bucket names, RDS endpoint, IAM role info) to GitHub secrets  
4) Proceed with pipeline runs

---

## Repository deliverables (Files Codex must create)
Create a clean repo with code + documentation. You decide the best structure, but include at least:

### 1) Documentation
- `README.md`
  - What this repo does
  - Quickstart (local dev)
  - How to provision AWS resources
  - How to run ingestion/transforms/load
  - How deployments/schedules work
- `docs/setup.md`
  - Prereqs (AWS creds, BEA API key, Postgres connection)
  - Local environment setup
  - `.env` schema and examples (no real secrets)
  - GitHub Actions secrets setup
- `docs/architecture.md`
  - Bronze/Silver/Gold definitions
  - S3 partitioning conventions
  - Postgres serving model (schemas, tables, views conceptually)
  - Environment strategy (staging vs prod)
- `docs/spec.md`
  - Functional requirements
  - Non-functional requirements (idempotency, reproducibility)
  - Dataset scope: BEA PCE by State categories to ingest (leave specifics configurable)
- `docs/roadmap.md`
  - MVP → v1 → v2 milestones
- `docs/operability.md` (NEW)
  - Logging strategy (where logs go, log levels, correlation IDs/run IDs)
  - Data quality checks
  - Alerting approach
  - Backfill strategy
  - Cost controls / lifecycle policies

### 2) Code
Implement a minimal but real pipeline with:
- A CLI entrypoint (e.g., `make ingest`, `make transform`, `make load`, or a Python CLI tool)
- Modules for:
  - **BEA API client**
  - **Bronze writer**
  - **Silver transforms**
  - **Gold modeling**
  - **Postgres loader**
  - **Run metadata + checks**
  - **Provisioning script** under `scripts/` (or similar)
- Configuration support:
  - `.env` for local
  - environment variables for CI
  - a config file (YAML/TOML/JSON) is okay if helpful
- Tests:
  - Unit tests for API client, schema normalization helpers, and transform logic
  - At least one “smoke test” mode that runs with a tiny pull
- Code quality:
  - Formatting + linting (pick sensible defaults)
  - Type hints where useful

### 3) CI / Scheduling
- `.github/workflows/ingest.yml`
  - Runs daily
  - Manual dispatch supported
  - Runs ingestion + transforms + load
  - Uses secrets for AWS + BEA key + Postgres
  - Emits artifacts/logs for debugging
- `.github/workflows/ci.yml` (NEW)
  - Runs tests + linting on PRs

---

## Best practices checklist (must implement lightly)
Include these practices without over-engineering:

### Observability
- Structured logs (JSON) with a **run_id**
- Clear log levels (INFO/WARN/ERROR)
- Persist run metadata (S3 manifest + Postgres `ingest_runs` table)

### Data quality + validation
- Row count checks / non-null key checks
- Basic schema validation (expected columns/types)
- Uniqueness checks on primary keys
- Fail the run if checks fail

### Alerts (lightweight)
- On GitHub Actions failure: notify via native GitHub notifications
- Optionally publish to SNS / email / Slack webhook if configured

### Data lineage (lightweight)
- Keep a per-run manifest containing:
  - source endpoint
  - request params
  - extraction time
  - hashes/checksums
  - output partitions produced
- Document lineage conventions in `docs/architecture.md` or `docs/operability.md`

### Reproducibility
- Pin dependencies
- Deterministic partitioning
- Idempotent upserts into Postgres

### Documentation
- Everything required to run locally and in CI
- A clear “operating the pipeline” guide

### Security
- No secrets in code
- Least-privilege IAM policies
- S3 encryption + public access block
- RDS security group restricting access (document expected network assumptions)

### Cost controls (lightweight but important)
- S3 lifecycle policies (at least for logs / old raw if desired)
- Small RDS instance sizing guidance
- Avoid unnecessary daily full reloads (use change detection)

---

## Implementation guidance (Key behaviors)
- **Idempotency**: Re-running the same date/params should not create duplicates; it should either no-op or overwrite deterministically.
- **Change detection**: Implement a practical method:
  - Compare hash of latest API payload vs most recent stored hash, OR
  - Maintain “latest known vintage” metadata and only fetch when it changes.
- **Schema stability**: Normalize to stable keys:
  - Geography: two-letter state abbreviation + FIPS where available
  - Time: year (and other grain if present)
  - Category: stable identifiers from BEA metadata
- **Storage formats**:
  - Bronze: raw JSON (or gzipped JSON) + metadata
  - Silver/Gold: Parquet preferred

---

## Output expectation
Create all files with sensible defaults and explanatory content. Make reasonable choices for language and libraries (Python is fine; pick a modern toolchain). Do not over-engineer.

When finished, ensure:
- A new user can follow docs to run locally end-to-end (even if with small sample pulls).
- Provisioning can be run locally for staging/prod after `.env` is configured.
- GitHub Actions workflows are ready to run with provided secrets.
- The code includes clear logging, basic error handling, and minimal data quality checks.

Start by scaffolding the repo, then fill in docs, then implement provisioning, then implement the pipeline with a minimal working example that ingests BEA PCE by State and loads curated results to Postgres.
