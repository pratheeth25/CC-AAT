# DataQA — Full System Explanation

A technical and non-technical breakdown of what DataQA does, how it works, why you'd use it, and where its current limits are.

---

## What is DataQA?

DataQA is a self-hosted, full-stack web application for **automated data quality analysis and cleaning**. You upload a CSV or JSON file, and the system immediately tells you:

- How "good" your data is (scored 0–100, graded A–F)
- What is wrong (missing values, invalid formats, duplicates, outliers, security threats, PII)
- How to fix it (one-click cleaning pipeline, actionable repair suggestions)
- How your data changed over time (version history, drift detection)
- Whether quality is trending up or down (predictive forecasting)

It is aimed at data engineers, analysts, and teams that need to validate and clean data before using it in pipelines, ML models, or reporting.

---

## Architecture Overview

```
Browser (React)
    │
    │  HTTP on port 80
    ▼
nginx (frontend container)
    │  proxies /api/* → port 8000
    ▼
FastAPI (backend container)
    │
    ├─ MongoDB Atlas          (dataset metadata, alerts, versions)
    └─ Disk (Docker volume)   (actual CSV/JSON files)
```

| Layer | Technology |
|---|---|
| Frontend | React 18, Vite 5, Tailwind CSS 3, Recharts, Axios |
| Backend | Python 3.11, FastAPI, Motor (async MongoDB driver) |
| Analysis | Pandas 2, NumPy 2, Scikit-learn 1.7, SciPy 1 |
| Database | MongoDB Atlas (cloud) |
| Hosting | Docker Compose on EC2 (or any Linux host) |
| Reverse proxy | nginx — serves frontend, proxies API |

---

## Features in Detail

### 1. Dataset Upload & Management

- Drag-and-drop CSV or JSON upload with a live progress bar
- File size validation (default 100 MB cap, configurable)
- Extension and parse validation — rejects non-parseable files before writing
- Each dataset is stored with a unique hash-prefixed filename to avoid collisions
- Full CRUD: list, view, delete datasets
- Upload a new version of an existing dataset at any time

**Limitation:** Only CSV and JSON are supported. Excel (`.xlsx`), Parquet, TSV, and other formats require code changes.

---

### 2. Data Profiling

Runs on every dataset version on demand. Returns:

- **Shape** — row and column counts
- **Missing value analysis** — per-column count and percentage, including smart detection of pseudo-nulls (`""`, `N/A`, `?`, `NULL`, `undefined`, `---` and 15+ more patterns pandas does not catch by default)
- **Duplicate analysis** — exact duplicates (identical rows) and logical duplicates (identical on all non-ID columns)
- **Per-column statistics:**
  - Data type (int, float, object, datetime, bool)
  - Unique value count and cardinality ratio
  - Top-N most frequent values
  - Min, max, mean, median, standard deviation, quartiles (numeric)
  - Sample values

Results are cached in-memory per `(dataset_id, version, analysis_type)` triple with an LRU cache. Subsequent requests for the same version are instant.

---

### 3. Quality Scoring Engine

Produces a 0–100 score across **7 dimensions**, each with full explainability (reason, impact level, affected columns, deduction amount):

| Dimension | Max Deduction | What it measures |
|---|---|---|
| Completeness | 35 pts | Missing cells (non-linear: higher % → exponentially larger penalty) |
| Validity | 35 pts | Invalid ages, emails, dates, mixed numeric/string columns |
| Consistency | 20 pts | Mixed date formats in one column, case-inconsistent status values, mixed delimiters |
| Uniqueness | 15 pts | Exact and logical duplicate rows |
| Security | Uncapped (hard cap 30) | XSS, SQL injection, path traversal, command injection, null-byte threats |
| Garbage/Junk | 15 pts | Placeholder strings (`test`, `???`, `#REF!`, sentinel numeric values) |
| Anomalies | 15 pts | Severity-weighted outlier/anomaly count |

**Additional mechanisms:**
- **Compounding penalty** — if 3+ dimensions fail simultaneously, an extra non-linear penalty is added (up to +20 pts deduction) to reflect systemic data corruption
- **Systemic cap** — if 5+ dimensions fail or total deduction ≥ 75, score is hard-capped at 38 (grade F), regardless of individual scores
- **Score calibration** — prevents artificially inflated scores: low-deduction datasets are capped at 95, medium at 88
- **Confidence score** — based on dataset row count (log10 scale); tiny datasets get lower confidence
- **Version delta** — when a previous version score exists, calculates `score_delta` and classifies as `stable / improved / degraded / unstable`
- **Fair score** — a de-duplicated score from root-cause analysis that prevents double-counting overlapping penalties (e.g. date validity and date consistency both deducting for the same column)

**Score bands:**

| Score | Grade | Verdict |
|---|---|---|
| 90–100 | A | Production Ready |
| 75–89 | B | Good |
| 60–74 | C | Needs Cleaning |
| 40–59 | D | Poor |
| < 40 | F | Unusable |

---

### 4. Anomaly Detection

Runs multiple methods per column and classifies each anomaly with a severity:

| Method | Columns | Severity |
|---|---|---|
| IQR (1.5× fence) | Numeric | Medium |
| Z-score (> 3σ) | Numeric | Medium |
| Isolation Forest (5% contamination) | Numeric ≥ 10 rows | Medium |
| Impossible age | age/years/edad | High |
| Sentinel numeric values (-1, 0, 999, 9999) | Numeric | Medium |
| Date validation (unparseable, impossible year, future dates) | Date columns | High |
| Categorical rarity (< 1% of mode frequency) | Low-cardinality text | Low |
| Junk/placeholder string patterns | Any text | Low–High |
| Email validation + placeholder emails | Email columns | High |
| Text length anomaly (> 100 chars in short-text fields) | Name/status/city | Low |

**Skips** high-cardinality identity columns (name, email, UUID, address) for categorical rarity — avoids false positives.

**Limitation:** Isolation Forest requires ≥ 10 non-null observations. Small datasets get fewer detection methods.

---

### 5. Smart Cleaning Pipeline

A 9-step pipeline that runs in order, recalculating the real quality score after each step:

| Step | Always runs? | What it does |
|---|---|---|
| 0a: Junk normalise | Yes | Replaces 18+ null-synonym strings with `NaN` |
| 0b: Fix mixed types | Yes | Detects columns that are ≥ 60% parseable as numbers but stored as `object` (e.g. because "twenty" or "abc" snuck in). Converts valid numbers, sets garbage strings to `NaN`, coerces column to `float64` |
| 0c: Fix domain values | Yes | Nulls out values that violate domain rules (age < 0 or > 120, score > 100, negative prices, etc.) |
| 0d: Re-impute numeric | Yes | After 0b + 0c, fills newly-created `NaN`s with mean or median (auto-selected by skew) |
| 1: Fill numeric | User opt-in | Fill remaining missing numerics (mean/median) |
| 2: Fill categorical | User opt-in | Fill missing categoricals with mode |
| 3: Remove duplicates | User opt-in | Exact row de-duplication |
| 4: Standardise dates | User opt-in | Parses 9 date formats → ISO 8601; drops impossible dates |
| 5: Fix emails | User opt-in | Sets invalid email strings to `NaN` |
| 6: Country normalise | User opt-in | Canonical names: USA → United States, UK → United Kingdom, etc. |
| 7: Text normalise | User opt-in | Whitespace trimming + optional lower/upper/title case |
| 8: Remove outliers | User opt-in | IQR 1.5× fence — removes entire rows containing outliers |

**Score > 80 gate:** If the current dataset score is already above 80, the cleaning page shows a "Already High Quality" banner and does not offer or run cleaning. This prevents unnecessary versioning of already-clean data.

Every cleaning run saves a **new dataset version** and stores a per-step repair report with `score_before`, `score_after`, `rows_affected`, and `confidence` (high/medium/low).

---

### 6. Repair Suggestions

Before cleaning, the system generates a prioritised list of human-readable repair suggestions:

- How many logical duplicates exist (excluding ID columns)
- Which columns contain garbage/placeholder values
- Which columns have invalid text in a numeric context
- Which columns have domain violations (out-of-range values)
- Which columns have missing values and what fill strategy is recommended
- Which emails are invalid
- Which date columns are non-ISO or contain unparseable values
- Which country columns contain synonyms
- Which columns have ≥ 50% missing (high-risk flag)

Each suggestion includes `column`, `issue`, `suggestion`, `affected_count`, and `priority` (high/medium/low).

---

### 7. Data Drift Detection

Compares any two versions of a dataset column-by-column using statistical tests:

| Column type | Test | Threshold |
|---|---|---|
| Numeric | Kolmogorov-Smirnov two-sample test | p < 0.05 |
| Categorical | Chi-squared frequency test | p < 0.05 |

For numeric drifted columns, also reports mean shift and distribution summary (min/max/mean changes). Returns a list of drifted columns and a list of stable columns, with p-values and severity.

**Limitation:** Requires both versions to share the same column name. Newly added or renamed columns are skipped.

---

### 8. PII Detection

Scans every column with regex + Luhn validation for:

| PII Type | Detection method |
|---|---|
| Email addresses | Regex |
| Phone numbers | Regex (international, US, Indian formats) |
| Credit card numbers | Regex + Luhn algorithm checksum |
| Aadhaar (Indian ID) | Regex (12-digit) |
| Passport numbers | Regex |
| IPv4 addresses | Regex |
| US SSN | Regex |

Column name hints boost confidence (a column named `email` with email-like values gets `high` confidence; a column named `notes` with the same values gets `medium`).

Returns risk level (`high / medium / low / none`), per-column findings with sample matches, and feeds PII deductions into the quality score.

---

### 9. Security Scanning

Scans every text cell for injection payloads:

| Threat | Patterns detected | Deduction |
|---|---|---|
| XSS | `<script>`, `javascript:`, `on*=` handlers | 25 pts/occurrence |
| SQL injection | SELECT/DROP/UNION/ALTER/EXEC keywords, `--` comments | 25 pts/occurrence |
| Path traversal | `../` and `..\` | 15 pts/occurrence |
| Command injection | `;`, `&`, `|`, `` ` `` followed by shell commands (ls, cat, rm, curl, …) | 20 pts/occurrence |
| Null byte injection | `\x00` | 10 pts/occurrence |

If any threats are found, the quality score is hard-capped at 30 regardless of other dimensions.

---

### 10. Predictive Quality Forecasting

Uses linear regression over historical version scores to predict future quality trend:

- **Trend** — `declining`, `improving`, or `stable` (based on regression slope)
- **Predicted scores** for the next 3 versions
- **SLA threshold** (default 70, configurable per request)
- **Breach risk** — `high / medium / low / none`
- **Estimated breach version** — the version number at which the score is predicted to fall below the SLA threshold

**Limitation:** Requires at least 2 versions for trend calculation. Single-version datasets return `"stable"` with no prediction.

---

### 11. Root-Cause Analysis

Sits above the quality scorer. De-duplicates penalties that share the same root cause across different dimensions (e.g. "date column has invalid values" deducting from both Validity and Consistency is collapsed into one penalty). Produces:

- Per-column root-cause summaries with confidence-annotated detection results
- A `fair_score` that avoids double-counting
- Issue groups with their combined impact

---

### 12. Alerts System

Auto-generates alerts when analysis is run:

| Trigger | Severity |
|---|---|
| Column missing ≥ 20% values | High |
| Column missing ≥ 50% values | Critical |
| Anomalies detected | High |
| Quality score < 60 | High |
| Data drift detected in any column | High |

Alerts are stored in MongoDB with `is_read` state. They can be marked as read individually or in bulk. The global alerts page filters orphaned alerts — alerts for deleted datasets are automatically excluded.

---

### 13. Versioning

- Every cleaning run creates a new version
- Versions are stored as separate files on disk
- SHA-256 checksum computed per version
- Version restore: roll back to any previous version as the current active version
- Version comparison: compare any two versions for structural and data differences
- Version deletion
- Version download via signed URL (HMAC-SHA256, configurable TTL)

---

### 14. Report Generation

Three output formats on demand:

1. **Structured JSON** — full machine-readable report aggregating all analysis outputs
2. **Human-readable text** — plain English narrative of findings and recommendations
3. **Executive summary** — 3–5 non-technical sentences for management/stakeholder sharing

---

### 15. Caching

Two-tier analysis cache:
- **Primary:** Redis (when `REDIS_URL` is configured in `.env`)
- **Fallback:** In-memory LRU cache (thread-safe, configurable size via `ANALYSIS_CACHE_SIZE`)

Cache keys are `(dataset_id, version, analysis_type)`. The cache is automatically invalidated when a dataset version is created, cleaned, restored, or deleted.

---

### 16. Rate Limiting

In-process sliding-window rate limiter (no Redis required). Configurable via `RATE_LIMIT=100/minute` in `.env`. Returns HTTP 429 when exceeded. Health-check and docs endpoints are always exempt.

---

## Why Use DataQA?

| Use case | How DataQA helps |
|---|---|
| Pre-pipeline data validation | Upload a CSV, get an instant score and breakdown before pushing to a database or ML model |
| Data cleaning without code | One-click pipeline fixes the most common issues and saves a clean version |
| Audit trail | Every change creates a versioned file with a full change log |
| Regulatory / compliance checks | PII detection surfaces personal data columns; security scan catches injection payloads |
| Drift monitoring | Compare production data snapshots over time and get statistical confirmation of distribution shift |
| Team visibility | Alerts and dashboard give a shared, centralised view of data health across all uploaded datasets |
| Self-hosted | No data leaves your infrastructure (except MongoDB Atlas connection). No SaaS lock-in. |

---

## Advantages

- **Zero vendor lock-in** — fully self-hosted with Docker Compose, one command to deploy
- **Explainable scoring** — every point deduction has a reason, impact level, and affected columns attached
- **Non-destructive versioning** — original data is never modified; every cleaning run creates a new file
- **Async-ready** — FastAPI + Motor are fully async; the backend handles concurrent requests efficiently
- **Smart null detection** — catches 18+ pseudo-null patterns (`N/A`, `?`, `---`, `undefined`, etc.) that `pd.read_csv` silently leaves as strings
- **Mixed-type repair** — automatically detects columns that are numerically intended but contain stray strings ("twenty", "abc") and coerces them correctly
- **Domain-aware cleaning** — understands that `age=150` and `score=200` are invalid, not just `NaN`
- **Security-aware scoring** — XSS and SQL injection in data cells tank the quality score, not just flag a warning
- **No schema required** — works on any CSV/JSON without prior configuration or schema definition
- **LRU cache** — repeated analysis requests on the same version are served instantly from memory

---

## Disadvantages and Limitations

### File format
- **Only CSV and JSON are supported.** Excel (`.xlsx`, `.xls`), Parquet, Avro, ORC, TSV, and database exports require manual conversion first.
- Large files close to the 100 MB limit may be slow to analyse (profiling, anomaly detection, security scan all load the full file into memory).

### Cleaning accuracy
- **Column-name dependent.** Many features rely on recognising column names (e.g. `age`, `email`, `date`, `country`, `score`). Columns with unusual names (e.g. `col_3`, `field_age_years`) may not trigger domain rules, date parsing, or email validation.
- **Categorical mode fill can be wrong** — if data is bimodal or the most common value is itself suspicious, mode imputation introduces bias.
- **Outlier removal deletes entire rows** — IQR outlier removal drops the whole row, not just the outlier cell.
- **Country normalisation is English-only** — the synonym map covers English variants only (USA, UK, India, etc.). Non-English country names are not normalised.

### Scoring
- **Score calibration caps** — a dataset with very few issues is capped at 95 (not 100). This is intentional (no real-world dataset is perfect) but may feel punitive for genuinely clean data.
- **Score is not comparable across datasets** — a score of 75 in one dataset does not mean the same thing as 75 in another. It reflects relative quality within that dataset.
- **Confidence is size-dependent** — a 10-row dataset gets a low confidence score regardless of its actual quality.

### PII and security detection
- **Regex-based** — PII detection uses regex patterns, not NLP or machine learning. It will miss PII in free-text fields (e.g. "Please call John on 9876543210" in a notes column).
- **False positives on credit cards** — Luhn validation reduces but does not eliminate false positives on numeric sequences that happen to pass the checksum.
- **Security scanner does not sanitise** — it detects and deducts from score, but does not remove or quarantine the flagged cells.

### Versioning
- **Versions accumulate on disk** — files are never auto-deleted. Long-running deployments with many cleaning runs will consume disk space.
- **No column-rename tracking** — if columns are renamed between versions, drift detection and change summary treat them as added/removed rather than renamed.

### Infrastructure
- **Single node only** — Docker Compose runs one backend instance. There is no horizontal scaling, load balancing, or worker queue for heavy analysis jobs. Very large files on a small EC2 instance (t2.micro) may cause OOM.
- **In-memory cache is per-process** — if the backend restarts, all cached analysis results are lost and must be recomputed.
- **No authentication** — there is no login, no user accounts, no access control. Anyone who can reach port 80 can upload, read, and delete all datasets. Not suitable for multi-tenant or public-facing deployment without adding an auth layer.
- **MongoDB Atlas required** — the app is configured for Atlas (cloud MongoDB). Running with a local MongoDB container requires editing the `MONGO_URI` in `backend/.env` and adding a `mongo` service to `docker-compose.yml`.

### Analysis depth
- **No schema validation** — there is no way to define expected column types or value ranges upfront (beyond the hardcoded domain rules). The system infers everything from the data.
- **Prediction requires 2+ versions** — quality trend forecasting is unavailable for first-upload datasets.
- **No cross-dataset analysis** — all analysis is per-dataset. There is no way to compare data quality across multiple datasets or detect referential integrity issues between related datasets.

---

## Default Configuration Reference

| Variable | Default | Purpose |
|---|---|---|
| `MONGO_URI` | `mongodb://localhost:27017` | MongoDB connection string |
| `MONGO_DB` | `data_quality` | Database name |
| `UPLOAD_DIR` | `./data/uploads` (local) / `/app/data/uploads` (Docker) | Where uploaded files are stored |
| `MAX_UPLOAD_SIZE_MB` | `100` | Maximum upload file size |
| `ALLOWED_ORIGINS` | `http://localhost:5173` | CORS origin — set to your EC2 IP for production |
| `ANALYSIS_CACHE_SIZE` | `128` | Maximum number of analysis results to keep in LRU cache |
| `MAX_PROFILE_ROWS` | `50000` | Rows sampled for profiling on very large files (0 = no limit) |
| `RATE_LIMIT` | `100/minute` | Per-IP request rate limit |
| `RATE_LIMIT_ENABLED` | `true` | Toggle rate limiting on/off |
| `ASYNC_ANALYSIS_ENABLED` | `true` | Toggle async analysis mode |
| `SIGNED_URLS_ENABLED` | `true` | Enable HMAC-signed download URLs |
| `SIGNED_URL_SECRET` | *(change this)* | Secret key for signing download tokens |
| `SIGNED_URL_TTL_SECONDS` | `3600` | Download link expiry time (1 hour) |

---

## API Reference

| Method | Path | Description |
|---|---|---|
| `POST` | `/upload` | Upload a new CSV/JSON dataset |
| `GET` | `/datasets` | List all datasets |
| `GET` | `/dataset/{id}` | Get dataset metadata and version list |
| `DELETE` | `/dataset/{id}` | Delete dataset and all its versions |
| `POST` | `/dataset/{id}/upload-version` | Upload a new version of an existing dataset |
| `GET` | `/dataset/{id}/profile` | Column profiling report |
| `GET` | `/dataset/{id}/quality` | Quality score with full breakdown |
| `GET` | `/dataset/{id}/anomalies` | Anomaly detection results |
| `GET` | `/dataset/{id}/repairs` | Smart repair suggestions |
| `POST` | `/dataset/{id}/clean` | Run the cleaning pipeline |
| `GET` | `/dataset/{id}/drift` | Drift detection between two versions |
| `GET` | `/dataset/{id}/security-scan` | Security threat scan |
| `GET` | `/dataset/{id}/pii` | PII detection |
| `GET` | `/dataset/{id}/prediction` | Quality score trend forecast |
| `GET` | `/dataset/{id}/report` | Full quality report (JSON + text + executive summary) |
| `GET` | `/dataset/{id}/delimiter-check` | Delimiter detection (CSV only) |
| `GET` | `/alerts` | List all alerts (orphaned alerts auto-filtered) |
| `PATCH` | `/alerts/{id}/read` | Mark an alert as read |
| `GET` | `/health` | Backend health check |
| `GET` | `/docs` | Interactive Swagger UI |

---

## What DataQA is NOT

- Not a database or data warehouse
- Not an ETL pipeline runner
- Not a data transformation tool (it cleans within its own defined operations — you cannot write custom SQL or Python transforms)
- Not a real-time streaming data quality monitor
- Not a multi-user SaaS platform (no auth, no tenancy)
- Not a schema registry or data catalog
