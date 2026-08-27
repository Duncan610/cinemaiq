# CinemaIQ

> *An end-to-end data engineering pipeline that asks: what actually makes a movie succeed, and can data tell us before opening weekend?*

## Project Report

**[View the full CinemaIQ report (PDF)](cinemaiq.pdf)**

---

## The Story

Every Friday, hundreds of millions of dollars ride on a single question nobody can reliably answer: *Will this movie make money?*

Studios spend $200–300 million producing a blockbuster, then another $100–150 million marketing it, and still walk into opening weekend essentially guessing. Some films with massive budgets, A-list casts, and saturating advertising campaigns open to near-empty theatres. Others, made for a fraction of the cost with no stars, become cultural moments that run for months.

The conventional wisdom big budget, big star, big opening breaks down constantly. *Everything Everywhere All at Once* was made for $14 million and became one of the highest-grossing A24 films ever. *Indiana Jones and the Dial of Destiny* had $295 million, Harrison Ford, and the nostalgia of a 40-year franchise, and lost the studio an estimated $130 million. *Oppenheimer*, a 3-hour R-rated film about nuclear physics with no traditional "action hero," grossed $952 million worldwide. By conventional logic, none of these outcomes make sense.

What if the signals were there all along, just scattered across the internet in places nobody was connecting?

Google Trends captures when public curiosity about a film starts spiking, weeks before release. News coverage volume tells you whether mainstream media is paying attention or ignoring a film entirely. The gap between what critics score a movie on Rotten Tomatoes versus what audiences rate it on IMDb hints at whether word-of-mouth will sustain it past the second weekend. Box office numbers tell yet another story once the dust settles.

None of these signals alone is enough. But assembled, cleaned, joined, and analyzed as a unified dataset, they start to tell a story.

That is what **CinemaIQ** is built to do.

---

## What CinemaIQ Does

CinemaIQ is a data engineering pipeline that:

1. **Ingests** data from 6 sources: TMDB, OMDB, NewsAPI, Box Office Mojo, Google Trends, and the full IMDb public dataset
2. **Stores** everything raw in Snowflake, preserving an audit trail of every load
3. **Transforms** the raw data through 3 dbt layers cleaning each source individually, joining across sources, and building final analytical tables
4. **Serves** the final data to a 4-page Looker Studio dashboard

The pipeline is orchestrated by Apache Airflow running in Docker and built using the same medallion-architecture pattern (Bronze → Silver → Gold).

---

## Architecture

```mermaid
flowchart TD
    %% ── DATA SOURCES ──────────────────────────────────────────
    subgraph SOURCES["Data Sources"]
        direction LR
        TMDB["TMDB API<br/>Metadata · Popularity · Ratings"]
        OMDB["OMDB API<br/>IMDb · Rotten Tomatoes · Metacritic"]
        NEWS["NewsAPI<br/>Media Articles"]
        BOM["Box Office Mojo<br/>Revenue - scraped"]
        GT["Google Trends<br/>Search Interest - pytrends"]
        IMDB["IMDb Public Datasets<br/>Full catalogue TSV"]
    end

    %% ── INGESTION ─────────────────────────────────────────────
    subgraph INGEST["Python Ingestion Layer"]
        direction LR
        B1["tmdb.py"]
        B2["omdb.py"]
        B3["news_api.py"]
        B4["mojo.py"]
        B5["trends.py"]
        B6["imdb.py"]
        B7["loader.py<br/>shared Snowflake writer"]
    end

    %% ── ORCHESTRATION ─────────────────────────────────────────
    subgraph AIRFLOW["Orchestration — Apache Airflow in Docker"]
        DAG["cinemiq_bronze_ingestion DAG<br/>SequentialExecutor"]
    end

    %% ── SNOWFLAKE LAYERS ──────────────────────────────────────
    subgraph SNOW["Snowflake — CINEMAIQ Database"]
        direction TB

        subgraph RAW["RAW Schema — Bronze"]
            direction LR
            R1["RAW_TMDB_MOVIES"]
            R2["RAW_OMDB_RATINGS"]
            R3["RAW_NEWS_ARTICLES"]
            R4["RAW_BOX_OFFICE"]
            R5["RAW_GOOGLE_TRENDS"]
            R6["RAW_IMDB_BASICS<br/>RAW_IMDB_RATINGS"]
            R7["PIPELINE_AUDIT"]
        end

        subgraph STG["STAGING Schema — Silver (dbt)"]
            direction LR
            S1["stg_tmdb_movies"]
            S2["stg_omdb_ratings"]
            S3["stg_news_articles"]
            S4["stg_box_office"]
            S5["stg_google_trends"]
            S6["stg_imdb_movies"]
        end

        subgraph INT["INTERMEDIATE Schema (dbt)"]
            direction LR
            I1["int_movies_unified<br/>fuzzy title+year join"]
            I2["int_movies_genres<br/>exploded"]
            I3["int_movie_cast<br/>exploded"]
            I4["int_news_coverage<br/>aggregated"]
            I5["int_trends_windowed<br/>aggregated"]
        end

        subgraph MARTS["MARTS Schema — Gold (dbt)"]
            direction LR
            M1["mart_success_drivers"]
            M2["mart_genre_trends"]
            M3["mart_hype_vs_performance"]
            M4["mart_prerelease_signals"]
        end

        RAW -->|"dbt run --select staging"| STG
        STG -->|"dbt run --select intermediate"| INT
        INT -->|"dbt run --select marts"| MARTS
    end

    %% ── PRESENTATION ──────────────────────────────────────────
    subgraph VIZ["Looker Studio — 4-Page Dashboard"]
        direction LR
        D1["Success Drivers"]
        D2["Genre Trends"]
        D3["Hype vs Performance"]
        D4["Pre-release Signals"]
    end

    %% ── CONNECTIONS ───────────────────────────────────────────
    TMDB --> B1
    OMDB --> B2
    NEWS --> B3
    BOM --> B4
    GT --> B5
    IMDB --> B6
    B1 & B2 & B3 & B4 & B5 & B6 --> B7
    B7 --> AIRFLOW
    AIRFLOW --> RAW
    RAW --> R7

    MARTS -->|"Snowflake connector"| VIZ

    %% ── STYLES ────────────────────────────────────────────────
    classDef source  fill:#1a1a2e,stroke:#e94560,color:#fff
    classDef ingest  fill:#2b2b40,stroke:#f0a500,color:#fff
    classDef airflow fill:#17153b,stroke:#c084fc,color:#fff
    classDef bronze  fill:#431407,stroke:#f97316,color:#fff
    classDef silver  fill:#1e3a5f,stroke:#60a5fa,color:#fff
    classDef inter   fill:#1a2e1a,stroke:#4ade80,color:#fff
    classDef gold    fill:#3b2f00,stroke:#fbbf24,color:#fff
    classDef viz     fill:#1a1a2e,stroke:#f472b6,color:#fff

    class TMDB,OMDB,NEWS,BOM,GT,IMDB source
    class B1,B2,B3,B4,B5,B6,B7 ingest
    class DAG airflow
    class R1,R2,R3,R4,R5,R6,R7 bronze
    class S1,S2,S3,S4,S5,S6 silver
    class I1,I2,I3,I4,I5 inter
    class M1,M2,M3,M4 gold
    class D1,D2,D3,D4 viz
```

---

## Data Sources

| Source | What it provides | Key signal |
|---|---|---|
| **TMDB API** | Movie metadata, budget, revenue, genres, cast, popularity | Core movie profile |
| **OMDB API** | IMDb + Rotten Tomatoes + Metacritic in one call | `critic_audience_gap`  divergence between critics and audiences |
| **NewsAPI** | Media articles mentioning each movie | Pre-release media buzz volume and sentiment |
| **Box Office Mojo** | Yearly worldwide/domestic revenue charts (scraped) | Ground-truth revenue outcome |
| **Google Trends** | Search interest over time (via `pytrends`) | Public awareness before release |
| **IMDb Public Datasets** | Ratings and vote counts for the full IMDb catalogue | Independent audience rating, historical context |

---

## Data Volume & Coverage

| Layer | Table / Model | Approx. Rows | Notes |
|---|---|---|---|
| Bronze | `RAW_TMDB_MOVIES` | ~600 | 2019–2024, discover endpoint |
| Bronze | `RAW_IMDB_BASICS` | 374,972 | filtered from ~12.4M rows via chunked read |
| Bronze | `RAW_IMDB_RATINGS` | 1,666,284 | full IMDb ratings dataset |
| Bronze | `RAW_OMDB_RATINGS` | 528 | expanded from an initial 12-movie test set |
| Bronze | `RAW_BOX_OFFICE` | ~400 | scraped, 2019–2024 |
| Bronze | `RAW_NEWS_ARTICLES` | 797 | top 80 movies by revenue, 30-day window |
| Bronze | `RAW_GOOGLE_TRENDS` | 2,320 | top 80 movies by revenue, 3-month + 12-month windows |
| Gold | `mart_success_drivers` | 600 | one row per movie |
| Gold | `mart_genre_trends` | ~180 | one row per (genre, year) |
| Gold | `mart_hype_vs_performance` | 600 | buzz fields real for top 80 movies only |
| Gold | `mart_prerelease_signals` | 600 | filtered to movies with revenue data |

**Final match rates** (`int_movies_unified`, 600 TMDB movies as the base):
- Matched to IMDb: 528 (88%)
- Matched to OMDB: 528 (88%)
- Matched to Box Office Mojo: 475 (79%)

---

## Constraints

- **Hardware:** built entirely on a personal laptop with 3.8GB RAM; every infrastructure decision (Airflow executor choice, worker counts, chunked file processing) was shaped by this limit.
- **Snowflake:** 30-day free trial, $400 starting credit.
- **API limits:** NewsAPI free tier caps at 100 requests/day; Google Trends (via `pytrends`, an unofficial scraper) actively rate-limits and can trigger CAPTCHA blocks under volume; OMDB free tier caps at 1,000 requests/day.
- **Box Office Mojo:** no official API; data obtained via HTML scraping, which is fragile to the site's own layout changes.

---

## Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Orchestration | Apache Airflow 2.10 (Docker) | Visibility (UI, logs, retries) over a plain cron job; `SequentialExecutor` chosen specifically for the 3.8GB RAM constraint |
| Storage | Snowflake | Cloud warehouse; medallion architecture (RAW → STAGING → INTERMEDIATE → MARTS) maps directly onto schemas |
| Transformation | dbt Core | Version-controlled SQL transformations with a built-in testing framework |
| Testing | dbt_utils | `accepted_range`, generic testing macros |
| Containerisation | Docker + Docker Compose | Reproducible local Airflow environment |
| Language | Python 3.11/3.12 | Ingestion scripts — `requests`, `BeautifulSoup`, `pytrends`, `pandas` |
| Loading | Snowflake `write_pandas` | Auto-creates Bronze tables; everything lands as VARCHAR by design |
| Dashboards | Looker Studio | Free, connects natively to Snowflake, no extra infra cost |
| Logging | loguru | Structured logs across every ingestion script |

---

## Two Virtual Environments

Airflow and dbt have conflicting dependency requirements and are kept fully separate:

```
airflow_env / venv   →  running ingestion scripts, loading data, Docker/Airflow
dbt_env              →  dbt run, dbt test, dbt debug
```

**Rule of thumb:** one terminal per environment. Always `export $(grep -v '^#' .env | xargs)` after activating `dbt_env` in a fresh terminal dbt's `profiles.yml` reads Snowflake credentials from environment variables, which don't persist across terminal sessions.

---

## Pipeline Walkthrough: Raw → Marts

**1. Ingestion (Python → Snowflake RAW)**
Six independent scripts (`tmdb.py`, `omdb.py`, `news_api.py`, `mojo.py`, `trends.py`, `imdb.py`) each pull from one source and hand their DataFrame to a shared `loader.py`. Every column lands as `VARCHAR` no type casting at ingest. This is deliberate: if a source changes its data format unexpectedly, the load still succeeds; type enforcement happens downstream, under version control, where a bad cast fails loudly in a dbt test instead of crashing a live load. Every load also writes a row to `PIPELINE_AUDIT` a lightweight lineage record of what loaded, from where, and when.

**2. Staging (dbt, RAW → STAGING)**
One model per source. Casts every VARCHAR to its real type (dates, integers, floats), renames columns to snake_case, and filters unusable rows. No cross-source joins happen here each staging model only knows about its own source. `stg_imdb_movies` is the one exception, joining IMDb's basics and ratings files together, since they come from the same source and share the same grain.

**3. Intermediate (dbt, STAGING → INTERMEDIATE)**
This is where sources start talking to each other. `int_movies_unified` is the spine it joins TMDB, IMDb, OMDB, and Box Office Mojo on normalized title + release year, since none of these sources share a common ID. Two other kinds of transforms happen here: exploding TMDB's pipe-delimited genre/cast strings into proper one-row-per-value tables (`int_movies_genres`, `int_movie_cast`), and aggregating NewsAPI/Google Trends from many-rows-per-movie down to one summary row per movie (`int_news_coverage`, `int_trends_windowed`).

**4. Marts (dbt, INTERMEDIATE → MARTS)**
Four analytical tables, each answering a specific question, materialized as physical tables (not views) for fast BI reads.

**5. Visualization (Looker Studio)**
Four dashboard pages, one per mart, connected directly to Snowflake.

---

## Engineering Decisions

- **Bronze lands everything as VARCHAR.** Type safety is enforced in dbt Staging, not at ingest keeps the ingestion layer resilient to upstream format surprises.
- **Fuzzy title+year join, with explicit match-quality flags.** Since TMDB, IMDb/OMDB, and Box Office Mojo share no common ID, `int_movies_unified` matches on normalized title + year and exposes `matched_imdb` / `matched_omdb` / `matched_box_office` booleans, so downstream analysis can see exactly how complete each row's data really is instead of silently trusting a fuzzy match.
- **Deduplication before joining, not after.** Common titles ("Beast," "Smile," "Rocketman") exist as multiple distinct films sharing a title and year. Each source is deduplicated (ranked by vote count / box office, keeping the strongest match) *before* the join, preventing row fan-out.
- **`SequentialExecutor` for Airflow, not `LocalExecutor`.** Chosen specifically for the 3.8GB RAM constraint trades task parallelism for a dramatically smaller memory footprint.
- **Chunked pandas reads for IMDb's basics file** (`chunksize=200,000`, filtered columns via `usecols`) — the full file is ~12.4M rows and would otherwise exhaust available RAM.
- **A dbt macro override (`generate_schema_name`)** ensures the `prod` target produces clean schema names (`MARTS`, not `STAGING_marts`), while `dev` keeps a safer prefixed sandbox (`DEV_MARTS`, `DEV_STAGING`, etc.).

---

## What Broke, and How I Fixed It

Every one of these was caught by actually looking at the data not by a test passing or failing.

**1. Reddit API access never arrived in time.**
Pivoted the "audience sentiment" signal to OMDB (critic vs. audience score divergence) and the "media buzz" signal to NewsAPI arguably richer signals than Reddit would have given.

**2. OMDB coverage gap — silent, not loud.**
The original OMDB load only ever fetched data for a 12-movie hardcoded test list, while TMDB pulled ~600. Nothing errored; the join just quietly matched almost nothing (12/600, ~2%). Rewrote the fetch to pull OMDB by IMDb ID for all 528 movies already matched to IMDb — coverage went to 100% of matched movies.

**3. Fan-out bug in the core join.**
`int_movies_unified`'s `tmdb_id` briefly failed a uniqueness test 9 movies were duplicated. Root cause: common titles matching multiple rows on the IMDb/OMDB/Box Office side. Fixed by deduplicating each source (via `QUALIFY ROW_NUMBER()`) before joining.

**4. Docker/Airflow infrastructure the unglamorous half of the project.**
Docker Desktop leftover credential-store references, UID/permission mismatches writing to mounted volumes, a pinned Airflow Snowflake provider forcing an incompatible old connector version, and a shared-package volume needed across Airflow's three containers so a package installed once was visible everywhere. None of this is "data engineering" in the usual sense, but it was the majority of the real debugging time.

**5. Box Office Mojo's opening weekend column doesn't exist.**
`mart_prerelease_signals` originally targeted opening weekend specifically. Built a scraper using header-based column detection (reading the actual `<th>` labels instead of assuming fixed positions) specifically to avoid another silent misalignment bug — and it correctly reported back that the page has no "Opening" column at all. Rather than build a much larger per-weekend scraper for a nice-to-have signal, pivoted the mart to target total revenue instead.

**6. Hype score thresholds were miscalibrated against data that didn't exist yet.**
`mart_hype_vs_performance` classified movies as high/moderate/low hype using thresholds picked before real buzz data existed. Once NewsAPI (797 articles) and Google Trends (2,320 rows) were expanded to the top 80 movies by revenue, the real score distribution turned out to max out at 50 while the "high hype" cutoff was set at 60. Nothing could ever qualify. Recalibrated against the actual observed range; three real hype categories emerged immediately.

**7. Google Trends rate-limiting mid-run.**
Hit repeated `429` errors escalating to an actual CAPTCHA wall partway through the top-80 buzz fetch. `pytrends`'s built-in retry/backoff recovered enough of the run to still land 2,320 rows, though coverage remains uneven across movies — documented as a known limitation rather than chased further.

---

## Visualization — The Looker Studio Dashboard

*(Full dashboard exported as [`cinemiq.pdf`](./cinemiq.pdf) in this repo.)*

**Page 1 — Success Drivers**
Scorecards for total movies, average ROI, and average critic/audience gap; a budget-vs-revenue scatter colored by critic/audience agreement category; average revenue by performance tier; and a table of the top 10 movies by ROI. This page answers: *what combination of factors shows up alongside a successful movie?*

**Page 2 — Genre Trends**
A multi-line chart tracking average revenue per genre across years, a bar chart of movie count by genre, and a genre × year table with average ROI and critic/audience gap. This page answers: *which genres are winning or fading?*

**Page 3 — Hype vs. Performance**
A scatter of hype score vs. revenue colored by outcome category, a donut chart of the three real outcome categories that emerged from the data (as-expected, quiet success, hype matched performance), and a table of the highest-buzz movies. This page answers: *did the buzz match the box office?*

**Page 4 — Pre-release Signals**
A scatter of pre-release news coverage volume vs. revenue, average revenue by coverage level, and a table of movies with real pre-release signal data. This page answers: *can early buzz predict box office outcome?* — with an explicit note that signal data only covers the top 80 movies by revenue, and that Google Trends coverage is partial due to rate-limiting encountered during collection.

---

## Build Evidence

### Environment Setup
![dbt virtual environment](images/dbtvenv.png)
![Airflow virtual environment](images/airflowvenv.png)
![dbt installed and version confirmed](images/dbtinstalledandversion.png)
![dbt debug — all checks passed](images/dbtdebugscreenshot.png)
![API and Snowflake connection tests](images/testingconnections.png)

### Bronze Layer (RAW)
![RAW schema tables](images/rawtables.png)
![All 7 RAW tables confirmed in Snowflake](images/seventablesdisplayedonsnowflake.png)

### Silver Layer (Staging)
![dbt run --select staging — success](images/dbtrunselectstagingsuccessful.png)
![dbt test --select staging — success](images/dbtselectstagingsuccessful.png)
![General dbt run output](images/dbtrun.png)
![General dbt test output](images/dbttest.png)
![Staging views in Snowflake](images/stagingviews.png)
![Result of staging views created](images/resultofthecreatedstgviewsonsnowflake.png)

### Intermediate Layer
![dbt run --select intermediate — success](images/dbtrunselectintermediatesuccess.png)
![dbt run/test fix for int_movies_genres](images/dbttestselectanddbtrunselectintmoviesgenresintermediate.png)
![Intermediate views in Snowflake](images/intermediateviews.png)
![OMDB match rate validation query](images/matchratevalidationomdb.png)

### Gold Layer (Marts)
![dbt run --select marts — success](images/dbtrunselectmarts.png)
![dbt test --select marts — success](images/dbttestselectmarts.png)
![Mart tables confirmed in Snowflake](images/martstables.png)
![Signature insight — critic/audience gap query](images/signatureinsight.png)
![Genre trends over time — Action example](images/genretrendsovertimeaction.png)
![Hype vs outcome distribution](images/hypevsoutcome.png)

---

## Project Structure

```
cinemaiq/
│
├── ingestion/                   # Python scripts — one per data source
│   ├── tmdb.py                  # TMDB movie metadata + credits
│   ├── omdb.py                  # Critic/audience scores + gap calculation
│   ├── news_api.py              # Movie news articles + headline sentiment
│   ├── mojo.py                  # Box Office Mojo scraper
│   ├── trends.py                # Google Trends search interest
│   ├── imdb.py                  # IMDb TSV file loader (chunked, run once)
│   └── loader.py                # Shared loader: DataFrame → Snowflake RAW
│
├── dags/
│   └── cinemiq_bronze_dag.py    # Airflow DAG: 5 ingestion tasks + audit
│
├── dbt_cinemaiq/                 # All dbt transformation code
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── profiles.yml             # Copy to ~/.dbt/ — never commit
│   ├── macros/
│   │   └── get_custom_schema.sql
│   └── models/
│       ├── staging/             # Silver: clean + type each source
│       ├── intermediate/        # Joins across sources
│       └── marts/               # Gold: final analytical tables
│
├── config/
│   ├── snowflake_setup.sql      # Run once to create DB, schemas, roles
│   ├── verify_snowflake.sql     # Verification queries per milestone
│   └── test_connections.py      # Pre-flight check: tests all connections
│
├── images/                      # Build evidence screenshots
├── cinemiq.pdf                  # Exported 4-page Looker Studio dashboard
├── docker-compose.yml           # Airflow webserver + scheduler + Postgres
├── requirements_airflow.txt     # Airflow venv dependencies
├── requirements_dbt.txt         # dbt venv dependencies
└── .env.example                 # Credential template (never commit .env)
```

---

## Getting Started

### Prerequisites
- Python 3.11+
- Docker Desktop (running)
- Snowflake account (free trial works)
- API keys: TMDB (free), OMDB (free), NewsAPI (free)

### Setup

**1. Clone the repo**
```bash
git clone https://github.com/Duncan610/cinemaiq.git
cd cinemaiq
```

**2. Create your .env**
```bash
cp .env.example .env
# Fill in your API keys and Snowflake credentials
```

**3. Set up Snowflake**
```sql
-- Run config/snowflake_setup.sql in your Snowflake worksheet as ACCOUNTADMIN
-- Creates the CINEMAIQ database, all schemas, the service account, and warehouses
```

**4. Create the two virtual environments**
```bash
# Airflow/ingestion environment
python -m venv airflow_env
source airflow_env/bin/activate
pip install -r requirements_airflow.txt

# dbt environment (separate terminal)
python -m venv dbt_env
source dbt_env/bin/activate
pip install -r requirements_dbt.txt
```

**5. Test all connections**
```bash
source airflow_env/bin/activate
cd config
python test_connections.py
# All checks must PASS before continuing
```

**6. Load IMDb data — one time**
```bash
# Download title.basics.tsv.gz and title.ratings.tsv.gz from https://datasets.imdbws.com
cd ingestion
python imdb.py --basics /path/to/title.basics.tsv.gz --ratings /path/to/title.ratings.tsv.gz
```

**7. Start Airflow and run the Bronze DAG**
```bash
docker-compose up airflow-init   # first time only
docker-compose up -d
# Open http://localhost:8080  |  login: admin / admin
# Trigger: cinemiq_bronze_ingestion
```

**8. Run dbt — Staging → Intermediate → Marts**
```bash
source dbt_env/bin/activate
cd dbt_cinemaiq
export $(grep -v '^#' ../.env | xargs)
dbt deps
cp profiles.yml ~/.dbt/profiles.yml
dbt debug
dbt run
dbt test
```

**9. Connect Looker Studio**
Point a Snowflake data source at each of the 4 tables in `CINEMAIQ.MARTS` and build/import the dashboard.

---

## Author

**Duncan Otieno** — Data/ Analytics Engineering Portfolio Project
