# YouTube API → Airflow ELT Pipeline (Postgres + Data Quality)

An end-to-end **ELT** project that pulls video-level metrics from the **YouTube Data API**, lands raw data as JSON, loads it into a **Postgres staging schema**, then transforms/upserts into a **core schema**—all orchestrated by **Apache Airflow** running in **Docker**.

> This repo is configured against a sample channel (e.g., *MrBeast*), but you can point it at any YouTube channel by changing the channel identifier used in the extraction step.

---

## Architecture (how the pieces fit together)

- **Airflow (in Docker)** schedules and runs the pipeline on a defined cadence (or manually from the UI).
- The pipeline **extracts** data from the **YouTube Data API** using Python and writes a raw **JSON landing file** to the repo (in `data/`).
- A second Airflow step **loads** that JSON into a **Postgres staging schema** (raw-ish tables with minimal shaping).
- A transformation step then **cleans/standardizes** fields and **upserts** records into a **Postgres core schema** (analytics-ready tables).
- A final step runs **Soda** checks to validate data quality across both **staging** and **core** layers.
- Everything runs locally via **docker compose** (Airflow services + Postgres) for repeatable setup.

---

## What this demonstrates to recruiters (Data Engineering)

- **Airflow orchestration**: multi-step DAG-driven pipeline (extract → load → transform → validate)
- **Containerized stack**: reproducible local environment with **Docker / docker compose**
- **Data modeling**: clear separation of **staging vs core** schemas (raw-ish vs analytics-ready)
- **Incremental loads / upserts**: first run full load, later runs refresh mutable metrics (views/likes/comments)
- **Testing**: **pytest** unit tests + **Soda** data quality checks
- **CI/CD**: **GitHub Actions** workflow to validate changes (build + test + pipeline checks)
- **Config management**: environment-based configuration using Airflow Variables/Connections conventions

---

## Repo layout

```text
.
├── dags/                 # Airflow DAGs + task code
├── data/                 # Raw JSON extracts produced by the pipeline
├── docker/
│   └── postgres/         # Postgres init scripts / docker helpers (if present)
├── include/
│   └── soda/             # Soda checks + configuration
├── images/               # Diagrams/screenshots used by README
├── tests/                # pytest unit tests
├── docker-compose.yaml   # Multi-container stack (Airflow + Postgres, etc.)
├── dockerfile            # Custom Airflow image (installs deps from requirements.txt)
└── requirements.txt      # Python dependencies for DAGs/tests/soda
```

---

## Data Source

The pipeline extracts data from the **YouTube Data API**. By default, it pulls from the popular channel **MrBeast**.

You can replicate this project for *any* YouTube channel by updating the **Channel ID** or **handle** used by the extraction step.

---

## What This Pipeline Does

This project uses **Airflow** for orchestration and runs fully containerized via **Docker Compose**.

### ELT flow

1. **Extract**: Python scripts call the YouTube API and generate raw output.
2. **Load (staging)**: Raw data is loaded into a `staging` schema in a **dockerized PostgreSQL** database.
3. **Transform + Load (core)**: A Python transformation step applies minor cleaning/reshaping and loads results into the `core` schema (same dockerized Postgres instance).

### Full load vs incremental updates

- The **first run** performs an initial **full load**.
- Subsequent runs **upsert** selected fields/columns (incremental refresh).

Once the `core` layer is populated and tests are passing, the data is ready for analysis or downstream use.

---

## Extracted Fields

The pipeline collects the following **7 attributes per video**:

- Video ID
- Video Title
- Upload Date
- Duration
- Video Views
- Likes Count
- Comments Count

---

## Tech Stack

- **Containerization**: Docker, Docker Compose  
- **Orchestration**: Apache Airflow  
- **Storage**: PostgreSQL  
- **Languages**: Python, SQL  
- **Testing**: pytest (unit), Soda Core (data quality)  
- **CI/CD**: GitHub Actions  

---

## Containerization Details (Airflow on Docker)

Airflow is deployed using the official `docker-compose.yaml` from the Airflow docs, with a few project-specific adjustments:

1. **Custom Airflow image**
   - The stack uses an *extended* Airflow image built via a `Dockerfile`.
   - That image is pushed/pulled to/from Docker Hub via a GitHub Actions CI/CD workflow.
   - The CI workflow also runs `docker-compose` to validate the stack.

2. **Airflow Connections & Variables via environment variables**
   - Connections use URI format with this naming convention:
     - `AIRFLOW_CONN_{CONN_ID}`
   - Variables are injected as:
     - `AIRFLOW_VAR_{VARIABLE_NAME}`

3. **Fernet key encryption**
   - A Fernet key is used to encrypt sensitive values stored in connections/variables.

---

## Orchestration (Airflow DAGs)

There are **three DAGs**, executed sequentially. You can view and trigger them in the Airflow UI:

- Airflow UI: `http://localhost:8080`

### DAG list

- **produce_json**  
  Produces a JSON file containing raw YouTube API data.

- **update_db**  
  Processes the JSON and loads data into both `staging` and `core` schemas.

- **data_quality**  
  Runs data quality checks against both layers in the database.

---

## Data Storage / Access

To explore the loaded YouTube data you can:

- Exec into the Postgres container and use `psql`, or
- Connect with a database client like **DBeaver** and run SQL queries directly.

---

## Testing

This project includes two layers of testing:

- **Unit tests** with `pytest`
- **Data quality checks** with **Soda Core**

These help ensure both the logic (code) and the output data remain correct as the pipeline evolves.

---

## CI/CD (GitHub Actions)

CI/CD is included so changes to Airflow code, Python dependencies, Docker images, or DAG logic can be validated automatically.

Typical CI tasks include:

- Building the custom Airflow Docker image
- Running the Docker Compose stack
- Ensuring the DAGs and tests still execute as expected

---
