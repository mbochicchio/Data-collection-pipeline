# Data-collection-pipeline

An automated pipeline for collecting, filtering, and statically analysing GitHub repositories using Apache Airflow, RepoQuester, and Designite.

## Background

The pipeline ingests a list of GitHub repositories, evaluates their quality via RepoQuester, and runs static analysis (DesigniteJava / DPy) on the versions that pass the quality gate. Results are stored in a PostgreSQL database and can be exported to DuckDB for offline analysis.

## Project structure

```
Data-collection-pipeline/
├── dags/                   # Airflow DAG definitions
│   ├── ingestion_dag.py    # Discovers new versions from GitHub
│   ├── quality_gate_dag.py # Filters projects via RepoQuester
│   ├── execution_dag.py    # Runs Designite static analysis
│   └── export_dag.py       # Exports PostgreSQL → DuckDB
├── plugins/
│   └── operators/          # Custom Airflow operators
├── common/                 # Shared DB helpers and domain models
├── config/
│   └── settings.py         # Central configuration (env-driven)
├── scripts/                # Utility scripts (init, reset, seed)
├── tools/
│   ├── Designite/          # DesigniteJava.jar + DPy binary
│   └── Repoquester/        # RepoQuester installation
├── data/                   # Database backups and DuckDB exports (Git LFS)
├── workspace/              # Temporary clone/analysis scratch space
├── ssh-keys/               # SSH keys for host ↔ container communication
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── init.sh                 # First-time setup
└── start.sh                # Start pipeline (Ctrl+C to stop + backup)
```

## Installation

### Requirements

- Docker Desktop (with VirtioFS or gRPC FUSE disabled if on Mac)
- Git LFS (`brew install git-lfs`)
- Apache Airflow 3.x (provided via Docker image)
- DesigniteJava.jar and DPy binaries (not included — see below)
- macOS with Apple Silicon or Intel (DPy requires macOS)

### Steps

**1. Clone the repository**

```bash
git clone <repo-url>
cd Data-collection-pipeline
git lfs pull
```

**2. Configure environment**

```bash
cp .env.example .env
# Fill in GITHUB_TOKEN, AIRFLOW keys, and Designite SSH settings
```

**3. Place Designite binaries**

```
tools/Designite/DesigniteJava.jar
tools/Designite/DPy
```

Register the DPy licence on your Mac:

```bash
cd tools/Designite && ./DPy register <YOUR_LICENSE_KEY>
```

**4. Set up SSH keys for DPy host execution**

```bash
mkdir -p ssh-keys
# Generate a key and add it to your Mac's authorized_keys
# (see .env.example for DESIGNITE_PYTHON_SSH_* variables)
```

**5. Initialise and start**

```bash
./init.sh    # first-time only — seeds the database
./start.sh
```

The Airflow UI will be available at `http://localhost:8080` (admin / admin).

## Usage

All DAGs default to manual trigger (`schedule=None`). Trigger them in order from the Airflow UI:

| DAG | Description |
|-----|-------------|
| `ingestion` | Fetches latest commits for all active projects |
| `quality_gate` | Scores projects with RepoQuester (threshold ≥ 5) |
| `execution` | Runs Designite on versions that passed the gate |
| `export` | Exports all tables to `data/pipeline_export.duckdb` |

To enable automatic scheduling, set the corresponding env variables in `.env`:

```env
INGESTION_SCHEDULE=@daily
QUALITY_GATE_SCHEDULE=@weekly
EXECUTION_SCHEDULE=@daily
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## Authors and acknowledgment
 
> **Dr. Matteo Bochicchio**
> PhD Student · University of Milano-Bicocca
> Department of Informatics, Systems and Communication — ESSeRE Lab
>
> 📧 [matteo.bochicchio@unimib.it](mailto:matteo.bochicchio@unimib.it)
> 🌐 [unimib.it/matteo-bochicchio](https://www.unimib.it/matteo-bochicchio)

## License

This project is distributed under the GNU General Public License v3 (GPL-3.0). See the LICENSE file for the full text.
