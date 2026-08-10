# HEARTS360 Toolkit

## Quick start

```bash
git clone https://github.com/simpledotorg/h360tk_grafana_core.git
cd h360tk_grafana_core
```

Then start the stack. The helper scripts pick free ports automatically, so this
works even when 3000, 5432, 8080 or 5050 are already in use on your machine:

```bash
# Linux / macOS
./scripts/heart360-up.sh

# Windows (PowerShell)
.\scripts\heart360-up.ps1
```

The script prints the URLs it settled on. If you prefer the plain command and
know the default ports are free:

```bash
docker compose up -d
```

| What | Default URL |
| --- | --- |
| Dashboards | <http://localhost:3000> |
| Upload files | <http://localhost:8080> |
| pgAdmin | <http://localhost:5050> |
| PostgreSQL | `localhost:5432` |

> **Something already using those ports, or a corporate proxy in the way?**
> See [docs/ports-and-proxy.md](docs/ports-and-proxy.md). Every port is
> configurable via `.env`, and the stack supports proxies, custom CA
> certificates, internal package mirrors and fully offline installs.

> **Before exposing this to anyone else**, read
> [the hardening checklist](docs/ports-and-proxy.md#hardening). The credentials
> in this repository are public defaults.

---

HEARTS360 Toolkit is a Grafana-based system designed to help District Health Offices monitor hypertension care across facilities. The system processes patient line list data and generates visual dashboards.

## What is HEARTS360 Toolkit

HEARTS360 Toolkit is a Grafana-based system designed to help District Health Offices monitor hypertension care across facilities. The system processes patient line list data and generates visual dashboards that show:

- **Facility-level dashboards:** Each facility can see their own patient data and performance metrics
- **District-level dashboards:** District health office can see aggregated data across all facilities
- **Overdue line lists:** Lists of patients who need follow-up care

## Key Use Cases

HEARTS360 Toolkit supports three main use cases:

**Routine Data Monitoring:** Facility staff export monthly patient line lists from their system and upload Excel files to HEARTS360 Toolkit. The system automatically processes the data and updates dashboards, providing real-time visibility into care delivery, easy identification of patients needing follow-up, and trend analysis over time.

**Dashboard Validation:** Staff upload line lists to HEARTS360 Toolkit and compare the charts with their EHR system dashboards to validate data accuracy, identify data entry errors, and ensure reporting consistency.

**District-Level Oversight:** Each facility uploads their data, and district team views the district-level dashboard to monitor hypertension care across all facilities. This enables district-wide visibility without compromising patient privacy, supports data-driven decision making, and helps allocate resources based on need.

---

## Technical Documentation

### System Overview

HEARTS360 Toolkit is built using:
- **Grafana:** Dashboard visualization platform
- **PostgreSQL:** Database for storing patient and encounter data
- **FileBrowser Quantum:** Web-based file upload interface
- **Python:** Data ingestion scripts
- **Docker:** Containerization for easy deployment

### Installation Instructions

#### Prerequisites

- Docker Desktop installed and running
- Git installed
- At least 4GB RAM available
- Internet connection

#### Step 1: Install Docker Desktop

**For macOS:**
brew install --cask dockerOpen Docker Desktop application and wait for it to start.

**For other operating systems:** Download Docker Desktop from [docker.com](https://www.docker.com/products/docker-desktop)

#### Step 2: Launch HEARTS360 Toolkit
```
git clone https://github.com/simpledotorg/h360tk_grafana_core.git
cd h360tk_grafana_core
./scripts/heart360-up.sh          # Windows: .\scripts\heart360-up.ps1
```
The launcher will:
- Check that ports 3000, 5432, 8080 and 5050 are free, and choose alternatives if not
- Record the chosen ports in `.env` so they stay stable across restarts
- Download required Docker images (first time only)
- Start PostgreSQL database
- Start Grafana dashboard server
- Start FileBrowser for file uploads
- Start file processing service
- Print the URLs to open

`docker compose up -d` on its own does the same thing, but uses the default
ports and fails if any of them is taken.

Wait 30-60 seconds for all services to initialize. Grafana now waits for the
database to report healthy, so the first start takes longer but no longer
crash-loops.

On a corporate network the build may fail while downloading Grafana plugins,
Python packages or the `pg_cron` source — typically with
`SSLError(1, '[SSL: SSLV3_ALERT_HANDSHAKE_FAILURE] ...')`.
[docs/ports-and-proxy.md](docs/ports-and-proxy.md) covers proxy configuration,
custom CA certificates, internal mirrors and offline installation.

#### Step 4: Verify Installation

Use the URLs printed by the launcher. With default ports:

1. **Check Grafana Dashboard:**
   - Open browser: `http://localhost:3000`
   - Login with: `admin` / `admin`
   - You should see the "Hypertension Program" dashboard

2. **Check File Upload:**
   - Open browser: `http://localhost:8080`
   - Login with: `admin` / `admin`
   - You should see the file upload interface

3. **Check Database:**
   - PostgreSQL is running on port 5432
   - Database name: `heart360tk_database`
   - Username: `heart360tk`
   - Password: see `.env.example`


#### Step 5: Test with Sample Data

1. Upload a test Excel file through FileBrowser (`http://localhost:8080`)
2. The file should be automatically processed.
3. The uploaded data is saved immediately in the database, but the dashboard graphs will **not** update straight away because they use precomputed data that needs to be refreshed first.
4. Manually trigger a refresh from the **Admin · Dashboard Refresh** page (`http://localhost:3000/d/heart360-admin-refresh`) to see the data immediately (see [Admin Dashboard — Refreshing Dashboard Data](<docs/Grafana Admin Dashboard Refresh – Visual Documentation.md>)).


#### Step 6: Set Up User Access

- **[User Access Setup Guide](docs/user-access-setup-guide.md)** — How to add users and control which facility's data they can see.

### Updating the System

To get the latest changes from the repository:
```
git pull
docker compose down
docker compose up -d
```
Your `.env` is not tracked by git, so any ports, proxy settings and credentials
you configured are preserved across updates.

### Troubleshooting

| Problem | Where to look |
| --- | --- |
| `port is already allocated` / `Ports are not available` | [docs/ports-and-proxy.md — Ports](docs/ports-and-proxy.md#part-1--ports) |
| `SSLV3_ALERT_HANDSHAKE_FAILURE`, `CERTIFICATE_VERIFY_FAILED`, `tls: handshake timeout` | [docs/ports-and-proxy.md — Proxies and TLS](docs/ports-and-proxy.md#part-2--proxies-and-tls) |
| Uploaded file is not ingested | `docker compose logs file-processor` |
| Dashboards show no new data | Run a refresh from **Admin · Dashboard Refresh** |
| `^M: not found` or `syntax error: unexpected "elif"` | Line endings — see [docs/ports-and-proxy.md](docs/ports-and-proxy.md#part-3--related-pitfalls) |

### Database Considerations

#### Current Setup: PostgreSQL

The system currently uses **PostgreSQL** as the database. This is the recommended setup because:
- The system is already configured and tested with PostgreSQL
- All SQL queries and functions are written for PostgreSQL
- Changing to MySQL would require significant code modifications

#### If You Need MySQL

You have two options:

**Option 1: Use PostgreSQL (Recommended)**
- Install PostgreSQL alongside MySQL
- They can coexist on the same machine
- Minimal changes needed to the system
- Easier to maintain and update

**Option 2: Adapt to MySQL**
- Requires modifying SQL queries (PostgreSQL-specific syntax)
- Need to rewrite database functions
- May require changes to Python ingestion scripts

**Recommendation:** Use PostgreSQL for HEARTS360 Toolkit, even if other systems use MySQL. This keeps the system simple and maintainable.

### System Architecture

#### Components

All host ports below are defaults and can be changed in `.env` — see
[docs/ports-and-proxy.md](docs/ports-and-proxy.md).

1. **Grafana Container**
   - Port: 3000 (`HEART360_GRAFANA_PORT`)
   - Purpose: Dashboard visualization
   - Configuration: `grafana_provisioning/`

2. **PostgreSQL Container**
   - Port: 5432 (`HEART360_POSTGRES_PORT`)
   - Purpose: Data storage
   - Initialization scripts: `pg_init_scripts/`

3. **FileBrowser Container**
   - Port: 8080 (`HEART360_FILEBROWSER_PORT`)
   - Purpose: File upload interface
   - Upload directory: `data/upload/`

4. **pgAdmin Container**
   - Port: 5050 (`HEART360_PGADMIN_PORT`)
   - Purpose: Database administration
   - Configuration: `pgadmin_config/`

5. **File Processor Container**
   - Purpose: Watches for new files and processes them
   - Scripts: `inotify_scripts/`

#### Data Flow

The system processes uploaded files through the following workflow:

1. **File Upload:** User uploads Excel file via FileBrowser web interface
2. **File Storage:** File is saved to `data/upload/` directory
3. **File Detection:** File processor detects new file (using inotify)
4. **Data Processing:** Python script (`ingest_file_h360tk.py`) processes the file
5. **Database Insertion:** Data is inserted into PostgreSQL raw tables (`heart360tk_schema`)
6. **Data Refresh Needed:** Uploaded data is saved in the database, but the dashboard graphs do **not** update until the dashboard data is refreshed (see [Admin Dashboard — Refreshing Dashboard Data](<docs/Grafana Admin Dashboard Refresh – Visual Documentation.md>))
7. **Dashboard Display:** After a refresh, Grafana shows the updated data on the dashboards

### Admin Dashboard — Refreshing Dashboard Data

When a sheet is uploaded, the data is saved in the database immediately, but it will **not** show up in the dashboard graphs straight away. The dashboards use precomputed data that needs to be refreshed first.

A refresh happens in two ways:

- **Automatically:** the data refreshes on its own once every hour.
- **Manually (admin only):** an admin can refresh it right away from the **Admin · Dashboard Refresh** dashboard instead of waiting for the hourly update.

#### Who can refresh

Only admins can refresh the dashboard data. Non-admin users will see an "Access Denied" message on this dashboard and cannot run a refresh.

#### How to refresh after uploading a sheet

1. Upload the sheet via FileBrowser (`http://localhost:8080`) and wait for it to be processed. The data is now in the database but not yet visible in the graphs.
2. Open the **Admin · Dashboard Refresh** dashboard: `http://localhost:3000/d/heart360-admin-refresh`
3. Click the **⟳ Refresh Dashboard** button and confirm. The refresh runs in the background and may take a few minutes.
4. Wait for the status on the dashboard to finish (it moves through **Queued → Refreshing… → All Good**).
5. Once it shows the refresh is complete, open any dashboard (Hypertension, Diabetes, Overdue, etc.) and the newly uploaded data will now be visible.

