In order to launch this poc:

```
git clone git@github.com:simpledotorg/poc_heart360_on_grafana.git
cd poc_heart360_on_grafana
docker compose up -d
```

Then just go to this url:

http://localhost:3000/d/heart360demo/heart-360-global-dashboard

to upload file:

http://localhost:8080/

---

# HEARTS360 Toolkit

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
git clone git@github.com:simpledotorg/poc_heart360_on_grafana.git
cd poc_heart360_on_grafana
docker compose up -d
```
`docker compose up -d` command will:
- Download required Docker images (first time only)
- Start PostgreSQL database
- Start Grafana dashboard server
- Start FileBrowser for file uploads
- Start file processing service

Wait 30-60 seconds for all services to initialize.

#### Step 4: Verify Installation

1. **Check Grafana Dashboard:**
   - Open browser: `http://localhost:3000`
   - Login with: `admin` / `admin`
   - You should see the "HEARTS360 Global Dashboard"

2. **Check File Upload:**
   - Open browser: `http://localhost:8080`
   - Login with: `admin` / `admin`
   - You should see the file upload interface

3. **Check Database:**
   - PostgreSQL is running on port 5432
   - Database name: `metrics_db`
   - Username: `grafana_user`
   - Password: `your_db_password`

#### Step 5: Test with Sample Data

1. Upload a test Excel file through FileBrowser (`http://localhost:8080`)
2. The file should be automatically processed
3. Check Grafana dashboard - data should appear within a few seconds

### Updating the System

To get the latest changes from the repository:
```
git pull
docker compose down
docker compose up -d
```
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

1. **Grafana Container**
   - Port: 3000
   - Purpose: Dashboard visualization
   - Configuration: `grafana_provisioning/`

2. **PostgreSQL Container**
   - Port: 5432
   - Purpose: Data storage
   - Initialization scripts: `pg_init_scripts/`

3. **FileBrowser Container**
   - Port: 8080
   - Purpose: File upload interface
   - Upload directory: `data/upload/`

4. **File Processor Container**
   - Purpose: Watches for new files and processes them
   - Scripts: `inotify_scripts/`

#### Data Flow

The system processes uploaded files through the following workflow:

1. **File Upload:** User uploads Excel file via FileBrowser web interface
2. **File Storage:** File is saved to `data/upload/` directory
3. **File Detection:** File processor detects new file (using inotify)
4. **Data Processing:** Python script (`ingest_file_puskesmas.py`) processes the file
5. **Database Insertion:** Data is inserted into PostgreSQL database
6. **Dashboard Display:** Grafana queries database and displays dashboards

