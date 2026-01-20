# European Football Match Predictor (End-to-End Data Engineering)

## Overview

This is a full-stack Data Engineering and Analytics application designed to predict the outcome of football matches across Europe's top 5 leagues (EPL, La Liga, Bundesliga, Serie A, Ligue 1).

Unlike simple static dashboards, this project implements a **Lakehouse Architecture** on Azure Databricks. It features a live data pipeline that processes raw match data through Silver and Gold layers, engineering advanced features like "Rolling Form" and "Venue-Specific Strength" to drive predictions.

---

Live View: https://eu-football-predictor.streamlit.app/

## Architecture

The system follows the **Medallion Architecture** pattern:

1.  **Ingestion (Bronze):** Raw match data is ingested into Azure Data Lake Storage (ADLS Gen2).
2.  **Processing (Silver):** Data is cleaned, standardized, and stored in Delta format using PySpark.
3.  **Feature Engineering (Gold):** Complex aggregations are performed to calculate:
    - **Rolling Form (Last 5 Games):** Weighted performance metrics.
    - **Venue Strength:** Specific performance metrics for Home vs. Away games.
    - **Goal Analysis:** Offensive and defensive ratings based on recent history.
4.  **Serving Layer:** A custom **Streamlit** application connects securely to the Databricks SQL Warehouse via REST API to serve real-time insights.

---

## The Prediction Model

This application utilizes a **Statistical Form-Based Heuristic Model**. Instead of a "Black Box" machine learning model, it uses a transparent, explainable algorithm preferred by sports analysts.

**How it works:**

1.  **Data Extraction:** Fetches the last 5 matches for both Home and Away teams.
2.  **Contextual Filtering:**
    - Calculates the Home Team's form specifically in **Home Games**.
    - Calculates the Away Team's form specifically in **Away Games**.
3.  **Form Differential:** It computes a `Form_Delta` score:
    $$\Delta = \text{HomeForm} - \text{AwayForm}$$
4.  **Decision Boundary:**
    - If $\Delta \ge 3$: Predicts **Home Win** (Strong probability).
    - If $\Delta \le -3$: Predicts **Away Win** (Strong probability).
    - Otherwise: Predicts **Draw / Tight Match**.

---

## How to Run Locally

Follow these steps to set up the project on your local machine.

### Prerequisites

- Python 3.8+
- An active Azure Databricks Cluster (for the backend)

### 1. Clone the Repository

```bash
git clone https://github.com/Gem726526/Football-Predictor.git
cd football-predictor
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Secrets (Security)

```bash
Create a file named .env in the root directory. Do not commit this file. Add your Databricks credentials:

DATABRICKS_HOSTNAME=adb-xxxxxxxx.xx.azuredatabricks.net
DATABRICKS_HTTP_PATH=sql/protocolv1/o/xxxx/xxxx
DATABRICKS_TOKEN=dapi...
```

### 4. Run the Application

```bash
streamlit run app.py
```

### Project Structure

```bash
football-predictor/
├── app.py                 # Main Application (Frontend & Logic)
├── requirements.txt       # Python Dependencies
├── README.md              # Documentation
├── .gitignore             # Security Rules
└── assets/                # Screenshots and diagrams
```

### Tech Stack

```bash
Frontend: Streamlit, Plotly

Backend: Azure Databricks (SQL Warehouse)

Storage: Delta Lake (Gold Layer)

Language: Python, SQL, PySpark
```

## Architecture & Data Pipeline

The project follows the standard **Medallion Architecture**:

### 1. Bronze Layer (Raw Ingestion)

- **Source:** External Football APIs.
- **Format:** JSON/CSV landing in Azure Data Lake Storage (ADLS).
- **Notebook:** `notebooks/1_bronze_ingest.py`

### 2. Silver Layer (Cleaning & Schema)

- **Transformation:** Deduplication, date standardization, and renaming columns (e.g., `FTHG` -> `Full Time Home Goals`).
- **Storage:** Delta Lake (Parquet).
- **Notebook:** `notebooks/2_silver_process.py`

### 3. Gold Layer (Feature Engineering)

This is where the predictive analysis happens. We transform raw match results into "Team Features".

- **Logic:** \* Explodes matches so every game creates two rows (one for Home, one for Away).
  - Calculates **Rolling 5-Game Form** using SQL Window Functions.
  - Computes **Venue Specific Form** (e.g., "How good is Arsenal at Home?").
- **Notebook:** `notebooks/3_gold_features.sql`

---

## The Prediction Logic

The app uses a **Statistical Heuristic Model** based on the derived Gold data:

1.  **Fetch Data:** Get the last 5 games for both Home and Away teams.
2.  **Calculate Form Score:**
    - _Home Team Strength_ = Rolling Points in last 5 **HOME** games.
    - _Away Team Strength_ = Rolling Points in last 5 **AWAY** games.
3.  **Prediction Algorithm:**
    $$\Delta = \text{HomeStrength} - \text{AwayStrength}$$
    - **Home Win:** $\Delta \ge 3$
    - **Away Win:** $\Delta \le -3$
    - **Draw:** $-3 < \Delta < 3$

---

### Setting up the Data (Databricks)

```bash
1.  **Upload Notebooks:** Import the `notebooks/` folder into your Databricks Workspace.
2.  **Run Pipeline:**
    * Run `00_Setup_Mount` to setup
    * Run `01_Ingest_Raw_Leagues` to fetch data.
    * Run `02_Transform_Silver` to clean data.
    * Run `03_Engineer_Features_Gold` to generate the Feature Store.
3.  **Get Credentials:** Go to **User Settings > Developer** and generate an Access Token.
```
