# European Football Match Predictor (End-to-End Data Engineering)

## Overview

This is a full-stack Data Engineering and Analytics application designed to predict the outcome of football matches across Europe's top 5 leagues (EPL, La Liga, Bundesliga, Serie A, Ligue 1).

Unlike simple static dashboards, this project implements a **Lakehouse Architecture** on Azure Databricks. It features a live data pipeline that processes raw match data through Silver and Gold layers, engineering advanced features like "Rolling Form" and "Venue-Specific Strength" to drive predictions.

---

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
git clone [https://github.com/YOUR_USERNAME/football-predictor.git](https://github.com/YOUR_USERNAME/football-predictor.git)
cd football-predictor
```
