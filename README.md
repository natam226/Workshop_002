# Workshop_002: ETL Pipeline for Spotify, Grammy and an API Data Analysis

This project builds an ETL (Extract, Transform, Load) pipeline that processes Spotify and enhances them with musical data from the Wikidata API. It includes data cleaning, fuzzy matching, API integration and in-depth analysis, orchestrated with Apache Airflow and stored in a PostgreSQL database.

---

## 📎 Project Overview

The pipeline performs the following steps:

- **Extract**:
  - Reads raw Spotify data from a CSV file.
  - Extracts Grammy nomination data from a PostgreSQL database.
  - Makes requests to the API to extract the data.

- **Transform**:
  - Cleans and preprocesses Spotify, Grammy and the API datasets.
  - Merges the datasets to align Spotify tracks with Grammy nominations.
  - Enriches the merged dataset with metadata from the Wikidata API.

- **Load**:
  - Stores the final enriched dataset in a PostgreSQL database.

- **Store** *(optional)*:
  - Uploads the results to Google Drive.

---

## 📂 Project Structure

```
Workshop_002/
├── airflow/                  # Airflow-related files
├── dag/                      # Airflow DAG
│   └── dag_workshop.py       # Main DAG file
├── data/                     # Data storaged
├── notebooks/                # Jupyter notebooks
├── src/                      # Python scripts for the ETL pipeline         
│   ├── extract/
│   │   ├── extract_api.py
│   │   ├── extract_grammy.py
│   │   ├── extract_spotify.py
│   ├── load/
│   │   ├── load.py
│   │   ├── store.py
│   ├── transform/
│   │   ├── merge.py
│   │   ├── transform_api.py
│   │   ├── transform_grammy.py
│   │   ├── transform_spotify.py
├── venv/                     # Virtual environment
├── .gitignore                # Git ignore file
└── requirements.txt          # Project dependencies
```

---

## ✅ Prerequisites

- Python 3.8+
- Apache Airflow
- PostgreSQL
- Google Drive API credentials *(optional)*

---

## 🛠️ Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository-url>
```

### 2. Set Up a Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Airflow

```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow webserver --port 8080
airflow scheduler
```

### 5. Set Up Credentials

Create a file `credentials.json` with the following structure:

```json
{
  "postgresql": {
    "username": "your_username",
    "password": "your_password",
    "host": "your_host",
    "port": "your_port",
    "database": "your_database"
  }
```

Ensure this file is included in `.gitignore`.

---

## 📥 Prepare the Data

- Place the raw Spotify dataset (`spotify_dataset.csv`) in `data`.
- Ensure the Grammy nominations data is stored in your PostgreSQL database under the table `grammy_raw_data`.

---

## 🚀 Usage

### Access Airflow UI

Open your browser and go to [http://localhost:8080](http://localhost:8080).  
Default credentials:  
- **Username**: `airflow`  
- **Password**: `airflow`

### Trigger the DAG

- Locate the `etl_pipeline` DAG.
- Turn it **On** and click **"Trigger DAG"**.

### Monitor the Pipeline

- Use the Airflow UI to track task status.
- Logs are available under `airflow/logs/`.

### Output

- Final dataset saved in PostgreSQL under `data_pipeline`.
- If implemented, data is uploaded to Google Drive.

---

## 📝 Pipeline Tasks

The `etl_pipeline` DAG includes:

- `task_extract_spotify`
- `task_extract_grammy`
- `task_extract_api`
- `task_transform_spotify`
- `task_transform_grammy`
- `task_transform_api`
- `task_merge`
- `task_load`
- `task_store_to_drive` *(optional)*

---

## 📁 Dependencies

Key packages in `requirements.txt` include:

- `pandas`
- `sqlalchemy`
- `apache-airflow`

---
