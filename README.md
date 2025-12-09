# 🚦 US Accidents ELT + ML Project (Databricks | Delta Lake | scikit-learn)

This project implements an end-to-end **ELT (Extract–Load–Transform)** pipeline on the **US Accidents** dataset using **Databricks**, **Delta Lake**, and **scikit-learn**.  
It was developed as part of the **MADSC102 – Unlocking the Power of Big Data** course.

The pipeline covers:

- Ingestion of a large CSV dataset into Databricks
- Storage in **Delta Lake**
- Cleaning & transformation with **PySpark**
- Exploratory analytics using SQL & notebooks
- A lightweight ML model using **scikit-learn**
- A final **Visualization layer** for insights

---

## 🧠 Overview

The goal of this project is to demonstrate how to build a **cloud-style data engineering workflow** using Databricks Community Edition:

- **Extract** US Accidents CSV from Kaggle via **Google Drive + `wget`**
- **Load** into Databricks FileStore (**DBFS**)
- **Transform** using PySpark and persist as **Delta tables**
- **Analyze** data with SQL + notebooks
- **Model** accident severity with a scikit-learn classifier
- **Orchestrate** execution via a simple “job trigger” notebook
- **Visualize** aggregated outputs

Because Databricks CE / Serverless has strict limits on Spark ML model size, the ML part is implemented with **pandas + scikit-learn**, which runs reliably in this environment.

---

## 🏗 Architecture

The high-level architecture is:

- Kaggle CSV → Google Drive → Databricks DBFS  
- PySpark ETL → Delta Lake  
- EDA Notebook + ML Notebook → Visualization Layer

> Save the provided architecture image as `docs/architecture.png`.

![Architecture Diagram](docs/architecture.png)

---

## 📦 Tools & Technologies

| Category        | Tools / Services                                      |
|----------------|--------------------------------------------------------|
| Platform       | Databricks Community Edition / Databricks Workspace   |
| Storage        | DBFS (FileStore), Delta Lake                          |
| Processing     | PySpark, SQL, Pandas                                  |
| Machine Learning | scikit-learn (Logistic Regression)                  |
| Orchestration  | Databricks notebook `%run` trigger                    |
| Visualization  | Any BI / plotting tool (e.g. matplotlib, Power BI, etc.) |
| Data Source    | US Accidents dataset (Kaggle)                          |

---

## 🗂 Dataset: US Accidents

The **US Accidents** dataset contains several million accident records collected via various traffic APIs. It includes:

- Accident ID and timestamps  
- Latitude & longitude  
- City, state  
- Severity (1–4)  
- Distance, duration  
- Weather and road condition fields (depending on version)

This project uses:

- **Time-based features** (`start_hour`, `duration_minutes`)  
- **Location features** (`state`)  
- **Engineered flags** (`is_weekend`)  

to support analytics and severity prediction.

---

## 🔄 ELT Pipeline

### 1️⃣ Extract – From Kaggle → Google Drive → Databricks

Due to upload limits in Databricks CE, the CSV is stored on **Google Drive** and pulled into **DBFS** using `wget`:

```bash
%sh
wget "https://drive.google.com/uc?export=download&id=<FILE_ID>" \
  -O /dbfs/FileStore/tables/us_accidents.csv
