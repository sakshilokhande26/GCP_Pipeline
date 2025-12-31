# 🚀 GCP Student Data Pipeline
An automated data pipeline built on Google Cloud Platform (GCP) that cleans dirty Excel/CSV files and loads them to BigQuery with duplicate detection and email notifications.

## 🎯 Overview
This pipeline automatically:
- Detects new file uploads to Cloud Storage
- Cleans dirty data (removes special characters, fixes phone numbers, standardizes dates)
- Loads cleaned data to BigQuery
- Archives original files
- Tracks processing history to prevent duplicates


## ✨ Features

| Feature | Description |
|---------|-------------|
| **Automatic Trigger** | Pipeline runs automatically when file is uploaded to GCS |
| **Duplicate Detection** | Tracks files in log table; skips unchanged files |
| **Data Cleaning** | Removes special characters, fixes phones, standardizes dates |
| **Data Archiving** | Original files archived with timestamp |
| **State Management** | Full audit trail in BigQuery |
| **Free Trial Compatible** | Works within GCP $300 free credits |
  
## 🏗️ Architecture
<img width="785" height="605" alt="image" src="https://github.com/user-attachments/assets/2f5b112b-1e5e-4cb2-9512-624ba7ffb840" />

## 🧩 Components

### GCP Services Used

| Service | Resource Name | Purpose |
|---------|--------------|---------|
| Cloud Storage | `{project}-raw-incoming` | Store incoming files |
| Cloud Storage | `{project}-staging-clean` | Store cleaned files (temp) |
| Cloud Storage | `{project}-archive` | Store archived originals |
| BigQuery | `student_data.students` | Clean data table |
| BigQuery | `student_data.file_load_log` | Processing history |
| Cloud Functions | `student-data-pipeline` | Python processing code |
| Eventarc | GCS Trigger | Auto-trigger on upload |

---

## 📊 Workflow

The pipeline executes 11 steps:

1. **Upload File** - User uploads `Students.xlsx` to GCS bucket
2. **Trigger** - Eventarc detects file and triggers Cloud Function
3. **Get Metadata** - Read file's last modified timestamp
4. **Lookup Log** - Query BigQuery: was file processed before?
5. **Compare Timestamps** - Determine: First load? Modified? Not modified?
6. **Clean Data** - Remove special characters, fix phones, format dates
7. **Write to Staging** - Save cleaned CSV to staging bucket
8. **Load to BigQuery** - Insert rows into students table
9. **Archive & Cleanup** - Move original to archive, delete staging
10. **Update Log** - Record success/skip in `file_load_log`


