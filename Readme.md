# Project Execution Guide

This project uses a TaskFile.yaml to run the data pipeline (ingestion → match facts → standings → exports).

Below is a short guide on how to execute the main tasks.

## Setup

Install project dependencies:

```bash
task set_up_project
```
## Core Tasks
Ingestion

Runs the ingestion step. Optional arguments: WEEK_ITR and BATCH.

task ingest LOG_PATH=logs/ingestion.log

Weekly example:

task ingest WEEK_ITR=5 BATCH=true LOG_PATH=logs/ingestion_week5.log

Match Facts

task match_facts LOG_PATH=logs/match_facts.log

Weekly example:

task match_facts WEEK_ITR=5 BATCH=true LOG_PATH=logs/match_facts_week5.log

Standings

task standings LOG_PATH=logs/standings.log

Weekly example:

task standings WEEK_ITR=5 BATCH=true LOG_PATH=logs/standings_week5.log

Save a Table to CSV

task save_csv INPUT_FILE_PATH="standings" OUTPUT_FILE_NAME="standings_final.csv" LOG_PATH="logs/save.log"

##. Full Pipelines
Final Standings (Full Season)

Runs ingestion → match facts → standings → CSV export:

task calculating_total_standings

Historical Weekly Standings

Simulates all weekly runs:

task calculating_historical_standings

4. Recent Form

If recent form calculation uses Docker:

task compute_recent_form

5. Notes

• Environment variables are loaded from .env and ~/.env
• Most tasks accept LOG_PATH to store logs
• Weekly execution is done using WEEK_ITR and BATCH=true