# ELT Pipeline Orhcestration
## How to use this project?
1. Requirements
2. Preparations
3. Orchestrate ELT Pipelines

### 1. Requirements
- OS :
    - Linux
    - WSL (Windows Subsystem For Linux)
- Tools :
    - Dbeaver
    - Docker
    - Cron
- Programming Language :
    - Python
    - SQL
- Python Libray :
    - Luigi
    - Pandas
    - Sentry-SDK
- Platforms :
    - Sentry

### 2. Preparations
- **Clone repo** :
  ```
  git clone https://github.com/rahilaode/orchestrate-elt-with-luigi.git
  ```

- **Create Sentry Project** :
  - Open : https://www.sentry.io
  - Signup with email you want to get notifications abot the error
  - Create Project :
    - Select Platform : Python
    - Set Alert frequency : `On every new issue`
    - Create project name.
  - After create the project, **store SENTRY DSN of your project into .env file**.

- **Create temp dir**. Execute this on root project directory :
    ```
    mkdir pipeline/temp/data
    mkdir pipeline/temp/log
    ```
  
- In thats project directory, **create and use virtual environment**.
- In virtual environment, **install requirements** :
  ```
  pip install -r requirements.txt
  ```

- **Create env file** in project root directory :
  ```
  # Source
  SRC_POSTGRES_DB=...
  SRC_POSTGRES_HOST=...
  SRC_POSTGRES_USER=...
  SRC_POSTGRES_PASSWORD=...
  SRC_POSTGRES_PORT=...

  # DWH
  DWH_POSTGRES_DB=...
  DWH_POSTGRES_HOST=...
  DWH_POSTGRES_USER=...
  DWH_POSTGRES_PASSWORD=...
  DWH_POSTGRES_PORT=...

  # SENTRY DSN
  SENTRY_DSN=... # Fill with your Sentry DSN Project 

  # DIRECTORY
  # Adjust with your directory. make sure to write full path
  DIR_ROOT_PROJECT=...     # <project_dir>
  DIR_TEMP_LOG=...         # <project_dir>/pipeline/temp/log
  DIR_TEMP_DATA=...        # <project_dir>/pipeline/temp/data
  DIR_EXTRACT_QUERY=...    # <project_dir>/pipeline/src_query/extract
  DIR_LOAD_QUERY=...       # <project_dir>/pipeline/src_query/load
  DIR_TRANSFORM_QUERY=...  # <project_dir>/pipeline/src_query/transform
  DIR_LOG=...              # <project_dir>/logs/
    ```

- **Run Data Sources & Data Warehouses** :
  ```
  docker compose up -d
  ```

### 3. Orchestrate ELT Pipeline
- Run Luigi UI :
  ```
  luigid --port 8082 &
  ```
- Create schedule to run pipline every one hour.
  ```
  0 * * * * <project_dir>/elt_run.sh
  ```

### Result
- Pipeline
    - ![alt text](https://github.com/rahilaode/orchestrate-elt-with-luigi/blob/master/img_asset/luigi.png)

- Log
    - ![alt text](https://github.com/rahilaode/orchestrate-elt-with-luigi/blob/master/img_asset/log.png)

- Summary
    - ![alt text](https://github.com/rahilaode/orchestrate-elt-with-luigi/blob/master/img_asset/pipeline-summary.png)