# ELT Pipeline Orchestration

- Purposes of this project is to create **ELT pipleines Orchestrations**.
- The data store or destination data of this project applies the **Kimbals approach**.

# How to use this projetct?
## Prerequisites
- OS : Linux or WSL
- Make sure you already installed :
    - Python
        - Python is used for running the ELT Pipeline
    - Docker
        - Docker is used for running postgres instances
        - Data sources & Data warehouse we will run it on Docker.
    - Dbeaver
        - Dbeaver is used to make it easier to access the database using a GUI

- Clone this repo :
  ```
  # Clone using SSH
  git clone git@github.com:rahilaode/orchestrate-elt-with-luigi.git
  ```
  ```
  # Clone using HTTPS
  git clone https://github.com/rahilaode/orchestrate-elt-with-luigi.git
  ```
- Create Sentry Project.
  - Open : https://www.sentry.io
  - Signup with email you want to get notifications abot the error
  - Create Project :
    - Select Platform : Python
    - Set Alert frequency : `On every new issue`
    - Create project name.
  - After create the project, store SENTRY DSN of your project into .env file.

- In thats project directory, create .venv (virtual environment) :
  ```
  python3 -m venv .venv
  ```

- Create env file in project root directory :
  ```
  sudo nano .env
  ```
  - Fill this into .env file :
    ```
    # Example 

    # Source
    SRC_POSTGRES_DB=mini_order
    SRC_POSTGRES_HOST=localhost
    SRC_POSTGRES_USER=postgres
    SRC_POSTGRES_PASSWORD=mypassword
    SRC_POSTGRES_PORT=5433

    # DWH
    DWH_POSTGRES_DB=dwh
    DWH_POSTGRES_HOST=localhost
    DWH_POSTGRES_USER=postgres
    DWH_POSTGRES_PASSWORD=mypassword
    DWH_POSTGRES_PORT=5434

    # SENTRY DSN
    SENTRY_DSN=... # Fill with your Sentry Project DSN
    ```

- Create permissions for bash scripts (setup & run_elt scripts)
  ```
  chmod -R 777 ./helper/utils/
  ```
- Run Setup script. *Notes : Modify paths specified in this script. Please read this script.*
  ```
  ./helper/utils/setup.sh 
  ```

## Run ELT Pipeline with Orchestrations
- Run ELT script. *Notes : Modify paths specified in this script. Please read this script.*
  ```
  ./helper/utils/run_elt.sh 
  ```
- Scheduling :
  - Make sure that your cron services is running. 
    ```
    sudo systemctl status cron
    ```
  - If not running, execute :
    ```
    sudo systemctl start cron
    ```
  - Create a schedule. Use command :
    ```
    crontab -e
    ```
    - Add this line :
      ```
      # Adjust with your path
      * * * * * /home/laode/pacmann/project/orchestrate-elt-with-luigi/helper/utils/run_elt.sh &
      ```
    - Thats schedule means that we want to run the pipeline **every minutes**

- Logging :
  - See [Log File](https://github.com/rahilaode/orchestrate-elt-with-luigi/blob/master/logs/logs.log)

- Reporting :
  - Example :
    | Timestamp               | Task      | Status  | Execution Time |
    |-------------------------|-----------|---------|----------------|
    | 2024-03-19 22:21:22.005 | Extract   | Success | 0.087          |
    | 2024-03-19 22:21:22.124 | Load      | Success | 5.976          |
    | 2024-03-19 22:21:28.155 | Transform | Success | 0.108          |
    | 2024-03-19 22:23:06.192 | Extract   | Success | 0.083          |
    | 2024-03-19 22:23:06.306 | Load      | Success | 4.684          |
    | 2024-03-19 22:23:11.038 | Transform | Success | 0.051          |

  - See [Pipeline Summary](https://github.com/rahilaode/orchestrate-elt-with-luigi/blob/master/pipeline_summary.csv)


- Alerting / Notifications :
  - Make sure again the `SENTRY_DSN` is correct.
  - If correct, so we will get notifications in email that we have signed up in sentry platforms.

# Results
- Open : `http://localhost:8082` to open luigi visualizer.
![alt text](https://sekolahdata-assets.s3.ap-southeast-1.amazonaws.com/notebook-images/mde-data-storage/09-12.png)