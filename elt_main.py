import luigi
import sentry_sdk
import pandas as pd

from pipeline.extract import Extract
from pipeline.load import Load
from pipeline.transform import Transform
from pipeline.utils.concat_dataframe import concat_dataframes
from pipeline.utils.copy_log import copy_log
from pipeline.utils.delete_files_in_directory import delete_files_in_directory

from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

sentry_sdk.init(
    dsn = os.getenv("SENTRY_DSN"),
)


# Execute the functions when the script is run
if __name__ == "__main__":
    luigi.build([Extract(),
                 Load(),
                 Transform()])
    
    concat_dataframes(
        df1 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline_summary.csv'),
        df2 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/data/extract-summary.csv')
    )
    
    concat_dataframes(
        df1 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline_summary.csv'),
        df2 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/data/load-summary.csv')
    )
    
    concat_dataframes(
        df1 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline_summary.csv'),
        df2 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/data/transform-summary.csv')
    )
    
    copy_log(
        source_file = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/log/logs.log',
        destination_file = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/logs/logs.log'
    )
    
    delete_files_in_directory(
        directory = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/data'
    )
    
    delete_files_in_directory(
        directory = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/log'
    )