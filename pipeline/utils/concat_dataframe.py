import pandas as pd

def concat_dataframes(df1, df2):
    """
    Concatenates two DataFrames along the rows.

    Parameters:
    - df1, df2: DataFrames

    Returns:
    - concatenated_df: DataFrame
    """
    concatenated_df = pd.concat([df1, df2], ignore_index=True)
    concatenated_df.to_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline_summary.csv', index = False)