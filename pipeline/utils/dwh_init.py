from db_conn import db_connection
from read_sql import read_sql_file
import warnings
warnings.filterwarnings('ignore')

def dwh_define_schema():
    """
    Defines the schema for the data warehouse (DWH) using an SQL script.

    Returns:
        None: This function does not return any value.

    Note:
        This function assumes that the SQL script for defining the schema
        is located at the specified file path.
        Make sure to handle exceptions properly in the calling code.
    """
    try:
        # Path to the SQL file defining the DWH schema
        file_path = '/home/laode/pacmann/project/elt-with-python/helper/db_init/dwh-stg-prod.sql'

        # Read the content of the SQL file
        sql_string = read_sql_file(file_path)

        # Establish connections to source and DWH databases
        conn_src, cur_src, conn_dwh, cur_dwh = db_connection()

        # Define the query using the SQL content
        query = f"""
        {sql_string}
        """

        # Execute the query to define the DWH schema
        cur_dwh.execute(query)

        # Commit the transaction
        conn_dwh.commit()

        # Close the cursor and connection
        conn_src.close()
        cur_src.close()
        conn_dwh.close()
        cur_dwh.close()
    
    except Exception as e:
        print(f"Error defining DWH schema: {e}")
        
# Execute the functions when the script is run
if __name__ == "__main__":
    dwh_define_schema()