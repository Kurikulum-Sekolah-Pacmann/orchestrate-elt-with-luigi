import pandas as pd
from helper.utils.db_conn import db_connection
import warnings
warnings.filterwarnings('ignore')


def get_table_data(table_name, engine):
    """
    Retrieves data from a specified table in a SQL database and returns it as a pandas DataFrame.

    Args:
        table_name (str): The name of the table from which to retrieve data.
        engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine object connected to the database.

    Returns:
        pandas.DataFrame: A DataFrame containing the data from the specified table.

    Notes:
        Ensure that the engine provided is properly configured and connected to the database.
        Handle exceptions properly in the calling code.
        If an error occurs during data retrieval, an empty DataFrame will be returned.
    """
    try:
        # Construct SQL query to select all data from the specified table
        query = f"SELECT * FROM {table_name};"

        # Execute the query and read the results into a DataFrame
        df = pd.read_sql(query, engine)
        
        return df

    except Exception as e:
        # Print the error message if an exception occurs
        print(f"Error: {e}")
        
        # Return an empty DataFrame in case of an error
        return pd.DataFrame()

def extract():
    """
    Extract data from source and staging databases, compare the data,
    and save the differences to CSV files.

    Returns:
    None

    Raises:
    Any exceptions raised by the underlying functions/db_connection,
    get_table_data, compare, or pandas.DataFrame.to_csv methods.
    """
    try:
        # Establish connections to source and DWH databases
        conn_src, cur_src, conn_dwh, cur_dwh = db_connection()
        
        # Extract from data sources
        src_category = get_table_data('category', conn_src)
        src_customer = get_table_data('customer', conn_src)
        src_order_detail = get_table_data('order_detail', conn_src)
        src_orders = get_table_data('orders', conn_src)
        src_product = get_table_data('product', conn_src)
        src_subcategory = get_table_data('subcategory', conn_src)
        
        # Save to csv file
        src_category.to_csv('/home/laode/pacmann/project/elt-with-python/helper/utils/temp_data/category.csv', index = False) 
        src_subcategory.to_csv('/home/laode/pacmann/project/elt-with-python/helper/utils/temp_data/subcategory.csv', index = False) 
        src_customer.to_csv('/home/laode/pacmann/project/elt-with-python/helper/utils/temp_data/customer.csv', index = False) 
        src_orders.to_csv('/home/laode/pacmann/project/elt-with-python/helper/utils/temp_data/orders.csv', index = False) 
        src_product.to_csv('/home/laode/pacmann/project/elt-with-python/helper/utils/temp_data/product.csv', index = False) 
        src_order_detail.to_csv('/home/laode/pacmann/project/elt-with-python/helper/utils/temp_data/order_detail.csv', index = False) 
        
        # Close the cursor and connection
        conn_src.close()
        cur_src.close()
        conn_dwh.close()
        cur_dwh.close()

    except Exception as e:
        print(f"Error extracting and loading data: {e}")
        
# Execute the functions when the script is run
if __name__ == "__main__":
    extract()