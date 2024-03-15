from extract import *
from load import *
from transform import *
from db_conn import *

# Execute the functions when the script is run
if __name__ == '__main__':
    extract()
    load()
    transform()