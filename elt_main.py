from helper.utils.extract import *
from helper.utils.load import *
from helper.utils.transform import *
from helper.utils.db_conn import *

# Execute the functions when the script is run
if __name__ == '__main__':
    extract()
    load()
    transform()