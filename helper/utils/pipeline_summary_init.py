import pandas as pd

def pipeline_summary_init():
    summary_data = {
        'timestamp': [],
        'tables_name': [],
        'task': [],
        'status' : [],
        'execution_time': []
    }
    
    df = pd.DataFrame(summary_data)
    df.to_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline_summary.csv', index = False)
    
if __name__ == '__main__':
    pipeline_summary_init()