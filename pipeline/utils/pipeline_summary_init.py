import pandas as pd

def pipeline_summary_init():
    summary_data = {
        'timestamp': [],
        'task': [],
        'status' : [],
        'execution_time': []
    }
    
    df = pd.DataFrame(summary_data)
    df.to_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline_summary.csv', index = False)
    df.to_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/data/extract-summary.csv', index = False)
    df.to_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/data/load-summary.csv', index = False)
    df.to_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/data/transform-summary.csv', index = False)
    
if __name__ == '__main__':
    pipeline_summary_init()