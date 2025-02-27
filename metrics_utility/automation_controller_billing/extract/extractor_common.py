import os
import tarfile
import pandas as pd

def process_tarballs(self, path, temp_dir):
    
    try:
        tar = tarfile.open(path)
        # The filter param is available in Python 3.9.17
        tar.extractall(path=temp_dir, filter='data', members=self.tarball_sanitize_members(tar))
    except TypeError:
        # Trying without filter for older python versions
        tar.extractall(path=temp_dir, members=self.tarball_sanitize_members(tar))
    finally:
        tar.close()

    config = self.load_config(os.path.join(temp_dir, 'config.json'))

    # # TODO: read the csvs in batches
    # for chunk in pd.read_csv(filename, chunksize=chunksize):
    # # chunk is a DataFrame. To "process" the rows in the chunk:
    # for index, row in chunk.iterrows():
    #     print(row)

    if os.path.exists(os.path.join(temp_dir, 'job_host_summary.csv')):
        job_host_summary = pd.read_csv(os.path.join(temp_dir, 'job_host_summary.csv'))
    else:
        job_host_summary = pd.DataFrame([{}])

    if os.path.exists(os.path.join(temp_dir, 'indirect_nodes.csv')):
        indirect_nodes = pd.read_csv(os.path.join(temp_dir, 'indirect_nodes.csv'))
    else:
        indirect_nodes = pd.DataFrame([{}])

    if os.path.exists(os.path.join(temp_dir, 'main_jobevent.csv')):
        main_jobevent = pd.read_csv(os.path.join(temp_dir, 'main_jobevent.csv'))
    else:
        main_jobevent = pd.DataFrame([{}])

    return {'main_jobevent': main_jobevent,
            'job_host_summary': job_host_summary,
            'indirect_nodes' : indirect_nodes,
            'config': config}
