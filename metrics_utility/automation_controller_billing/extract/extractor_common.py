import os
import tarfile
import pandas as pd


def safe_extract(path, extract_path):
    count = 0

    size = 0
    try:
        tar = tarfile.open(path)
        for member in tar.getmembers():
            if member.isdir():
                continue
            if member.name.endswith("json") is False and member.name.endswith("csv") is False:
                continue
            if ".." in member.path:
                continue

            tar.extract(member, path=extract_path)
            size += member.size
            count += 1

            if count > 100:
                print(f'Maximum members of tarball {path} is 100')
                return

            if size > 1024 * 1024 * 1024:
                print(f'Maximum size of tarball files {path} is 1 GB')
                return

    except Exception as e:
        raise e


def process_tarballs(self, path, temp_dir):
    try:
        safe_extract(path, temp_dir)
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
    except Exception as e:
        raise e
