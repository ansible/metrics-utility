import pandas as pd
import os
import tarfile


def write_member(member_path, file_obj, max_size, total_extracted_size):
    with open(member_path, 'wb') as out_f:
        chunk_size = 1024 * 1024  # 1 MB buffer
        while True:
            data = file_obj.read(chunk_size)
            if not data:
                break
            total_extracted_size += len(data)
            if total_extracted_size > max_size:
                # Stop if we exceed total extraction size
                raise ValueError("Extraction aborted: Maximum total size exceeded.")
            out_f.write(data)
    return total_extracted_size


def safe_extract(tar_path, extract_path, max_files=100, max_size=1024*1024*1024):
    """
    Safely extract a tar archive from 'tar_path' into 'extract_path' with constraints:
      - Only extract *.json or *.csv files
      - Skip directories, symbolic links, and hard links
      - Limit number of extracted files to 'max_files'
      - Limit total uncompressed size to 'max_size' bytes
    """
    extracted_files = 0
    total_extracted_size = 0

    # Ensure the extraction directory exists
    os.makedirs(extract_path, exist_ok=True)

    with tarfile.open(tar_path, 'r:*') as tar:
        for member in tar.getmembers():

            # 1) Skip directories and links
            if member.isdir():
                continue
            if member.issym() or member.islnk():
                print(f"Skipping link: {member.name}")
                continue

            # 2) Only allow .json or .csv
            if not (member.name.endswith('.json') or member.name.endswith('.csv')):
                continue

            # 3) Build a fully qualified path for this member
            #    and ensure it stays within extract_path.
            member_path = os.path.abspath(os.path.join(extract_path, member.name))
            extract_path_abs = os.path.abspath(extract_path)
            if not member_path.startswith(extract_path_abs + os.sep):
                print(f"Skipping potentially unsafe file (path traversal): {member.name}")
                continue

            # 4) Limit total files
            if extracted_files >= max_files:
                print(f"Reached max file limit of {max_files}.")
                break

            # 5) Extract file contents manually, in chunks,
            #    to avoid trusting the tar's metadata size.
            file_obj = tar.extractfile(member)
            if file_obj is None:
                # Could not read the file content for some reason
                continue

            # Make sure the subdirectory structure exists
            os.makedirs(os.path.dirname(member_path), exist_ok=True)

            # Write out the file, limiting max size
            total_extracted_size = write_member(member_path, file_obj, max_size, total_extracted_size)

            extracted_files += 1

    print(f"Extraction complete. Files extracted: {extracted_files}, "
          f"Total size: {total_extracted_size} bytes.")

def process_tarballs(self, path, temp_dir):
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

    if os.path.exists(os.path.join(temp_dir, 'main_indirectmanagednodeaudit.csv')):
        indirect_nodes = pd.read_csv(os.path.join(temp_dir, 'main_indirectmanagednodeaudit.csv'))
    else:
        indirect_nodes = pd.DataFrame([{}])

    if os.path.exists(os.path.join(temp_dir, 'main_jobevent.csv')):
        main_jobevent = pd.read_csv(os.path.join(temp_dir, 'main_jobevent.csv'))
    else:
        main_jobevent = pd.DataFrame([{}])

    if os.path.exists(os.path.join(temp_dir, 'main_host.csv')):
        main_host = pd.read_csv(os.path.join(temp_dir, 'main_host.csv'))
    else:
        main_host = pd.DataFrame([{}])


    return {'main_jobevent': main_jobevent,
            'main_host': main_host,
            'job_host_summary': job_host_summary,
            'indirect_nodes' : indirect_nodes,
            'config': config}
