# Begin by activating a venv containing rasterio and other required packages.
# This can be achieved for now with "source <REL>/hls_transfer/bin/activate" 
# where <REL> is some relative path to the given folder. Current full path is:
# /p/largedata2/sdlrsd/data_transfer_test/hls_transfer/bin/activate

# This code should be copied over to and run from the 
# ./earthdata-data-downloader/code/ directory.

# It is advisable that this script be run in a separate screen (see "man screen") as this is a parallelized script that might take some time to complete.

# from pathlib import Path
# print(f"CWD: {Path.cwd()}")
from data_preparer import DataPreparer
from cmr_connector import COLLECTIONS
import getpass
import multiprocessing
import os

# desired_path = input("[presumably Relative or Absolute]Path to where we want to download the data: ")
desired_path = "../../data_transfer_files/downloaded_data/"
username = input("Username: ")
password = getpass.getpass()
num_processes = 1

def get_path(i:int):
    return f"../../data_transfer_files/us_hls_tiles_purged_total{num_processes}_{i}.json"
    
def worker_function(i):
    print(f"Worker {i} has been started...")
    for collection_id in COLLECTIONS:
        dp = DataPreparer(collection_id, username, password, desired_path, get_path(i), "train_indices")
        # print(f"{collection_id}, self.tiles_info: {dp.tiles_info}")
        # continue
        dp.download_and_prepare_clips()

# for collection_id in COLLECTIONS:
#     dp = DataPreparer(collection_id, username, password, desired_path, "../../data_transfer_files/us_hls_tiles_purged_t3_s3_p00.json", "train_indices")
#     dp.download_and_prepare_clips()


if __name__ == "__main__":
    # num_processes = 8
    processes = []
    print("Number of usable cores: ", len(os.sched_getaffinity(0)))
    for i in range(num_processes):
        p = multiprocessing.Process(target=worker_function, args=(i,))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()
