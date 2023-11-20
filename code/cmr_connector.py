import numpy as np
import requests
import time

from datetime import datetime
from lib.utils import extract_minimum_bounding_box

CMR_URL = "https://cmr.earthdata.nasa.gov/search/granules.umm_json?collection_concept_id={collection_id}&temporal[]={start_datetime}&temporal[]={end_datetime}&page_size=1000"
QUERY = ""
# HLS L30 = C2021957657-LPCLOUD
# HLS S30 = C2021957295-LPCLOUD
COLLECTIONS = ['C2021957657-LPCLOUD', 'C2021957295-LPCLOUD']

import logging
from pathlib import Path
FOLDER = Path(__file__).parent.parent.parent / "data_transfer_files" 

logging.basicConfig(filename=FOLDER / "debug.log", level=logging.DEBUG)
handler = logging.FileHandler(FOLDER / "warning.log")
warn_log = logging.getLogger("Warning_logger")
warn_log.setLevel(logging.WARNING)
warn_log.addHandler(handler)


class CMRConnector:
    def __init__(self, collection_id, tiles_info, split):
        self.collection_id = collection_id
        self.cmr_urls = list()

        for date in tiles_info[split]:
            date_time = datetime.strptime(date, '%Y%j')
            date = date_time.strftime('%Y-%m-%d')
            start_datetime = f"{date}T00:00:00Z"
            end_datetime = f"{date}T23:59:59Z"
            self.cmr_urls.append(
                CMR_URL.format(
                    collection_id=self.collection_id,
                    start_datetime=start_datetime,
                    end_datetime=end_datetime,
                )
            )
            # break  # NB: This change (to comment this line, "break") is to work with products from multiple datetimes

    def list_downloadable_links(self):
        # pass json file to download
        # query cmr based on the dates
        downloadable_links = list()
        count = 0
        for cmr_url in self.cmr_urls:
            count += 1
            print(f"list_downloadable_links() running iteration: {count}")
            load = True
            page_num = 1
            while load:
                try:
                    retries = 10
                    try_number = 0
                    try:
                        while try_number <= retries:
                            try:
                                response = requests.get(f"{cmr_url}&page_num={page_num}", timeout=200+100*try_number)
                                # if response.status_code == 404:
                                #     print(f"404 Status code, trying pagesize 1000...")
                                #     logging.error(f"404 Status code, trying pagesize 1000...")
                                #     warn_log.error(f"Status code == 404 for link: {cmr_url}&page_num={page_num}")
                                #     if cmr_url[-4:] == "1000":
                                #         try_number += 1
                                #     cmr_url = cmr_url[:-4] + "1000"  # setpage size to 1000
                                #     continue
                                break
                            except requests.exceptions.Timeout as e:
                                print(f"Timeout received on try number (zero-indexed): {try_number}, with timeout time set to {200+100*try_number}s")
                                logging.info(f"Timeout received on try number (zero-indexed): {try_number}, with timeout time set to {200+100*try_number}s")
                            except requests.exceptions.HTTPError as e:
                                print(f"HTTP Error received on try number (zero-indexed): {try_number}: {e}")
                                logging.error(f"HTTP Error received on try number (zero-indexed): {try_number}: {e}")
                                time.sleep(min(2**10, 2**try_number+10))
                                if response.status_code == 404 or try_number > retries//2:
                                    if response.status_code == 404:
                                        print(f"Status code == 404 for link: {cmr_url}&page_num={page_num}")
                                        warn_log.error(f"Status code == 404 for link: {cmr_url}&page_num={page_num}")
                                    cmr_url = cmr_url[:-4] + "1000"  # setpage size to 1000
                            try_number += 1
                            if try_number > retries:
                                logging.warning(f"Max retries reached for cmr_url: {cmr_url}&page_num={page_num}")
                                continue

                        response.raise_for_status()
                    except Exception as e:
                        print(e)
                        warn_log.error(e)
                        print(f"Error on url: {cmr_url}&page_num={page_num}")
                        warn_log.error(f"Error on url: {cmr_url}&page_num={page_num}")
                        load = False
                        continue

                    if not (response.json()['items']):
                        load = False
                        continue
                    len_dl_links = len(downloadable_links)
                    for item in response.json()['items']:
                        for url in item['umm']['RelatedUrls']:
                            if (
                                ('s3:' not in url['URL'])
                                and ('.tif' in url['URL'])
                                # and ('mask' not in url['URL'])
                            ):
                                downloadable_links.append(url['URL'])
                    page_num += 1
                    if len(downloadable_links) == len_dl_links or page_num % 10 == 0:
                        print(f"Current page num: {page_num}")
                except Exception as e:
                    warn_log.error(f"Exception received on 'try' statement from within 'while load': {e}")
                    load = False
        return downloadable_links
