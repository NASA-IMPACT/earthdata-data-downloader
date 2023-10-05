import numpy as np
import requests

from datetime import datetime
from lib.utils import extract_minimum_bounding_box

CMR_URL = "https://cmr.earthdata.nasa.gov/search/granules.umm_json?collection_concept_id={collection_id}&temporal[]={start_datetime}&temporal[]={end_datetime}&page_size=2000"
QUERY = ""
# HLS L30 = C2021957657-LPCLOUD
# HLS S30 = C2021957295-LPCLOUD
COLLECTIONS = ['C2021957657-LPCLOUD', 'C2021957295-LPCLOUD']


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
            break

    def list_downloadable_links(self):
        # pass json file to download
        # query cmr based on the dates
        downloadable_links = list()
        for cmr_url in self.cmr_urls:
            response = requests.get(cmr_url)
            response.raise_for_status()

            for item in response.json()['items']:
                for url in item['umm']['RelatedUrls']:
                    if (
                        ('s3:' not in url['URL'])
                        and ('.tif' in url['URL'])
                        # and ('mask' not in url['URL'])
                    ):
                        downloadable_links.append(url['URL'])
        return downloadable_links
