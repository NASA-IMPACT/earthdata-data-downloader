import numpy as np
import requests

from lib.utils import extract_minimum_bounding_box

CMR_URL = "https://cmr.earthdata.nasa.gov/search/granules.umm_json?collection_concept_id={collection_id}"
QUERY = "&bounding_box={bounding_box}&temporal[]={datetime}"


class CMRConnector:
    def __init__(self, collection_id=None, creds_file=None):
        self.collection_id = collection_id
        self.cmr_url = CMR_URL.format(collection_id=self.collection_id)


    def list_downloadable_links(self, geojson, date_time):
        bounding_box = extract_minimum_bounding_box(
                geojson
            )
        bounding_box = ','.join([str(coord) for coord in bounding_box])

        print(f"Querying CMR for bounding_box: {bounding_box}, date time: {date_time}")

        response = requests.get(
                f"{self.cmr_url}{QUERY.format(bounding_box=bounding_box, datetime=date_time)}"
            )
        response.raise_for_status()

        downloadable_links = list()

        for item in response.json()['items']:
            for url in item['umm']['RelatedUrls']:
                # specific to HLS, needs to be modified for other data.
                if ('s3:' not in url['URL']) and ('.tif' in url['URL']) and ('mask' not in url['URL']):
                    downloadable_links.append(url['URL'])
        return downloadable_links


