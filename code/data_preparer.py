import json
import re
import rasterio

from cmr_connector import CMRConnector
from datetime import datetime
from downloader import Downloader

from lib.utils import extract_bounding_box, mkdir
from rasterio.windows import get_data_window
from rasterio.windows import from_bounds
from rasterio.enums import Resampling

from pyproj import CRS, Transformer

DST_CRS = 'EPSG:4326'

TRANSFORMER = Transformer.from_crs(DST_CRS, "EPSG:32619", always_xy=True)
REVERSE = Transformer.from_crs("EPSG:32619", DST_CRS, always_xy=True)
DATETIME_REGEX = re.compile("\d{8}")


class DataPreparer:
    def __init__(self, collection_id, username, password, folder_name):
        self.collection_id = collection_id
        self.cmr_connector = CMRConnector(collection_id)
        self.downloader = Downloader(username, password)
        self.folder_name = folder_name
        mkdir(self.folder_name)


    def download_and_prepare_clips(self, geojson_file):
        date_time = DATETIME_REGEX.findall(geojson_file)[0]
        date_time = datetime.strptime(date_time, '%Y%m%d').isoformat()
        with open(geojson_file) as geo_file:
            geojson = json.load(geo_file)

        scenes = self.cmr_connector.list_downloadable_links(geojson, date_time)
        downloaded_scenes = self.downloader.download(self.collection_id, scenes)

        self.segment_tiles(geojson, downloaded_scenes)


    def segment_tiles(self, geojson, downloaded_scenes):
        for feature in geojson['features']:
            bounding_box = extract_bounding_box(feature)
            self.find_and_extract_window(bounding_box, downloaded_scenes)


    def find_and_extract_window(self, bounding_box, downloaded_scenes):
        for index, scene in enumerate(downloaded_scenes):
            raster = rasterio.open(scene)
            # bounding_box[0], bounding_box[1] = TRANSFORMER.transform(bounding_box[0], bounding_box[1])
            # bounding_box[2], bounding_box[3] = TRANSFORMER.transform(bounding_box[2], bounding_box[3])
            # test = [0, 0, 0, 0]
            # test[0], test[1] = REVERSE.transform(bounding_box[0], bounding_box[1])
            # test[2], test[3] = REVERSE.transform(bounding_box[2], bounding_box[3])

            if self.check_intersection(raster.bounds, bounding_box):
                print(raster.bounds, bounding_box)
                self.extract_window(raster, bounding_box, index)

            raster.close()


    def check_intersection(self, source_bound, bounding_box):
        return source_bound[0] <= bounding_box[0] and source_bound[1] <= bounding_box[1] and source_bound[2] >= bounding_box[2] and source_bound[3] >= bounding_box[3]


    def extract_window(self, raster, bounding_box, index):
        filename = raster.name.split('/')[-1]

        crop = raster.read(
                1,
                window=from_bounds(*bounding_box, raster.transform)
            )
        profile = raster.profile
        transform = rasterio.transform.from_bounds(*bounding_box, *crop.shape)
        profile.update({
            'height': crop.shape[1],
            'width': crop.shape[1],
            'transform': transform
        })

        with rasterio.open(f"{self.folder_name}/{index}-{filename}", 'w', **profile) as clip:
            clip.write(crop, 1)
