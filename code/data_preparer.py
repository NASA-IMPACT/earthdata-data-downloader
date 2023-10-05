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

WINDOW_SIZE = 224


class DataPreparer:
    def __init__(self, collection_id, username, password, folder_name, tiles_info_filename, split):
        self.collection_id = collection_id
        with open(tiles_info_filename) as tiles_file:
            self.tiles_info = json.load(tiles_file)
        self.split = split
        self.cmr_connector = CMRConnector(collection_id, self.tiles_info, split)
        self.downloader = Downloader(username, password)
        self.folder_name = folder_name
        mkdir(self.folder_name)

    def download_and_prepare_clips(self):
        scenes = self.cmr_connector.list_downloadable_links()
        downloaded_scenes = self.downloader.download_for(
            self.collection_id, scenes, self.tiles_info, self.split
        )

        self.segment_tiles(downloaded_scenes)

    def segment_tiles(self, downloaded_scenes):
        for downloaded_scene in downloaded_scenes:
            self.find_and_extract_window(downloaded_scene)

    def find_and_extract_window(self, downloaded_scene):
        raster = rasterio.open(downloaded_scene['file_name'])
        for index, indices in enumerate(downloaded_scene['indices']):
            self.extract_window(raster, downloaded_scene, indices)

        raster.close()

    def extract_window(self, raster, download_scene, indices):
        filename = raster.name.split('/')[-1]
        band_id = filename.split('.')[-2]
        foldername = f"{self.folder_name}/{download_scene['download_id']}_{download_scene['date']}_{indices[0]}_{indices[1]}"
        mkdir(foldername)
        filename = f"{foldername}/{band_id}.tif"
        indices = [int(indices[0]), int(indices[1])]
        window = rasterio.windows.Window(
            indices[0], indices[1], indices[0] + WINDOW_SIZE, indices[1] + WINDOW_SIZE
        )
        win_transform = raster.window_transform(window)
        updated_profile = raster.meta.copy()
        updated_profile.update(
            {'transform': win_transform, 'width': WINDOW_SIZE, 'height': WINDOW_SIZE}
        )

        kwargs = raster.meta.copy()
        kwargs.update(
            {
                'height': window.height,
                'width': window.width,
                'transform': rasterio.windows.transform(window, raster.transform),
            }
        )

        with rasterio.open(filename, 'w', **updated_profile) as dst:
            dst.write(raster.read(window=window))
