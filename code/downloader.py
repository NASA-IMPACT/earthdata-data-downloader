import os
import rasterio

from tqdm import tqdm

from lib.utils import mkdir
from lib.session_with_header_redirection import SessionWithHeaderRedirection
from rasterio.warp import calculate_default_transform, reproject, Resampling


DST_CRS = 'EPSG:4326'


class Downloader:
    def __init__(self, username, password):
        self.session = SessionWithHeaderRedirection(username, password)

    def download(self, collection_id, links):
        downloaded_scenes = list()
        mkdir(collection_id)
        for link in tqdm(links):
            filename = f"{collection_id}/{link[link.rfind('/')+1:]}"
            try:
                response = self.session.get(link, stream=True)
                response.raise_for_status()
                with open(filename, 'wb') as fd:
                    for chunk in response.iter_content(chunk_size=1024*1024):
                        fd.write(chunk)
                updated_filename = self.reproject(filename)
                downloaded_scenes.append(updated_filename)
                os.remove(filename)
            except Exception as e:
                print(f"Couldn't download link: {link}, Status Code: {response.status_code}, {e}")
                continue
        return downloaded_scenes


    def reproject(self, filename):
        updated_filename = f"{filename.replace('.tif', '_4326.tif')}"
        with rasterio.open(filename) as raster:
            transform, width, height = calculate_default_transform(
                raster.crs, DST_CRS, raster.width, raster.height, *raster.bounds
            )
            kwargs = raster.meta.copy()
            kwargs.update({
                'crs': DST_CRS,
                'transform': transform,
                'width': width,
                'height': height
            })

            with rasterio.open(updated_filename, 'w', **kwargs) as dst:
                for i in range(1, raster.count + 1):
                    reproject(
                        source=rasterio.band(raster, i),
                        destination=rasterio.band(dst, i),
                        raster_transform=raster.transform,
                        raster_crs=raster.crs,
                        dst_transform=transform,
                        dst_crs=DST_CRS,
                        resampling=Resampling.nearest)
        return updated_filename