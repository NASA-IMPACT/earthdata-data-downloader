import os
import rasterio

from tqdm import tqdm

from lib.utils import mkdir
from lib.session_with_header_redirection import SessionWithHeaderRedirection
from rasterio.warp import calculate_default_transform, reproject, Resampling


class Downloader:
    def __init__(self, username, password):
        self.session = SessionWithHeaderRedirection(username, password)

    def download_for(self, collection_id, links, tiles_info, split):
        mkdir(collection_id)
        downloaded_scenes = list()
        for link in tqdm(links):
            filename = link[link.rfind('/') + 1 :]
            file_path = f"{collection_id}/{filename}"
            tile_id, datetime = filename.split('.')[2:4]
            date = datetime.split('T')[0]
            if not ('.2017' in link):
                print(link)
                continue
            else:
                print(link)
            if date in tiles_info[split]:
                # import code

                # code.interact(local=dict(globals(), **locals()))

                for download_id in tiles_info[split][date]:
                    if download_id in filename:
                        try:
                            if not (os.path.isfile(file_path)):
                                response = self.session.get(link, stream=True)
                                response.raise_for_status()
                                with open(file_path, 'wb') as fd:
                                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                                        fd.write(chunk)
                            downloaded_scenes.append(
                                {
                                    'file_name': file_path,
                                    'date': date,
                                    'download_id': download_id,
                                    'indices': tiles_info[split][date][download_id],
                                }
                            )
                        except Exception as e:
                            print(f"Couldn't download link: {link}, {e}")
                            continue
        return downloaded_scenes
