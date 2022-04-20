import numpy as np
import os

WORLD_BOUNIDNG_BOX = [0, 0, 0, 0]

def extract_minimum_bounding_box(geojson):
    bounding_box = WORLD_BOUNIDNG_BOX
    for feature in geojson['features']:
        internal_bounding_box = extract_bounding_box(feature)
        bounding_box[0] = bounding_box[0] if bounding_box[0] < internal_bounding_box[0] else internal_bounding_box[0]
        bounding_box[1] = bounding_box[1] if bounding_box[1] < internal_bounding_box[1] else internal_bounding_box[1]
        bounding_box[2] = bounding_box[2] if bounding_box[2] > internal_bounding_box[2] else internal_bounding_box[2]
        bounding_box[3] = bounding_box[3] if bounding_box[3] > internal_bounding_box[3] else internal_bounding_box[3]
    return bounding_box


def extract_bounding_box(feature):
    coordinates = np.asarray(feature['geometry']['coordinates'])[0]
    lats, lons = sorted(coordinates[:, 1]), sorted(coordinates[:, 0])
    return [lons[0], lats[0], lons[-1], lats[-1]]


def mkdir(dirname):
    if os.path.exists(dirname):
        print(f'Folder {dirname} already exists.')
    else:
        os.mkdir(dirname)
