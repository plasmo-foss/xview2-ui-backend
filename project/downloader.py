import requests
# Todo: try to find replacement for cv2...it's notorious for screwing every environment
import cv2
import shapely
import mercantile
import threading
from queue import Queue
import numpy as np
import rasterio.merge


# Todo: integrate this to Imagery class
class TileDataset:
    def __init__(self, url, output_dir, bounding_box, zoom, job_id):
        self.subdomains = ['tiles0', 'tiles1', 'tiles2', 'tiles3']
        self.url = url
        self.output_dir = output_dir
        self.bounding_box = bounding_box
        self.zoom = zoom
        self.job_id = job_id

    def _get_image_from_tile(self, tile):
        """
        Args:
            tile: a mercantile Tile object
        Returns
            a np.ndarray of size 256x256x3 with uint8 datatype containing the imagery
                for the input tile
        """
        url = self.url.format(
            subdomain=np.random.choice(self.subdomains),
            x=tile.x,
            y=tile.y,
            z=tile.z
        )
        with requests.get(url) as r:
            arr = np.asarray(bytearray(r.content), dtype=np.uint8)
        img = cv2.imdecode(arr, -1)
        img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

        return img

    def _get_tile_as_virtual_raster(self, tile):
        """
        Args:
            tile: a mercantile Tile object
        Returns
            a rasterio.io.MemoryFile with the imagery for the input tile
        """
        img = self._get_image_from_tile(tile)
        geom = shapely.geometry.shape(mercantile.feature(tile)["geometry"])
        minx, miny, maxx, maxy = geom.bounds
        dst_transform = rasterio.transform.from_bounds(minx, miny, maxx, maxy, 256, 256)
        dst_profile = {
            "driver": "GTiff",
            "width": 256,
            "height": 256,
            "transform": dst_transform,
            "nodata": 0,
            "crs": "epsg:4326",
            "count": 3,
            "dtype": "uint8",
        }
        test_f = rasterio.io.MemoryFile()
        with test_f.open(**dst_profile) as test_d:
            test_d.write(img[:, :, 0], 1)
            test_d.write(img[:, :, 1], 2)
            test_d.write(img[:, :, 2], 3)
        test_f.seek(0)

        return test_f

    def _dequeue_get_tile_as_virtual_raster(self, q, virtual_files, virtual_datasets):
        while not q.empty():
            tile = q.get()
            f = self._get_tile_as_virtual_raster(tile)
            virtual_files.append(f)
            virtual_datasets.append(f.open())
            q.task_done()

    def get_data_from_extent(self, geom, zoom_level=16):
        """Gets georeferenced imagery from the input geom at a given zoom level.
        Specifically, this will iterate over all the quadkeys in the input geom at the
        given zoom level and save the imagery as a virtual raster. The virtual raster
        lets us either quickly read the data or quickly write it to file as a GeoTIFF.
        Args:
            geom: A geojson object in EPSG:4326 (i.e. with lat/lon coordinates)
        Returns:
            a rasterio.io.MemoryFile with the corresponding data
        """
        shape = shapely.geometry.shape(geom)
        minx, miny, maxx, maxy = shape.bounds

        virtual_files = []
        virtual_datasets = []

        output_width_degrees = maxx-minx
        output_height_degrees = maxy-miny
        if output_width_degrees > 1 or output_height_degrees > 1:
            raise ValueError(
                "Trying to export file with height or width larger than a degree which"
                + " will result in a huge output tile. The input geom should be split"
                + " up into smaller chunks."
            )

        # TODO: This should _probably_ be done multithreaded
        # for tile in mercantile.tiles(minx, miny, maxx, maxy, zoom_level):
        #     f = self._get_tile_as_virtual_raster(tile)
        #     virtual_files.append(f)
        #     virtual_datasets.append(f.open())

        tile_queue = Queue()
        num_threads = 4
        num_tiles = 0
        for tile in mercantile.tiles(minx, miny, maxx, maxy, zoom_level):
            tile_queue.put(tile)
            num_tiles += 1

        print(f"Fetching {num_tiles} tiles...")

        for i in range(num_threads):
            thread = threading.Thread(
                target = self._dequeue_get_tile_as_virtual_raster,
                args=(tile_queue, virtual_files, virtual_datasets)
            )
            thread.start()

        tile_queue.join()

        # get the largest x and y resolutions over all patches to use as the merged tile
        # resolution if we don't explicitely set this, then it is likely that some of
        # the patches not have the correct resolution (they will be off by tiny
        # fractions of a degree) and there will be single nodata lines between rows of
        # tiles
        x_res = 0
        y_res = 0
        for ds in virtual_datasets:
            x_res = max(x_res, ds.res[0])
            y_res = max(y_res, ds.res[1])

        out_image, out_transform = rasterio.merge.merge(
            virtual_datasets,
            res=(x_res, y_res),
            bounds=(minx, miny, maxx, maxy),
        )

        for ds in virtual_datasets:
            ds.close()
        for f in virtual_files:
            f.close()

        dst_crs = "epsg:4326"
        dst_profile = {
            "driver": "GTiff",
            "width": out_image.shape[2],
            "height": out_image.shape[1],
            "transform": out_transform,
            "crs": dst_crs,
            "count": 3,
            "dtype": "uint8",
        }
        test_f = rasterio.io.MemoryFile()
        with test_f.open(**dst_profile) as test_d:
            test_d.write(out_image)
        test_f.seek(0)

        return test_f

    def save_memory_file_to_disk(self, memory_file, prepost):
        with memory_file.open() as src:
            profile = src.profile.copy()
            profile["compress"] = "lzw",
            profile["predictor"] = 2
            output_path = self.output_dir / self.job_id / prepost / f"{self.job_id}_{prepost}_merged.tif"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with rasterio.open(output_path, "w", **profile) as dst:
                dst.write(src.read())