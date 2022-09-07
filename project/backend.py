import sky
from abc import ABC, abstractmethod
import os
import json
import boto3
import geopandas as gpd
from imagery import Imagery
from schemas import Coordinate
from pathlib import Path
from shapely.geometry import Polygon, MultiPolygon
from utils import rdspostgis_sa_client, rdspostgis_client, update_pdb_status


class Backend(ABC):
    def __init__(self) -> None:
        self.provider = None

    @classmethod
    def get_backend(cls, backend: str):
        """Returns appropriate backend class given input string

        Args:
            backend (str): String of backend class to return

        Returns:
            Backend: Backend class
        """
        if backend == "Sky":
            return SkyML()

    @abstractmethod
    def get_imagery(
        self,
        img_provider: str,
        api_key: str,
        job_id: str,
        image_id: str,
        out_path: str,
        poly_dict: dict,
        pre_post: str,
    ):
        """Provides method for backend to retrieve imagery utilizing imagery class download imagery method

        Args:
            img_provider (str): string of imagery provider
            api_key (str): API key for imagery provider
            job_id (str): job ID
            image_id (str): imagery provider image_id
            out_path (str): output imagery path
            poly_dict (dict): dictionary of AOI
            pre_post (str): string indicating "pre" or "post" imagery
        """
        pass

    @abstractmethod
    def launch(
        self,
        s3_bucket: str,
        job_id: str,
        pre_image_id: str,
        post_image_id: str,
        img_provider: str,
        poly_dict: dict,
    ):
        """Launches inference

        Args:
            s3_bucket (str): S3 bucket to output results
            job_id (str): job ID
            pre_image_id (str): pre_image ID
            post_image_id (str): post image ID
            img_provider (str): string representing imagery provider
            poly_dict (dict): dictionary of requested AOI
        """
        pass


class Local(Backend):
    # Todo: create an ansyc local inference backend
    # When complete, simply sky.launch this class on the instance and be done :)
    # Would also allow running on DGX
    def get_imagery(self):
        pass

    def launch(self):
        pass


class SkyML(Backend):
    # Todo:
    # 1. Persist OSM results
    # 2. Add ability to skip OMS polygons (ie. if polygons are no good for an area)
    # 3. Test for OSM polys before passing to inf engine
    # 4. Explore multi-stage Docker builds with Conda-pack (https://pythonspeed.com/articles/conda-docker-image-size/)

    def __init__(self) -> None:
        super().__init__()
        self.provider = "Sky"
        self.ACCELERATORS = {"V100": 4}
        self.LOCAL_MNT = "/home/ubuntu/output"

        self.remote_pre_in_dir = "/home/ubuntu/input/pre"
        self.remote_post_in_dir = "/home/ubuntu/input/post"
        self.remote_poly_dir = "/home/ubuntu/input/polys"
        self.remote_temp_out = "/home/ubuntu/output_temp"

    def _make_task(self, command, gpu=False, is_detach=False):
        """Wraps a command into a sky.Dag."""
        print(command)  # Debug: remove for production perhaps
        with sky.Dag() as dag:
            task = sky.Task(run=command)
            if gpu:
                task.set_resources(sky.Resources(accelerators=self.ACCELERATORS))

        return sky.exec(dag, cluster_name=self.cluster_name, detach_run=is_detach)

    def get_imagery(
        self,
        img_provider: str,
        api_key: str,
        job_id: str,
        image_id: str,
        out_path: str,
        poly_dict: dict,
        pre_post: str,
    ):
        # # get imagery
        # for pre_post in ["pre", "post"]:

        #     if pre_post == "pre":
        #         img_id = self.pre_image_id
        #         remote_dir = self.remote_pre_in_dir
        #     else:
        #         img_id = self.post_image_id
        #         remote_dir = self.remote_post_in_dir

        #     self._make_task(
        #         f"docker run --rm -v {remote_dir}:/output 316880547378.dkr.ecr.us-east-1.amazonaws.com/xv2-inf-backend:latest python backend_runner.py imagery --provider {img_provider} --api_key {os.getenv('PLANET_API_KEY')} --job_id {job_id} --image_id {img_id} --coordinates '{json.dumps(poly_dict)}' --out_path /output --pre_post {pre_post}"
        #     )
        pass

    def launch(
        self,
        s3_bucket: str,
        job_id: str,
        pre_image_id: str,
        post_image_id: str,
        img_provider: str,
        img_api_key: str,
        poly_dict: dict,
        get_osm: bool,
    ):

        # set this to be used in task creation
        self.cluster_name = f"xv2-inf-{job_id[-5:]}"

        try:
            with sky.Dag() as dag:
                resources = sky.Resources(sky.AWS(), accelerators=self.ACCELERATORS)
                task = sky.Task(run="echo start", workdir=".").set_resources(resources)
                store = sky.Storage(name=s3_bucket)
                store.add_store("S3")
                task.set_storage_mounts({self.LOCAL_MNT: store})

            sky.launch(
                dag,
                cluster_name=self.cluster_name,
                retry_until_up=True,
                idle_minutes_to_autostop=20,  # Todo: Change autostop time (used currently for debugging)
            )

            # pull containers
            self._make_task(
                "aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 316880547378.dkr.ecr.us-east-1.amazonaws.com"
            )
            self._make_task(
                "docker pull 316880547378.dkr.ecr.us-east-1.amazonaws.com/xv2-inf-engine:latest",
                is_detach=True,
            )
            self._make_task(
                "docker pull 316880547378.dkr.ecr.us-east-1.amazonaws.com/xv2-inf-backend:latest"
            )

            # get imagery
            for pre_post in ["pre", "post"]:

                if pre_post == "pre":
                    img_id = pre_image_id
                    remote_dir = self.remote_pre_in_dir
                else:
                    img_id = post_image_id
                    remote_dir = self.remote_post_in_dir

                self._make_task(
                    f"docker run --rm -v {remote_dir}:/output 316880547378.dkr.ecr.us-east-1.amazonaws.com/xv2-inf-backend:latest python backend_runner.py imagery --provider {img_provider} --api_key {img_api_key} --job_id {job_id} --image_id {img_id} --coordinates '{json.dumps(poly_dict)}' --out_path /output --pre_post {pre_post}"
                )

            # create list for extra args
            inf_xtra_args = []

            # get OSM polygons
            if get_osm:
                self._make_task(
                    f"docker run --rm -v {self.remote_poly_dir}:/output 316880547378.dkr.ecr.us-east-1.amazonaws.com/xv2-inf-backend:latest python backend_runner.py fetch_polys --job_id {job_id} --coordinates '{json.dumps(poly_dict)}'"
                )
                
                inf_xtra_args.append("--bldg_polys /input/polys/polys.geojson")

            # run xv2
            self._make_task(
                # Todo: currently skips using bldg_polys
                f"docker run --rm --gpus all --shm-size 56g -v {self.remote_pre_in_dir}:/input/pre -v {self.remote_post_in_dir}:/input/post -v {self.remote_temp_out}:/output -v {self.remote_poly_dir}:/input/polys 316880547378.dkr.ecr.us-east-1.amazonaws.com/xv2-inf-engine:latest --dp_mode {' '.join(inf_xtra_args)}",  # BUG: Bug in inference engine does not produce correct outputs with 4 GPUs unless run in dp_mode. Adding flag as stopgap
                gpu=True,
            )

            # Debug: check checksum before pussing to S3 to check Sky S3 storage
            self._make_task(f"md5sum {self.remote_temp_out}/mosaics/overlay.tif")

            # move output to S3 mount
            self._make_task(
                f"mkdir {self.LOCAL_MNT}/{job_id} && sudo cp -r {self.remote_temp_out}/* {self.LOCAL_MNT}/{job_id}"
            )

        except:
            pass

        else:
            # persist results
            s3 = boto3.resource("s3")

            json_content = json.loads(
                s3.Object(s3_bucket, f"{job_id}/vector/damage.geojson")
                .get()["Body"]
                .read()
                .decode("utf-8")
            )

            gdf = gpd.GeoDataFrame.from_features(
                json_content, crs=json_content.get("crs").get("properties").get("name")
            )
            gdf["uid"] = job_id
            gdf = gdf.to_crs(4326)

            gdf["geometry"] = [
                MultiPolygon([feature]) if isinstance(feature, Polygon) else feature
                for feature in gdf["geometry"]
            ]

            # if we don't have polygons, we don't get the osmid column
            if not "osmid" in gdf.columns:
                gdf["osmid"] = None

            # get rid of extraneous columns such as 'filename' that gets created if we don't use polygons
            gdf = gdf[["geometry", "osmid", "dmg", "area", "uid"]]

            # Push results to Postgres
            engine = rdspostgis_sa_client()
            gdf.to_postgis("xviewui_results", engine, if_exists="append")

            # Update job status
            conn = rdspostgis_client()
            update_pdb_status(conn, job_id, "done")

        # teardown instance
        finally:
            sky.down(self.cluster_name)


# job polling
# In the CLI world, you can poll for the prior jobs (each exec = 1 job) statuses and wait until they are done (sky logs CLUSTER JOB_ID --status). We don’t have a nice API to directly call for this at the moment.
# Utilized to run imagery download on remote instances
