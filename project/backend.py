import sky
from abc import ABC, abstractmethod
import os
import json
from imagery import Imagery
from schemas import Coordinate
from pathlib import Path


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
        temp_path: str,
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
            temp_path (str): temp download path
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
    # 2. Remove Docker for backend_runner (it already gets synced)
    # 3. Explore multi-stage build for backend and xv2 inf engine

    def __init__(self) -> None:
        super().__init__()
        self.provider = "Sky"
        self.ACCELERATORS = {"V100": 1}

    def _make_dag(self, command, gpu=False):
        """Wraps a command into a sky.Dag."""
        print(command)
        with sky.Dag() as dag:
            task = sky.Task(run=command)
            if gpu:
                task.set_resources(sky.Resources(accelerators=self.ACCELERATORS))

        return dag

    def get_code_base(self, repo_url: str):
        cmd = "git clone https://github.com/RitwikGupta/xView2-Vulcan.git && conda create -n xv2 --file xView2-Vulcan/spec-file.txt"

        return self._make_dag(cmd)

    def get_weights(self, url: str, tmp_name: str, out_path: str):
        cmd = f"wget {url} -O {tmp_name}\n"
        cmd += f"mkdir -p {out_path}\n"
        cmd += f"tar -xzvf {tmp_name} -C {out_path} && rm {tmp_name}"

        return self._make_dag(cmd)

    def get_imagery(
        self,
        img_provider: str,
        api_key: str,
        job_id: str,
        image_id: str,
        temp_path: str,
        out_path: str,
        poly_dict: dict,
        pre_post: str,
    ):
        cmd = f"conda run -n xv2_backend python imagery.py --provider {img_provider} --api_key {api_key} --job_id {job_id} --image_id {image_id} --out_path {out_path} --temp_path {temp_path} --pre_post {pre_post} --coordinates '{json.dumps(poly_dict)}'"
        # command that works!
        # conda run -n xv2 python imagery.py --provider Planet --api_key API --job_id 70c560e1-c10e-42e9-b99f-c25310cb4489 --image_id 20211122_205605_ssc14_u0001 --out_path ~/input/pre --temp_path ~/temp --pre_post pre --coordinates '{"start_lon": -84.51025876666456, "start_lat": 39.135462800807794, "end_lon": -84.50162668204827, "end_lat": 39.12701207640838}'
        return self._make_dag(cmd)

    def get_polygons(self):
        pass

    def run_xv(self, pre_dir, post_dir, out_dir, local_mnt):
        cmd = f"cd xView2-Vulcan && conda run -n xv2 python handler.py --pre_directory {pre_dir} --post_directory {post_dir} --output_directory {out_dir} && cp -r {out_dir}/* {local_mnt}"
        return self._make_dag(cmd, gpu=True)

    def launch(
        self,
        s3_bucket: str,
        job_id: str,
        pre_image_id: str,
        post_image_id: str,
        img_provider: str,
        poly_dict: dict,
    ):
        LOCAL_MNT = "/output"
        CLUSTER_NAME = f"xv2-inf-{job_id[-5:]}"

        remote_pre_in_dir = "~/input/pre"
        remote_post_in_dir = "~/input/post"
        remote_poly_dir = "~/input/polys"

        try:
            with sky.Dag() as dag:
                resources = sky.Resources(sky.AWS(), accelerators=self.ACCELERATORS)
                task = sky.Task(run="echo start", workdir=".").set_resources(resources)
                store = sky.Storage(name=s3_bucket)
                store.add_store("S3")
                task.set_storage_mounts({LOCAL_MNT: store})

            sky.launch(
                dag,
                cluster_name=CLUSTER_NAME,
                retry_until_up=True,
                idle_minutes_to_autostop=60,  # Todo: Change autostop time (used currently for debugging)
            )
            # working imagery command:
            # docker run --rm xview2uibackend conda run -n xv2_backend python backend_runner.py --task imagery --api_key API --provider Planet --job_id 70c560e1-c10e-42e9-b99f-c25310cb4489 --image_id 20211122_205605_ssc14_u0001 --coordinates '{"start_lon": -84.51025876666456, "start_lat": 39.135462800807794, "end_lon": -84.50162668204827, "end_lat": 39.12701207640838}' --out_path /Downloads/output --temp_path /temp --pre_post pre

            # pull backend_runner container
            sky.exec(
                self._make_dag(
                    "aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 316880547378.dkr.ecr.us-east-1.amazonaws.com && docker pull 316880547378.dkr.ecr.us-east-1.amazonaws.com/xv2-inf-backend:latest"
                ),
                cluster_name=CLUSTER_NAME,
            )

            # get imagery
            for pre_post in ["pre", "post"]:

                if pre_post == "pre":
                    img_id = pre_image_id
                    remote_dir = remote_pre_in_dir
                else:
                    img_id = post_image_id
                    remote_dir = remote_post_in_dir

                sky.exec(
                    self._make_dag(
                        f"docker run --rm -v {remote_dir}:/output 316880547378.dkr.ecr.us-east-1.amazonaws.com/xv2-inf-backend:latest conda run -n xv2_backend python backend_runner.py imagery --provider {img_provider} --api_key {os.getenv('PLANET_API_KEY')} --job_id {job_id} --image_id {img_id} --coordinates '{json.dumps(poly_dict)}' --out_path /output --temp_path ~/temp --pre_post {pre_post}"
                    ),
                    cluster_name=CLUSTER_NAME,
                )

            # get OSM polygons
            sky.exec(
                self._make_dag(
                    f"docker run --rm -v {remote_poly_dir}:/output 316880547378.dkr.ecr.us-east-1.amazonaws.com/xv2-inf-backend:latest conda run -n xv2_backend python backend_runner.py fetch_polys --job_id {job_id} --coordinates '{json.dumps(poly_dict)}'"
                ),
                cluster_name=CLUSTER_NAME,
            )

            # run xv2
            # Todo: pass OSM polys
            temp_out = "~/output_temp"
            sky.exec(
                self._make_dag(
                    f"docker run --rm --shm-size 56g -v {remote_pre_in_dir}:/input/pre -v {remote_post_in_dir}:/input/post -v {temp_out}:/output -v {remote_poly_dir}:/input/polys --gpus all 316880547378.dkr.ecr.us-east-1.amazonaws.com/xv2-inf-engine:latest --bldg_polys /input/polys/polys.geojson && mv {temp_out} {LOCAL_MNT}/{job_id}",
                    gpu=True,
                ),
                cluster_name=CLUSTER_NAME,
            )

            # # persist results
            # sky.exec(
            #     self._make_dag(
            #         f"docker run --rm -v {remote_output_dir}:/output 316880547378.dkr.ecr.us-east-1.amazonaws.com/xv2-inf-backend:latest conda run -n xv2_backend python backend_runner.py persist_results --job_id {job_id} --geojson {remote_output_dir}/vector/damage.geojson"
            #     ),
            #     cluster_name=CLUSTER_NAME,
            # )

        except:
            pass

        # finally:
        #     # Teardown instance
        #     # See https://github.com/sky-proj/sky/pull/978 for future use of Python API
        #     handle = sky.global_user_state.get_handle_from_cluster_name(CLUSTER)
        #     sky.backends.CloudVmRayBackend().teardown(handle, terminate=True)


# job polling
# In the CLI world, you can poll for the prior jobs (each exec = 1 job) statuses and wait until they are done (sky logs CLUSTER JOB_ID --status). We don’t have a nice API to directly call for this at the moment.
# Utilized to run imagery download on remote instances
