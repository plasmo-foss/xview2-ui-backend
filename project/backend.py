import sky
import textwrap
from abc import ABC, abstractmethod
import imagery
import os
import json
from shapely.geometry import Polygon


class Backend(ABC):
    def __init__(self) -> None:
        self.provider = None

    @classmethod
    def get_backend(cls, backend: str):
        if backend == "Sky":
            return SkyML()

    @abstractmethod
    def get_imagery():
        pass

    @abstractmethod
    def launch(self):
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
    # Todo: explore the ability to pass these methods to celery ie:
    # cmd = (prepare, get_repo, get_imagery, get_polys) | run_xv; cmd.apply_async()

    def __init__(self) -> None:
        super().__init__()
        self.provider = "Sky"
        self.ACCELERATORS = {"V100": 1}


    def _make_dag(self, command, gpu=False):
        """Wraps a command into a sky.Dag."""
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
        cmd = f"conda run -n xv2 python imagery.py --provider {img_provider} --api_key {api_key} --job_id {job_id} --image_id {image_id} --out_path {out_path} --temp_path {temp_path} --pre_post {pre_post} --coordinates {json.dumps(poly_dict).replace(' ', '')}"
        # command that works!
        # conda run -n xv2 python imagery.py --provider Planet --api_key PLAKcc6a392a192d41de8ed39504826419ec --job_id 70c560e1-c10e-42e9-b99f-c25310cb4489 --image_id 20211122_205605_ssc14_u0001 --out_path ~/input/pre --temp_path ~/temp --pre_post pre --coordinates '{"start_lon":-84.51025876666456,"start_lat":39.135462800807794,"end_lon":-84.50162668204827,"end_lat":39.12701207640838}'
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
                idle_minutes_to_autostop=60, # Todo: Change autostop time (used currently for debugging)
            )

            sky.exec(self.get_code_base("https://github.com/RitwikGupta/xView2-Vulcan.git"), cluster_name=CLUSTER_NAME)
            # sky.exec(self.get_weights("https://xv2-weights.s3.amazonaws.com/first_place_weights.tar.gz", "~/fp_weights.tar.gz", "xView2-Vulcan/weights"), cluster_name=CLUSTER_NAME)
            # sky.exec(self.get_weights("https://xv2-weights.s3.amazonaws.com/backbone_weights.tar.gz", "~/backbone_weights.tar.gz", "~/.cache/torch/hub/checkpoints/"), cluster_name=CLUSTER_NAME)
            
            # get imagery
            for i in ["pre", "post"]:

                if i == "pre":
                    img_id = pre_image_id
                else:
                    img_id = post_image_id
            
                sky.exec(
                    self.get_imagery(
                        img_provider,
                        os.getenv("PLANET_API_KEY"),
                        job_id,
                        img_id,
                        "/temp",
                        f"~/input/{i}",
                        poly_dict,
                        i,
                    ),
                    cluster_name=CLUSTER_NAME,
                )

            #run xv2
            # Todo: get OSM polys
            sky.exec(self.run_xv("~/input/pre", "~/input/post", "~/output_temp", LOCAL_MNT))


        except:
            pass

        # finally:
        #     # Teardown instance
        #     # See https://github.com/sky-proj/sky/pull/978 for future use of Python API
        #     handle = sky.global_user_state.get_handle_from_cluster_name(CLUSTER)
        #     sky.backends.CloudVmRayBackend().teardown(handle, terminate=True)


# job polling
# In the CLI world, you can poll for the prior jobs (each exec = 1 job) statuses and wait until they are done (sky logs CLUSTER JOB_ID --status). We don’t have a nice API to directly call for this at the moment.
