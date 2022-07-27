import sky
import textwrap
from abc import ABC, abstractmethod
import imagery


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


class SkyML(Backend):

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

    def get_imagery(self, imagery: list, out_path: str):
        cmd = ""
        for i in imagery:
            cmd += f"wget {i} -P {out_path} &"

    def launch(
        self,
        s3_bucket: str,
        job_id: str,
        pre_image_id: str,
        post_image_id: str,
        img_provider: str
    ):
        LOCAL_MNT = "/output"
        PRE_PATH = "~/input/pre"
        POST_PATH = "~/input/post"

        SETUP_CMD = (
            """\
            # Exit if error occurs
            set -ex

            install() {
                git clone https://github.com/RitwikGupta/xView2-Vulcan.git
                conda create -n xv2 --file xView2-Vulcan/spec-file.txt
            }

            fetch_weights() {
                wget https://xv2-weights.s3.amazonaws.com/first_place_weights.tar.gz
                mkdir xView2-Vulcan/weights
                tar -xzvf first_place_weights.tar.gz -C xView2-Vulcan/weights && rm first_place_weights.tar.gz
            }

            fetch_backbone_weights() {
                mkdir -p ~/.cache/torch/hub/checkpoints
                wget https://xv2-weights.s3.amazonaws.com/backbone_weights.tar.gz
                tar -xzvf backbone_weights.tar.gz -C ~/.cache/torch/hub/checkpoints/ && rm backbone_weights.tar.gz
            }

            fetch_pre_imagery() {
                conda activate xv2 && python -c 'from imagery import Imagery;from utils import create_bounding_box_poly; from main import fetch_coordinates; poly=create_bounding_box_poly(fetch_coordinates({job_id}));cls=Imagery.get_provider("Sky");cls.download_imagery_helper({job_id}, "pre", {pre_image_id}, {polygon}, "/temp", {PRE_PATH})'
            }

            fetch_post_imagery() {
                conda activate xv2 && mkdir -p {POST_PATH} && {POST_IMG_CMD}
            }

            # Run the function in the background for parallel execution
            install &
            fetch_weights &
            fetch_backbone_weights &
            wait
            """.replace(
                "{PRE_PATH}", PRE_PATH
            )
            .replace("{POST_PATH}", POST_PATH)
            .replace("{job_id}", job_id)
            .replace("{pre_image_id}", pre_image_id)
            .replace("{PRE_PATH}", PRE_PATH) 
        )

        SETUP = textwrap.dedent(SETUP_CMD)

        RUN_CMD = """\
            wait
            cd xView2-Vulcan
            conda run -n xv2 python handler.py --pre_directory ~/input/pre --post_directory ~/input/post --output_directory ~/output_temp/jobid
            cp -r ~/output_temp/* {local_mnt}
            """

        RUN = textwrap.dedent(RUN_CMD)

        # SETUP = "echo setup"
        # RUN = "echo run"

        try:
            with sky.Dag() as dag:
                resources = sky.Resources(sky.AWS(), accelerators=self.ACCELERATORS)
                task = sky.Task(setup=SETUP, workdir=".").set_resources(resources)
                store = sky.Storage(name=s3_bucket)
                store.add_store("S3")
                task.set_storage_mounts({LOCAL_MNT: store})

            sky.launch(
                dag,
                cluster_name=f"xv2-inf-{job_id[-5:]}",
                retry_until_up=True,
                idle_minutes_to_autostop=10
            )

            sky.exec(self._make_dag(RUN, gpu=True), cluster_name=self.CLUSTER)

        except:
            pass

        # finally:
        #     # Teardown instance
        #     # See https://github.com/sky-proj/sky/pull/978 for future use of Python API
        #     handle = sky.global_user_state.get_handle_from_cluster_name(CLUSTER)
        #     sky.backends.CloudVmRayBackend().teardown(handle, terminate=True)


# job polling
# In the CLI world, you can poll for the prior jobs (each exec = 1 job) statuses and wait until they are done (sky logs CLUSTER JOB_ID --status). We don’t have a nice API to directly call for this at the moment.
