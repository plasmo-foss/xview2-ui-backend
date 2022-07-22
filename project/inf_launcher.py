import sky
import textwrap

# Todo: move this to a config file (.env)
ACCELERATORS = {"V100": 1}
S3_BUCKET = "xv2-outputs"
LOCAL_SOURCE = "~/output"
CLUSTER = "xv2test"

SETUP = textwrap.dedent("""\
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

    # Run the function in the background for parallel execution
    install &
    fetch_weights &
    fetch_backbone_weights &
    wait
    """)

RUN = textwrap.dedent("""\
    # download imagery
    mkdir -p ~/input/pre && wget https://xv2-weights.s3.amazonaws.com/pre_mosaic.tif -P ~/input/pre/ &
    mkdir -p ~/input/post && wget https://xv2-weights.s3.amazonaws.com/post_mosaic.tif -P ~/input/post &
    wait
    cd xView2-Vulcan
    conda run -n xv2 python handler.py --pre_directory ~/input/pre --post_directory ~/input/post --output_directory ~/output_temp/jobid
    cp -r ~/output_temp/* ~/output
    """)


def make_dag(command, gpu=False):
    """Wraps a command into a sky.Dag."""
    with sky.Dag() as dag:
        task = sky.Task(run=command)
        if gpu:
            task.set_resources(sky.Resources(accelerators=ACCELERATORS))

    return dag


def inf_launch():
    try:
        with sky.Dag() as dag:
            resources = sky.Resources(sky.AWS(), accelerators=ACCELERATORS)
            task = sky.Task(setup=SETUP).set_resources(resources)
            store = sky.Storage(name=S3_BUCKET)
            store.add_store("S3")
            task.set_storage_mounts({LOCAL_SOURCE: store})

        sky.launch(
            dag, cluster_name=CLUSTER, retry_until_up=True, idle_minutes_to_autostop=20,
        )

        sky.exec(make_dag(RUN, gpu=True), cluster_name=CLUSTER)

    except:
        pass

    finally:
        # Teardown instance
        # See https://github.com/sky-proj/sky/pull/978 for future use of Python API
        handle = sky.global_user_state.get_handle_from_cluster_name(CLUSTER)
        sky.backends.CloudVmRayBackend().teardown(handle, terminate=True)


# job polling
# In the CLI world, you can poll for the prior jobs (each exec = 1 job) statuses and wait until they are done (sky logs CLUSTER JOB_ID --status). We don’t have a nice API to directly call for this at the moment.
