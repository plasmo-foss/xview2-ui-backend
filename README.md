# xView2 UI Backend

This is the code for the backend that supports the UI at [https://github.com/plasmo-foss/no-war-pls](https://github.com/plasmo-foss/no-war-pls).

## Install
### Conda
Conda is a requirement for running the backend. Ensure it is installed, the from terminal, navigate to the backend project that you cloned earlier.
```
cd xview2-ui-backend/project
conda env create -f environment.yml
conda activate xv2_backend
pip install -r requirements
```
the above installed the conda requirements, activated the conda environment, then installed the fastapi requirements.

### .env
You need a file named `.env` containing AWS and Planet API credentials and a file named `.env.access_keys`. Ask the group for these files.
These files will live in the xview2-ui-backend/project folder

## Running
Once conda is setup, we can run the project. It consists of three parts. The API server, celery worker and celery flower server.

In three different terminal windows, cd to xview2-ui-backend/project, activate conda and run:

```
uvicorn main:app --reload --port 80 --host 0.0.0.0
```

```
celery --app=worker.celery worker --loglevel=info --logfile=logs/celery.log
```

```
celery --app=worker.celery flower --port=5555
```

In your browser goto:
* celery flower: http://localhost:5555/
* api docs: http://localhost/docs


### xView2-Vulcan-Model setup
currently we are running production on branch "ms_model"

## State Diagram
```
+--------------------+
|                    |
|  Send coordinates  |
|                    |
+----------+---------+
           |
           | waiting_imagery
           |
+----------+---------+
|                    |
|     Get imagery    |
|                    |
+----------+---------+
           |
           | waiting_assessment
           |
+----------+---------+
|                    |
|  AI assess submit  |
|                    |
+----------+---------+
           |
           | running_assessment
           |
+----------+---------+
|                    |
| AI assess complete |
|                    |
+----------+---------+
           |
           | waiting_osm_clip
           |
+----------+---------+
|                    |
|      Clip OSM      |
|                    |
+----------+---------+
           |
           | done
           |
+----------+---------+
|                    |
|        Done        |
|                    |
+--------------------+
```
