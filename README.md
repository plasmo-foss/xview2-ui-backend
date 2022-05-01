# xView2 UI Backend

This is the code for the backend that supports the UI at [https://github.com/plasmo-foss/no-war-pls](https://github.com/plasmo-foss/no-war-pls).

## Install
```
pip install "fastapi[all]"
conda install boto3
```

## Running
```
sudo /home/ubuntu/miniconda3/envs/xview2-ui-backend/bin/uvicorn main:app --reload --port 80 --host 0.0.0.0
```

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