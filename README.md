# xView2 UI Backend

This is the code for the backend that supports the UI at [https://github.com/louisgv/no-war-pls](https://github.com/louisgv/no-war-pls).

## Install
```
pip install "fastapi[all]"
conda install boto3
```

## Running
```
sudo /home/ubuntu/miniconda3/envs/xview2-ui-backend/bin/uvicorn main:app --reload --port 80 --host 0.0.0.0
```