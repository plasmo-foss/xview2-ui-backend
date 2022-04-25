import json
import os
import uuid
from decimal import Decimal
from typing import Optional

import boto3
from dotenv import load_dotenv
from fastapi import FastAPI

from schemas import Coordinate

app = FastAPI()

client = None
ddb = None

@app.on_event("startup")
async def startup_event():
    global ddb

    conf = load_dotenv()
    client = boto3.client('dynamodb',
                         region_name=os.getenv('DB_REGION_NAME'),
                         aws_access_key_id=os.getenv('DB_ACCESS_KEY_ID'),
                         aws_secret_access_key=os.getenv('DB_SECRET_ACCESS_KEY')
    )
    ddb = boto3.resource('dynamodb',
                         region_name=os.getenv('DB_REGION_NAME'),
                         aws_access_key_id=os.getenv('DB_ACCESS_KEY_ID'),
                         aws_secret_access_key=os.getenv('DB_SECRET_ACCESS_KEY')
    )
    ddb_exceptions = client.exceptions


@app.post("/send-coordinates")
async def send_coordinates(coordinate: Coordinate):
    uid = uuid.uuid4()
    item = json.loads(coordinate.json(), parse_float=Decimal)
    ddb.Table('xview2-ui-job').put_item(
        Item={
            'uid': str(uid),
            **item
        }
    )

    return uid

@app.get("/job-status")
async def job_status(job_id: str):
    resp = ddb.Table('xview2-ui-inference').get_item(
        Key={
            'uid': job_id
        }
    )

    if "Item" in resp:
        return resp["Item"]
    else:
        return None
