"""Database utilities"""
from dotenv import load_dotenv
from schemas.coordinate import Coordinate
import os
import boto3

def awsddb_client():
    load_dotenv(override=True)

    return boto3.resource(
        "dynamodb",
        region_name=os.getenv("DB_REGION_NAME"),
        aws_access_key_id=os.getenv("DB_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("DB_SECRET_ACCESS_KEY"),
        endpoint_url=os.getenv("DB_ENDPOINT_URL"),
    )

ddb = awsddb_client()

def get_coordinates(job_id):
    resp = ddb.Table("xview2-ui-coordinates").get_item(Key={"uid": job_id})

    if "Item" in resp:
        ret = resp["Item"]
        return Coordinate(
            start_lon=ret["start_lon"],
            start_lat=ret["start_lat"],
            end_lon=ret["end_lon"],
            end_lat=ret["end_lat"],
        )
    else:
        return None