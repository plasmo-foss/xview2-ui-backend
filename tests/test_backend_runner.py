import pytest


class TestDownloadImagery:
    def test_fetch_imagery(self):
        # working command to pull imagery
        # python backend_runner.py --task imagery --api_key API_KEY --provider Planet --job_id 70c560e1-c10e-42e9-b99f-c25310cb4489 --image_id 20211122_205605_ssc14_u0001 --coordinates '{"start_lon": -84.51025876666456, "start_lat": 39.135462800807794, "end_lon": -84.50162668204827, "end_lat": 39.12701207640838}' --out_path ~/Downloads/output --temp_path ~/Downloads/temp --pre_post pre
        assert False


class TestFetchOSM:
    def test_fetch_osm(self):
        assert False

