import time

from inbound.core.publisher import upload_metadata_to_gcs


def test_publish_metadata_google_creds():

    run_id = str(round(time.time() * 1000))

    path = f"tests/data/metadata/test.txt"
    res = upload_metadata_to_gcs("test", path, "dataservice-test", run_id)

    assert res == "DONE"


def test_publish_metadata():

    run_id = str(round(time.time() * 1000))

    path = f"tests/data/metadata/test.txt"
    res = upload_metadata_to_gcs("test", path, "dataservice-test", run_id)

    assert res == "DONE"
