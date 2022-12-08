import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from google.cloud import storage

from inbound.core import settings
from inbound.core.logging import LOGGER
from inbound.core.settings import Settings


def publish_metadata(id: str, paths: List[str], bucket: str, run_id: str = None) -> str:
    """[summary]

    Args:
        id (str): deata service id
        paths (List[str]): list of source files to upload
        bucket (str): destination bucket
        run_id (str, optional): unique run id. Defaults to None.

    Returns:
        str: success flag
    """
    for path in paths:
        upload_metadata_to_gcs(id, path, bucket, run_id)

    return "DONE"


def upload_metadata_to_gcs(id: str, path: str, run_id: str = None) -> str:
    """uploads a file to a bucket

    Args:
        id (str): data service id
        path (str): path to source file
        bucket_name (str): destination bucket
        run_id (str, optional): unique run id. Defaults to None.

    """

    settings = Settings()

    res = "FAILED"

    full_path = Path.cwd() / Path(path).resolve()

    name = f'{id.replace(".","_")}/{path.lower()}'

    if run_id:
        name = f'{id.replace(".","_")}/{run_id}/{path.lower()}'

    if Path(full_path).is_file():
        try:
            # using GOOGLE_APPLICATION_CREDENTIALS
            storage_client = storage.Client()

            bucket = storage_client.get_bucket(settings.spec.gcp.metadatabucket)
            # upload current run
            blob = bucket.blob(name)
            blob.upload_from_filename(full_path)
            LOGGER.info(f"Metadata file {full_path} uploaded to {name}.")
            res = "DONE"
        except Exception as e:
            LOGGER.debug(f"Error uploading metadata file {full_path} to {name}. {e}")
    else:
        LOGGER.debug(
            f"Error uploading metadata file {full_path} to {name}. The file could not be found"
        )

    return res
