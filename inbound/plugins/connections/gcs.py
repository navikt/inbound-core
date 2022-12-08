import os
import random
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator, Tuple

import pandas
from google.cloud import storage

from inbound.core import JobResult, Profile, connection_factory, logging
from inbound.core.logging import LOGGER
from inbound.core.models import SyncMode
from inbound.plugins.common import retry_with_backoff
from inbound.plugins.connections.connection import BaseConnection
from inbound.plugins.connections.file import FileConnection

LOGGER = logging.LOGGER


class GCSConnection(BaseConnection):
    def __init__(self, profile: Profile = None):
        super().__init__(profile, __file__)

        self.bucket = None
        self.bucket_name = profile.spec.bucket or os.getenv("INBOUND_GCS_BUCKET")
        self.prefix = profile.spec.prefix or None
        self.blob_name = profile.spec.blob or os.urandom(24).hex()
        self.blob_format = profile.spec.format or "parquet"
        self.chunk_size = profile.spec.chunksize or 10000

    def __enter__(self):
        try:
            self.client = self.get_client()
            self.bucket = self.client.bucket(self.bucket_name)
            return self
        except Exception as e:
            LOGGER.info(f"Error connecting to GCS. {str(e)}")

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def __str__(self) -> str:
        return self.name

    @property
    def root(self):
        return f"gs://{self.bucket_name}"

    @retry_with_backoff()
    def get_client(self):
        try:
            conn = storage.Client()
            LOGGER.info(f"Connected to {self.bucket_name}:{self.spec.blob}")
            return conn
        except Exception:
            raise

    def download_to_filename(self, file_name: str, blob_name: str = None):
        try:
            bucket = self.bucket
            blob = bucket.blob(blob_name or self.spec.blob)
            blob.download_to_filename(file_name)
        except Exception as e:
            LOGGER.info(f"Error downloading blob {blob} to file {file_name}. {str(e)}")

    def to_pandas(
        self, job_id: str = None, format: str = "parquet"
    ) -> Iterator[Tuple[pandas.DataFrame, JobResult]]:

        for blob in self.client.list_blobs(
            self.bucket, prefix=f"{self.prefix}/", delimiter="//"
        ):
            with tempfile.NamedTemporaryFile() as tf:
                file_name = tf.name
                try:
                    self.download_to_filename(file_name, blob.name)

                    # TODO: use dask to read chunkwise
                    if format == "csv.gz":
                        df = pandas.read_csv(file_name, compression="gzip")
                    else:
                        df = pandas.read_parquet(file_name)

                    result = JobResult(result="DONE", job_id=job_id)
                    LOGGER.info(result)
                    yield df, result
                except Exception as e:
                    LOGGER.error(f"Error {e}")
                    yield None, JobResult("FAILED", job_id=job_id)

    def upload_from_filename(
        self, file_name: str, blob_name: str = None
    ) -> Tuple[Any, JobResult]:
        if blob_name is None:
            blob_name = f"{self.blob_name}.{self.blob_format}"
        if self.prefix is not None:
            blob_name = f"{self.prefix}/{blob_name}"

        try:
            blob = self.bucket.blob(blob_name)
            blob.upload_from_filename(file_name)
            return "DONE", JobResult(result="DONE", size=os.stat(file_name).st_size)
        except Exception as e:
            LOGGER.info(
                f"Error uploading file {file_name} to blob {blob_name}. {str(e)}"
            )
            return "FAILED", JobResult("FAILED")

    def from_pandas(
        self,
        df: pandas.DataFrame,
        job_id: str = None,
        chunk: int = 0,
        mode: str = "append",
    ) -> Tuple[Any, JobResult]:
        mode = (
            SyncMode.REPLACE if (chunk == 0 and mode == "replace") else SyncMode.APPEND
        )

        with tempfile.NamedTemporaryFile(suffix=None) as tf:
            file_name = tf.name
            blob_name = self.blob_name

            # write dataframe to tempfile
            try:
                if mode == SyncMode.REPLACE:
                    if self.blob_format == "parquet":
                        df.to_parquet(file_name, index=False)
                    else:
                        df.to_csv(file_name, index=False)
                else:
                    blob_name = f"{blob_name}_{datetime.utcnow().strftime('%Y_%m_%dT%H_%M_%S%z')}"
                    if self.blob_format == "parquet":
                        df.to_parquet(file_name, index=False)
                    else:
                        df.to_csv(file_name, index=False)
                LOGGER.info(
                    f"Datafram of size {df.memory_usage(index=True).sum()} written to file {file_name}"
                )
                self.upload_from_filename(file_name, f"{blob_name}.{self.blob_format}")
                return blob_name, JobResult(result="DONE")
            except Exception as e:
                LOGGER.info(
                    f"Error writing of size {df.memory_usage(index=True).sum()} to file {file_name}. {str(e)}"
                )
                return None, JobResult(result="DONE")

    def drop(self) -> JobResult():
        try:
            os.remove(self.path)
            return JobResult(result="DONE")
        except OSError:
            return JobResult(result="FAILED")


def register() -> None:
    """Register connector"""
    connection_factory.register("gcs", GCSConnection)
