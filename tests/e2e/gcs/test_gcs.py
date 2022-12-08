import os

import numpy as np

from inbound.core.job_result import JobResult
from inbound.core.models import BucketSpec, Profile, Spec
from inbound.plugins.connections.duckdb import DuckDBConnection
from inbound.plugins.connections.gcs import GCSConnection
from inbound.plugins.utils import df

credentials = Spec(name="duckdb", database=":memory", table="test")
profile = Profile(spec=credentials)

gcs_spec = BucketSpec(bucket="sync-service", blob="test/test_duckdb.csv.gz")
gcs_profle = Profile(spec=gcs_spec)


def test_roundtrip():
    bucket = "sync-service"
    blob = "test"
    format = "parquet"
    prefix = "test_gcs"
    with GCSConnection(
        Profile(spec=Spec(blob=blob, bucket=bucket, prefix=prefix))
    ) as gcs:
        res, job_res = gcs.from_pandas(df)
        assert job_res.success

        res_iterator = gcs.to_pandas()
        df_res, job_res = next(res_iterator)
        assert job_res.success
        assert len(df) == len(df_res)


if __name__ == "__main__":
    os.environ["GOOGLE_CLOUD_PROJECT"] = "fist-dev-db45"
    test_roundtrip()
