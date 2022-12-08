import csv
import gzip
import os
import shutil
import tempfile
import time
from typing import List

from google.cloud import storage

from inbound.core.job_id import generate_id
from inbound.core.logging import LOGGER
from inbound.core.models import ColumnModel, Profile, SyncMode
from inbound.core.settings import Settings
from inbound.plugins.connections.oracle import OracleConnection
from inbound.plugins.connections.snowflake import SnowflakeConnection


def source_type_to_target_type(source_type):
    return {
        "NUMBER": "NUMERIC",
        "NVARCHAR2": "VARCHAR",
        "TIMESTAMP": "TIME",
        "BINARY_FLOAT": "BINARY",
        "BINARY_DOUBLE": "BINARY",
        "CLOB": "VARIANT",  # TODO: is this correct? varchar?
        "VARCHAR2": "VARCHAR",
        "DATE": "TIMESTAMP_NTZ",
        "DATETIME": "TIMESTAMP_NTZ",
        "TIMESTAMP": "TIMESTAMP_NTZ",
        "JSON": "VARIANT",
    }.get(source_type, source_type)


def get_target_schema(source_db, table_name) -> List[ColumnModel]:
    source_schema: List[ColumnModel] = source_db.get_schema(table_name)

    target_schema = []
    for source_col in source_schema:
        target_col = source_col
        target_col.data_type = source_type_to_target_type(source_col.data_type)
        target_schema.append(target_col)

    return target_schema


def sync_table(
    source_connector: Profile,
    target_connector: Profile,
    mode: SyncMode = SyncMode.REPLACE,
    bookmark: str = None,
):

    source = OracleConnection(profile=source_connector)
    target = SnowflakeConnection(profile=target_connector)
    settings = Settings()

    table_name = source_connector.spec.table
    dataproduct_name = settings.metadata.name
    run_id = generate_id()
    blob_name = f"{dataproduct_name}/{table_name}/{run_id}.csv"

    source_schema = None
    target_schema = None

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(settings.spec.gcp.syncbucket)

    tmp = tempfile.NamedTemporaryFile(suffix=".csv")

    # copy source table data to temporary local csv file
    with open(tmp.name, "w") as f:
        writer = csv.writer(
            f, delimiter="|", lineterminator="\n", quoting=csv.QUOTE_MINIMAL
        )
        with source as source_db:
            source_db.to_csv(table_name, writer, bookmark)
            target_schema = get_target_schema(source_db, table_name)

        # TODO: compare upload and unpack time with compressed file
        """     with open(tmp.name, "rb") as f_in:
        with gzip.open(f"{tmp.name}.gz", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out) """

    # copy csv fil to bucket
    try:
        # TODO compare current upload time with upload time using gsutil
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(f"{tmp.name}")
        LOGGER.info(
            f"CSV file of size {os.stat(tmp.name).st_size} uploaded to {blob_name}"
        )
    except Exception as e:
        LOGGER.error(f"Error uploading csv file to {blob_name}. Message {e}")

    # import csv file content from bucket to target database table
    with target as target_db:
        # TODO: make connector method to get valid table name
        target_table = f"{target.spec.snowflake.database}.{target.spec.snowflake.database_schema}.{target.spec.table}"

        if mode == SyncMode.REPLACE:
            target_db.drop(target_table)

        fields = ",".join(
            [(field.column_name + " " + field.data_type) for field in target_schema]
        )

        target_db.create(target_table, fields)

        sql = f"""
            copy into {target_table}
            from @gcs_sync_stage_dev
            files = ('{blob_name}')
        """
        target_db.execute(sql)
