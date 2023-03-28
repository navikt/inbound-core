import logging
import os

import numpy
import pandas

from inbound.core.models import Profile, Spec
from inbound.core.utils import generate_id
from inbound.plugins.connections.duckdb import DuckDBConnection
from inbound.plugins.connections.snowflake import SnowflakeConnection
from inbound.plugins.utils import df

duck_credentials = Spec(name="duckdb", database=":memory", table="test")
duck_profile = Profile(spec=duck_credentials)

LOGGER = logging.getLogger(__name__)

user = os.getenv("SNOWFLAKE_DATAPRODUCT_USER")
pwd = os.getenv("SNOWFLAKE_DATAPRODUCT_PASSWORD")
account = (
    os.getenv("SNOWFLAKE_DATAPRODUCT_ACCOUNT")
    + "."
    + os.getenv("SNOWFLAKE_DATAPRODUCT_REGION")
)

spec = Spec(
    database="vdl_dataproduct",
    database_schema="dev",
    warehouse="vdl_dataproduct_warehouse",
    role="vdl_dataproduct_role",
    name=f"snowflake {account}",
    connection_string=f"snowflake://{user}:{pwd}@{account}",
    table="test_from_duck_db",
    query="select * from testdb.testschema.test",
    bucket="sync-service",
)

sf_profile = Profile(type="snowflake", name=f"snowflake {account}", spec=spec)


def test_pandas():
    job_id = generate_id()
    test_df = df.copy()
    iterations = -1
    with SnowflakeConnection(profile=sf_profile) as sf_db:
        create_warehouse_database_and_schema(sf_db)
        with DuckDBConnection(profile=duck_profile) as duck_db:
            duck_db.from_pandas(test_df, job_id)
            res_iterator = duck_db.to_pandas()
            for index, (dfi, job_res) in enumerate(res_iterator):
                res, job_res = sf_db.from_pandas(dfi, job_id=job_id)
                LOGGER.info(f"Upload: {index}. Result: {res}. {job_res}")
                iterations = index
        drop_warehouse_database_and_schema(sf_db)

    assert iterations >= 0


# TODO: Unfinished
def u_copy():
    with SnowflakeConnection(profile=sf_profile) as sf_db:
        with DuckDBConnection(profile=duck_profile) as duck_db:
            duck_db.from_pandas(df)

            dir_name, result = duck_db.to_dir()

            if result.success:

                bucket_name, result = sf_db.from_dir(dir_name, "duckdb_test")

                # create_warehouse_database_and_schema(sf_db)

                # drop_warehouse_database_and_schema(sf_db)

                duck_db.drop(duck_profile.spec.table)

                assert True


def create_warehouse_database_and_schema(db):
    db.execute("CREATE WAREHOUSE IF NOT EXISTS test_warehouse")
    db.execute("CREATE DATABASE IF NOT EXISTS testdb")
    db.execute("USE DATABASE testdb")
    db.execute("CREATE SCHEMA IF NOT EXISTS testschema")

    db.execute("USE WAREHOUSE test_warehouse")
    db.execute("USE SCHEMA testdb.testschema")
    db.execute(f"USE ROLE {spec.role}")


def drop_warehouse_database_and_schema(db):
    db.execute("DROP SCHEMA IF EXISTS testschema")
    db.execute("DROP DATABASE IF EXISTS testdb")
    db.execute("DROP WAREHOUSE IF EXISTS test_warehouse")
