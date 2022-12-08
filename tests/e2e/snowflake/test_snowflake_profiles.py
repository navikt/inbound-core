import logging
import os
from pathlib import Path

import pandas

from inbound.core.job_result import JobResult
from inbound.core.models import Profile, Spec
from inbound.plugins.connections.snowflake import SnowflakeConnection

user = os.getenv("SNOWFLAKE_DATAPRODUCT_USER")
pwd = os.getenv("SNOWFLAKE_DATAPRODUCT_PASSWORD")
account = (
    os.getenv("SNOWFLAKE_DATAPRODUCT_ACCOUNT")
    + "."
    + os.getenv("SNOWFLAKE_DATAPRODUCT_REGION")
)

detailed_spec = Spec(
    account=account,
    user=user,
    password=pwd,
    database="vdl_dataproduct",
    schema="dev",
    warehouse="vdl_dataproduct_warehouse",
    role="vdl_dataproduct_role",
    name=f"snowflake {account}",
    table="test",
    query="select * from testdb.testschema.test",
)
detailed_profile = Profile(
    type="snowflake", name=f"snowflake {account}", spec=detailed_spec
)


sa_spec = Spec(
    role="vdl_dataproduct_role",
    name=f"snowflake {account}",
    connection_string=f"snowflake://{user}:{pwd}@{account}/vdl_dataproduct/dev?warehouse=vdl_dataproduct_warehouse",
    table="test",
    query="select * from testdb.testschema.test",
)
sa_profile = Profile(type="snowflake", name=f"snowflake {account}", spec=sa_spec)

dbt_spec = Spec(
    name=f"snowflake {account}",
    profile="test-snowflake-db",
    target="dev",
    profiles_dir=str(Path(__file__).parent),
    table="test",
    query="select * from testdb.testschema.test",
)
dbt_profile = Profile(type="snowflake", name=f"snowflake {account}", spec=dbt_spec)


def test_dbt_profile():
    roundtrip(dbt_profile)


def test_sa_profile():
    roundtrip(sa_profile)


def test_detailed_profile():
    roundtrip(detailed_profile)


def roundtrip(profile: Profile):
    with SnowflakeConnection(profile=profile) as db:
        df_out = pandas.DataFrame()
        create_warehouse_database_and_schema(db)
        create_test_table(db)
        insert_data(db)
        res_iterator = db.to_pandas()
        df, job_res = next(res_iterator)
        if type(df) == pandas.DataFrame:
            print(f"result: {df.iloc[0]}")
            df_out = df
        drop_table(db)
        drop_warehouse_database_and_schema(db)

        assert df_out.size > 0
        assert df_out.iloc[0][1] == "Oda"


def create_test_table(db):
    db.execute(
        """
        create or replace table
        test(id integer, name string)
        """
    )


def insert_data(db):
    db.execute(
        """
        insert into test
        (id, name)
        values
        (1, 'Oda')
        """
    )


def select_data(db):
    return db.execute(
        """
        select * from test
        """
    )


def drop_table(db):
    db.execute("USE DATABASE testdb")
    db.execute("USE SCHEMA testdb.testschema")
    db.execute(
        """
        drop table test
        """
    )


def create_warehouse_database_and_schema(db):
    db.execute("CREATE WAREHOUSE IF NOT EXISTS test_warehouse")
    db.execute("CREATE DATABASE IF NOT EXISTS testdb")
    db.execute("USE DATABASE testdb")
    db.execute("CREATE SCHEMA IF NOT EXISTS testschema")

    db.execute("USE WAREHOUSE test_warehouse")
    db.execute("USE SCHEMA testdb.testschema")
    db.execute(f"USE ROLE {detailed_spec.role}")


def drop_warehouse_database_and_schema(db):
    db.execute("DROP SCHEMA IF EXISTS testschema")
    db.execute("DROP DATABASE IF EXISTS testdb")
    db.execute("DROP WAREHOUSE IF EXISTS test_warehouse")


test_sa_profile()
