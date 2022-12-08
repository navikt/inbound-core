import json
import logging
import os
from datetime import datetime

from common.connectors.connector_snowflake import ConnectorSnowflake
from faker import Faker

fake = Faker()

from common.bookmark_service import BookmarkService
from common.connectors import ConnectorOracle
from common.database import (
    Bookmark,
    ConfigModel,
    ConnectorModel,
    SnowflakeConnectorModel,
    SyncMode,
)
from common.oracle_to_snowflake import sync_table

LOGGER = logging.getLogger(__name__)
TEST_TABLE = "TEST"
TEST_TABLE_ROWS = 1000


# Source config
source_config = ConfigModel(
    user="system", password="oracle", dsn="localhost:1521/xe", table=TEST_TABLE
)
source_connector = ConnectorModel(config=source_config)

# Target config
target_user = os.getenv("SNOWFLAKE_DATAPRODUCT_USER")
target_pwd = os.getenv("SNOWFLAKE_DATAPRODUCT_PASSWORD")
target_account = (
    os.getenv("SNOWFLAKE_DATAPRODUCT_ACCOUNT")
    + "."
    + os.getenv("SNOWFLAKE_DATAPRODUCT_REGION")
)

snowflake_config = SnowflakeConnectorModel(
    database="vdl_dataproduct",
    database_schema="dev",
    warehouse="vdl_dataproduct_warehouse",
    account=target_account,
)

target_config = ConfigModel(
    user=target_user,
    password=target_pwd,
    table="test",
    snowflake=snowflake_config,
)
target_connector = ConnectorModel(config=target_config)


def create_test_table(db, table=TEST_TABLE):
    sql = f"""
        create table {table}
        (id number(10),
       name varchar2(256),
       email varchar2(256))
    """
    db.execute(sql)

    sql = "BEGIN "
    for i in range(1, TEST_TABLE_ROWS):
        sql += f"""
            insert into {table}
            (id, name, email)
            values
            ({i}, '{fake.name()}','{fake.email()}');
        """
    sql += " COMMIT;"
    sql += " END;"

    db.execute(sql)


def drop_test_table(db, table=TEST_TABLE):
    sql = f"""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE {table}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 THEN
            RAISE;
        END IF;
        END;
        """
    db.execute(sql)


def test_sync():
    with ConnectorOracle(connector=source_connector) as db:
        drop_test_table(db)
        create_test_table(db)
        sync_table(source_connector, target_connector)
        drop_test_table(db)


def test_sync_append():
    with ConnectorOracle(connector=source_connector) as db:
        drop_test_table(db)
        create_test_table(db)
        sync_table(source_connector, target_connector, mode=SyncMode.APPEND)
        drop_test_table(db)


def test_bookmark():
    config = target_config

    table = f"{config.snowflake.database}.{config.snowflake.database_schema}.bookmarks"
    connector = ConnectorModel(config=config)

    db_service = ConnectorSnowflake(connector=connector)
    bookmark_service = BookmarkService(db_service, table)

    bookmark = bookmark_service.set_bookmark(
        "dummy", Bookmark(column="id", last_value=400).json()
    )
    bookmark = bookmark_service.set_bookmark(
        "dummy", Bookmark(column="id", last_value=401).json()
    )
    bookmark = bookmark_service.set_bookmark(
        "dummy", Bookmark(column="id", last_value=402).json()
    )

    bookmark = bookmark_service.get_bookmark("dummy")

    res = json.loads(bookmark)

    assert res["last_value"] == 402


if __name__ == "__main__":
    test_bookmark()
