import base64
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Iterator, Tuple

import pandas
import sqlalchemy
from google.cloud import bigquery
from google.oauth2 import service_account

from inbound.core import JobResult, Profile, Spec, connection_factory, logging
from inbound.plugins.connections.connection import BaseConnection

LOGGER = logging.LOGGER


class BigQueryConnection(BaseConnection):
    def __init__(self, profile: Profile):
        super().__init__(profile, __file__)

        self.engine = None
        self.connection = None
        self.query = None
        self.client = None

    def __enter__(self):
        self.chunksize = self.spec.chunksize

        self.query = self.spec.query

        self.engine = _create_engine(self.spec)
        self.connection = self.engine.connect()

        self.client = _bigquery_get_client(self.spec)

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.connection:
            self.connection.close()

        if self.engine:
            self.engine.dispose()

    # TODO: use bigquery storage directly?
    def to_pandas(
        self, job_id: str = None
    ) -> Iterator[Tuple[pandas.DataFrame, JobResult]]:
        """Get iterator for BigQuery results

        Args:
            query (str): SQL string

        Returns:
            Iterator
        """

        LOGGER.info(f"Execute query {self.query} in BigQuery {self.spec.project_id}")

        try:
            iterator = pandas.read_sql(
                self.query, self.engine, chunksize=self.chunksize
            )
            return iterator
        except Exception as e:
            LOGGER.info(f"Error reading from BigQuery. {str(e)}")
            return [], JobResult(result="DONE")

    def from_pandas(
        self,
        df: pandas.DataFrame,
        job_id: str = None,
        chunk: int = 0,
        mode: str = "append",
    ) -> Tuple[Any, JobResult]:
        temp_table = self.spec.table + "_NEW"

        LOGGER.info(f"Write to table {temp_table} in BigQuery")

        if not self.client:
            LOGGER.info(f"Bigquery client not available")
            return "FAILED", JobResult()

        job_config = bigquery.LoadJobConfig(
            schema=self.spec.table_schema,
            write_disposition="WRITE_TRUNCATE"
            if (chunk == 0 and mode == "replace")
            else "WRITE_APPEND",
        )

        try:
            job = self.client.load_table_from_dataframe(
                df, temp_table, job_config=job_config
            )
            LOGGER.debug(
                f"Writing dataframe {df.size} rows in chunk number {chunk} to {temp_table} in BigQuery."
            )
            return "DONE", JobResult(
                job.result().state, len(df), df.memory_usage(index=True).sum()
            )

        except Exception as e:
            LOGGER.debug(
                f"Error writing dataframe chunk {chunk} to {temp_table} in BigQuery. {str(e)}"
            )
            return "DONE", JobResult("FAILED")

    def drop(self, table: str) -> JobResult:
        """Drop table in database

        Args:
            table (str): [description]

        Returns:
            str: 'DONE' or 'FAILED'
        """

        try:
            res = self.client.delete_table(table, not_found_ok=True)
            return JobResult(result="DONE")
        except Exception as e:
            LOGGER.debug(f"Table {table} could not be deleted. {str(e)}")
            return JobResult()


def register() -> None:
    """Register connector"""
    connection_factory.register("bigquery", BigQueryConnection)


def _clean_credentials_json(creds: str):
    keyfile = creds
    keyfile = re.sub(r"(\\n)|(\\\n)|(\\\\n)", r"\n", keyfile)
    keyfile = keyfile.replace("\\", "")
    return keyfile


def _create_engine(config: Spec):  # noqa: C901
    """Create Bigquery SQLAlchemy engine

    Args:
        config (ConfigModel): credentials and config values

    Returns:
        engine: sqlalchemy engine
    """
    keyfile = config.keyfile
    project_id = config.project_id
    client_email = config.client_email
    private_key = config.private_key

    if keyfile:
        if os.path.exists(keyfile):
            try:
                return sqlalchemy.create_engine("bigquery://", credentials_path=keyfile)
            except Exception as e:
                LOGGER.debug(
                    f"Error creating bigquery sqlalchemy engine with credentials path. {str(e)}"
                )
        else:
            try:
                keyfile = _clean_credentials_json(keyfile)
                account_info = json.loads(keyfile, strict=False)
                return sqlalchemy.create_engine(
                    "bigquery://", credentials_info=account_info
                )
            except Exception as e:
                LOGGER.debug(
                    f"Error creating bigquery sqlalchemy engine with credentials info. {str(e)}"
                )

    if private_key and client_email and project_id:
        try:
            account_info = {
                "type": "service_account",
                "client_email": client_email,
                "private_key": private_key,
                "keyfile_uri": "https://oauth2.googleapis.com/keyfile",
                "project_id": project_id,
            }
            try:
                service_account.Credentials.from_service_account_info(account_info)
            except Exception as e:
                LOGGER.debug(
                    f"Error creating bigquery sqlalchemy creditials from service account account info. {str(e)}"
                )

            return sqlalchemy.create_engine(
                "bigquery://", credentials_info=account_info
            )
        except Exception as e:
            LOGGER.debug(
                f"Error creating bigquery sqlalchemy engine with client_email and private_key credentials. {str(e)}"
            )

    LOGGER.info(f"Error creating bigquery sqlalchemy engine")


def _bigquery_get_client(config: Spec):
    keyfile = config.keyfile
    project_id = config.project_id
    client_email = config.client_email
    private_key = config.private_key

    if os.path.exists(keyfile):
        try:
            credentials = service_account.Credentials.from_service_account_file(keyfile)
            return bigquery.Client(credentials=credentials, project=project_id)
        except Exception as e:
            LOGGER.debug(
                f"Error creating bigquery client from credentials file {str(keyfile)}. {str(e)}"
            )
            pass
    else:
        try:
            if base64.b64encode(base64.b64decode(keyfile)) == keyfile:
                keyfile = base64.b64decode(keyfile)
                keyfile = _clean_credentials_json(keyfile)
                account_info = json.loads(keyfile, strict=False)
                credentials = service_account.Credentials.from_service_account_info(
                    account_info
                )
                return bigquery.Client(credentials=credentials, project=project_id)
        except:
            pass

        try:
            keyfile = _clean_credentials_json(keyfile)
            account_info = json.loads(keyfile, strict=False)
            credentials = service_account.Credentials.from_service_account_info(
                account_info
            )
            return bigquery.Client(credentials=credentials, project=project_id)
        except Exception as e:
            LOGGER.info(f"Error creating BigQuery client from credentials. {str(e)}")
            pass

    if private_key and client_email and project_id:
        try:
            account_info = {
                "type": "service_account",
                "client_email": config["client_email"],
                "private_key": config["private_key"],
                "keyfile_uri": "https://oauth2.googleapis.com/keyfile",
            }
            credentials = service_account.Credentials.from_service_account_info(
                account_info
            )
            return bigquery.Client(credentials=credentials, project=project_id)
        except Exception as e:
            LOGGER.info(
                f"Error creating BigQuery client from client email and private key. {str(e)}"
            )
