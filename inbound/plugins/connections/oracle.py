import os
import sys

import oracledb

from inbound.core import Profile, connection_factory, logging
from inbound.plugins.connections.sqlalchemy import SQLAlchemyConnection

LOGGER = logging.LOGGER


class OracleConnection(SQLAlchemyConnection):
    def __init__(self, profile: Profile):
        super().__init__(profile)

        self.engine = None
        self.connection = None

        # TODO: delete when upgraded to sqlalchemy 2.0
        if (
            self.profile.spec.connection_string
            and "oracle" in self.profile.spec.connection_string
            and not "cx_oracle" in self.profile.spec.connection_string
        ):
            oracledb.version = "8.3.0"
            sys.modules["cx_Oracle"] = oracledb

        if (
            self.profile.spec.connection_string
            and "oracle" in self.profile.spec.connection_string
            and "cx_oracle" in self.profile.spec.connection_string
        ):
            try:
                oracledb.version = "8.3.0"
                if os.environ.get("INBOUND_ORACLE_CLIENT_LIB_PATH") is not None:
                    oracledb.init_oracle_client(
                        os.environ.get("INBOUND_ORACLE_CLIENT_LIB_PATH")
                    )
                else:
                    oracledb.init_oracle_client()
            except Exception as e:
                LOGGER.error(
                    f"Error. Please make sure the cx_Oracle module and client libraries are installed. {e}"
                )

    def check_mode(self):
        sql = """SELECT UNIQUE CLIENT_DRIVER
                FROM V$SESSION_CONNECT_INFO
                WHERE SID = SYS_CONTEXT('USERENV', 'SID')"""

        return self.execute(sql)


def register() -> None:
    """Register connector"""
    connection_factory.register("oracle", OracleConnection)
