from typing import Protocol

from inbound.core.logging import LOGGER


class DatabaseService(Protocol):
    def __enter__():
        ...

    def __exit__():
        ...

    def execute(slq: str):
        ...

    def fetchone(sql: str):
        ...

    def fetchall(sql: str):
        ...


class BookmarkService:
    # TODO: Make singleton
    def __init__(self, service: DatabaseService, table):
        self.service = service
        self.bookmarks_table = table
        self.initialized = False
        if not self.initialized:
            self.init()

    def init(self):
        with self.service as db:
            try:
                sql = f"create table if not exists {self.bookmarks_table}(id int autoincrement (1,1), name varchar(256), bookmark variant)"
                db.execute(sql)
                self.initialized = True
            except Exception as e:
                LOGGER.error(f"Error: {e}")

    def get_bookmark(self, table):
        with self.service as db:
            try:
                sql = f"select bookmark from (select *,row_number() over(partition by name order by id desc) AS row_number from {self.bookmarks_table}) where row_number = 1 and name = '{table}';"
                bookmark = db.fetchone(sql)
                LOGGER.info(f"Bookmark {bookmark} for table {table}")
                if isinstance(bookmark, tuple):
                    return bookmark[0]
                return bookmark
            except Exception as e:
                LOGGER.error(
                    f"Error getting bookmark from table {self.bookmarks_table} form table {table}. Error {e}"
                )

    def set_bookmark(self, table, bookmark):
        with self.service as db:
            try:
                sql = f"insert into {self.bookmarks_table}(name, bookmark) select '{table}', parse_json($${bookmark}$$);"
                bookmark = db.execute(sql)
                LOGGER.info(f"Bookmark {bookmark} set for table {table}")
            except Exception as e:
                LOGGER.error(
                    f"Error registering bookmark {bookmark} in table {self.bookmarks_table} for table {table}. Error {e}"
                )
