import logging
import os
import sys

from loguru import logger


class InterceptHandler(logging.Handler):
    def emit(self, record):
        # Get corresponding Loguru level if it exists
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


logging.basicConfig(handlers=[InterceptHandler()], level=logging.INFO)


class SnowflakeHandler(logging.Handler):
    """
    A class which sends log records to Snowflake
    """

    def __init__(self, capacity=1):
        """
        Initialize the handler
        """
        logging.Handler.__init__(self)
        self.capacity = capacity
        self.buffer = []
        # self.connection = open_snowflake_connection()

    def shouldFlush(self, record):
        return len(self.buffer) >= self.capacity

    def emit(self, record):
        self.buffer.append(record)
        if self.shouldFlush(record):
            self.flush()

    def flush(self):
        """
        Process records and flush buffer
        """
        self.acquire()
        try:
            self.commit()
            self.buffer.clear()
        finally:
            self.release()

    def close(self):
        try:
            self.flush()
        finally:
            logging.Handler.close(self)

    def json(self, record):
        return record.__dict__

    def commit(self):
        """
        Send the records to Snowflake
        """
        for record in self.buffer:
            try:
                # TODO: Send to SF
                print("Snowflakehandler", self.json(record))
                pass

            except Exception:
                self.handleError(record)


logger.remove(0)

fmt = "{time:HH:mm:ss.SSS!UTC} |  <level>{level}</level> | {message}"

logger.add(
    sys.stdout,
    colorize=True,
    format=fmt,
)

if os.getenv("INBOUND__LOGGER_SNOWFLAKE"):
    snowflake = SnowflakeHandler()
    logger.add(snowflake)

LOGGER = logger
