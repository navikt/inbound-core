import functools
import gzip
import os
import random
import shutil
import time
from pathlib import Path
from typing import Tuple

from inbound.core import JobResult
from inbound.core.logging import LOGGER


def retry_with_backoff(retries=5, backoff_in_ms=1000):
    def wrapper(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            x = 0
            while True:
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    LOGGER.info(f"Unable to connect. Error: {e}")
                    if x == retries:
                        raise
                    else:
                        sleep_ms = backoff_in_ms * 2**x + random.uniform(0, 1)
                        time.sleep(sleep_ms / 1000)
                        x += 1
                        LOGGER.info(f"Retryig to connect. {x+1}/{retries}")

        return wrapped

    return wrapper


def compress_with_gzip(file_name: str, tmp_dir: str) -> Tuple[str, JobResult]:
    base_name = Path(file_name).name
    compressed_file_name = str(Path(tmp_dir) / base_name) + ".gz"
    LOGGER.info(f"{file_name} compressed as {compressed_file_name}")
    with open(file_name, "rb") as fr:
        with gzip.GzipFile(compressed_file_name, "wb") as fw:
            shutil.copyfileobj(fr, fw)
    statinfo = os.stat(compressed_file_name)
    return compressed_file_name, JobResult(result="DONE", size=statinfo.st_size)
