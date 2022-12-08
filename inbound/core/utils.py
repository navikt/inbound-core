import os
import re
from contextlib import contextmanager


@contextmanager
def use_dir(path):
    """
    Utility function to temporarily switch directory
    """
    current_dir = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(current_dir)


def clean_column_names(s):
    """A BigQuery column name must contain only letters (a-z, A-Z),
    numbers (0-9), or underscores (_), and it must start with a letter or underscore.
    The maximum column name length is 300 characters.
    A column name cannot use any of the following prefixes: _TABLE_, _FILE_, _PARTITION

    Args:
        s (string): original column name

    Returns:
        string: string which can be used as a BigQuery column name
    """
    res = re.sub(
        "[^a-zA-Z0-9]+",
        "_",
        s.lower()
        .strip()
        .replace("ø", "o")
        .replace("æ", "ae")
        .replace("å", "aa")
        .replace("_TABLE_", "table_")
        .replace("_FILE_", "file_")
        .replace("_PARTITION", "partition_"),
    ).strip()

    if res[0].isdigit():
        res = "_" + res

    return res[0:300]
