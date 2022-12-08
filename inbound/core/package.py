import re
from pathlib import Path

SOURCE_DIR = Path(__file__).parent.parent
ROOT_DIR = SOURCE_DIR.parent
DATA_DIR = ROOT_DIR / "data"
CONFIG_PATH = SOURCE_DIR / "inbound_project.yml"


def get_package_version():
    with open(SOURCE_DIR / "__init__.py", encoding="utf-8") as f:
        result = re.search(r'__version__ = ["\']([^"\']+)', f.read())
        if not result:
            raise ValueError("Can't find the package version in inbound/__init__.py")
        return result.group(1)


def get_pacage_name():
    with open(SOURCE_DIR / "__init__.py", encoding="utf-8") as f:
        result = re.search(r'__name__ = ["\']([^"\']+)', f.read())
        if not result:
            raise ValueError("Can't find the package name in inbound/__init__.py")
        return result.group(1)
