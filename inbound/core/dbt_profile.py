import glob
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
from jinja2 import Template
from pydantic import BaseModel, BaseSettings
from pydantic.env_settings import SettingsSourceCallable

from inbound.core.environment import get_env
from inbound.core.logging import LOGGER
from inbound.core.models import Profile


class DbtProfileModel(BaseModel):
    elements: Dict[str, Dict[str, Any]]

    @property
    def config(self) -> Dict[str, Any]:
        if "config" in self.elements:
            return self.elements["config"]
        return None

    def items(self) -> Dict[str, Any]:
        for x in self.elements:
            if x != "config":
                yield {"name": x, "spec": self.elements[x]}

    def get_profile(self, profile_name) -> Dict[str, Any]:
        if profile_name in self.elements:
            return self.elements[profile_name]
        return None

    def get_profile_target(self, profile_name) -> Dict[str, Any]:
        if profile_name in self.elements:
            return self.elements[profile_name]["target"]
        return None

    def get_connection_spec(self, profile_name) -> Dict[str, Any]:
        if profile_name in self.elements:
            target = self.elements[profile_name]["target"]
            spec = self.elements[profile_name]["outputs"][target]
            return spec
        return None


class DbtProfile(BaseSettings):
    profile: DbtProfileModel
    profile_name: Optional[str]
    profiles_dir: Optional[str]

    class Config:
        default_dir = Path.cwd()
        env_prefix = "DBT_"
        env_nested_delimiter = "__"

        @classmethod
        def customise_sources(
            cls,
            init_settings: SettingsSourceCallable,
            env_settings: SettingsSourceCallable,
            file_secret_settings: SettingsSourceCallable,
        ) -> Tuple[SettingsSourceCallable, ...]:
            return (
                init_settings,
                env_settings,
                dbt_profile_from_yml,
                file_secret_settings,
            )


def is_dbt_profiles_dir(profiles_dir: str):
    if not profiles_dir:
        return False

    path = Path(profiles_dir) / "profiles.yml"
    if path.is_file():
        return True
    return False


def dbt_profile_from_yml(settings: BaseSettings) -> Dict[str, Any]:

    profiles_dir = settings.__config__.default_dir
    LOGGER.info(f"Profiles in settings: {profiles_dir}")

    if not is_dbt_profiles_dir(profiles_dir):
        profiles_dir = os.getenv("DBT_PROFILES_DIR")

    if not is_dbt_profiles_dir(profiles_dir):
        start_search = Path.cwd().parents[1]
        LOGGER.info(f"Searching for profiles dir starting from: {start_search}")
        pattern = f"{start_search}/**/dbt/profiles.yml"
        for filename in glob.iglob(pattern, recursive=True):
            profiles_dir = Path(filename).parent
            LOGGER.info(f"Loading dbt profiles from path {profiles_dir}")
            break

    if not is_dbt_profiles_dir(profiles_dir):
        LOGGER.error(
            f"Error loading dbt profile from {Path(profiles_dir)}. Please provide a path or set the 'DBT_PROFILES_DIR' environment variable"
        )
        return {}

    path = Path(profiles_dir) / "profiles.yml"
    LOGGER.info(f"Loading dbt profile from {path}")
    if path.is_file():
        try:
            with open(path, "r") as stream:
                # Get dbt profile from profiles.yml file
                try:
                    profile_json = yaml.safe_load(stream)
                    # Replace 'env_var's in template
                    temp = Template(json.dumps(profile_json)).render(env_var=get_env)
                    final_json = json.loads(temp, strict=False)

                    profile = DbtProfileModel(elements=final_json)
                    return {"profile": profile}
                except Exception as e:
                    LOGGER.error(f"Error parsing dbt profile from {path}. {e}")
        except Exception as e:
            LOGGER.error(f"Error loading dbt profile from {path}. {e}")
    else:
        LOGGER.info(
            f"Error loading dbt profile from {path}. Please provide a path or set the 'DBT_PROFILE_DIR' environment variable"
        )

    return {}


def dbt_connection_params(
    profile_name: str, target: None, profiles_dir: str = None
) -> Dict[str, Any]:

    if (
        profiles_dir
        and is_dbt_profiles_dir(profiles_dir)
        and os.environ.get("DBT_PROFILES_DIR") is None
    ):
        os.environ["DBT_PROFILES_DIR"] = profiles_dir

    profiles = DbtProfile(profile_name, target, profiles_dir).profile.elements
    LOGGER.info(f"Loaded profiles {str(list(profiles.keys()))}")
    if not profiles:
        LOGGER.error(
            f"Profile with name {profile_name} and target {target} not found in profile_dir {profiles_dir}"
        )
        return {}

    LOGGER.info(f"Loading target: {target} from profile {profile_name}")
    target = target or profiles[profile_name]["target"] or "dev"
    LOGGER.info(f"Loading profile {profile_name}. Target: {target}")
    params = profiles[profile_name]["outputs"][target]
    return params


def dbt_config(profiles_dir: str = None) -> Dict[str, Any]:
    profiles = DbtProfile(profiles_dir=profiles_dir).profile.elements
    if not profiles:
        return {}
    try:
        config = profiles["config"]
        return config
    except:
        return {}
