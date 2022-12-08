import json
import os
import re
from pathlib import Path

from inbound.core.logging import LOGGER


def _get_env_path() -> str:
    env_local = os.environ.get("ENV_LOCAL")

    if not env_local:
        env_local = "/var/run/secrets"
        os.environ["ENV_LOCAL"] = env_local

    return env_local


def get_env(t):
    try:
        v = os.environ.get(t)

        try:
            val = re.sub(r"(\\n)|(\\\n)|(\\\\n)", r"\n", v)
            js = json.loads(val, strict=False)
            secret_path = _write_json_env_var_value_to_file(t, js)
            return secret_path
        except Exception as e:
            pass

        if v is None:
            LOGGER.info(
                f"Required environment variable {t} could not be found. Please make sure the environment variable is defined."
            )
            return t
        else:
            return v
    except:
        LOGGER.info(
            f"Required environment variable {t} could not be found. Please make sure the environment variable is defined."
        )
    return t


# TODO: use context manager pattern to create temp secrets files in job module
def _write_json_env_var_value_to_file(key: str, val: dict) -> str:
    path = Path(os.environ["INBOUND_SECRET_DIR"])
    try:
        path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        LOGGER.info(f"Could not open secrets directory {path}. {str(e)}")

    secret_file = (path / f"{key.lower()}.json").as_posix()
    LOGGER.info(f"Write {key} as json to file: {secret_file}")
    try:
        with open(secret_file, "w+") as f:
            json.dump(val, f)
        secret_path_env = f"{key}"
        os.environ[secret_path_env] = secret_file
        LOGGER.info(f"Set secret path {secret_path_env} to {secret_file}")
        return secret_file
    except Exception as e:
        LOGGER.info(f"Could not write secret {key} to file. {str(e)}")
        pass


def iterate_env_file(env_local, filename) -> None:
    try:
        with open(f"{env_local}/{filename}") as file:
            for line in file:
                if line.startswith("#") or not line.strip():
                    continue
                name, var = line.strip().split("=", 1)
                LOGGER.info(f"Set env variable from env file: {name}: {var}")
                os.environ[name] = var
    except Exception as e:
        LOGGER.info(f"Could not set env variable from file {filename}. {str(e)}")


def write_to_json_file(env_local, filename) -> None:
    try:
        with open(f"{env_local}/{filename}") as file:
            name = filename.strip()
            try:  # try to load as json
                var = json.load(file)
                if isinstance(var, dict):
                    path = Path(env_local) / "secrets"
                    path.mkdir(parents=True, exist_ok=True)
                    secret_file = (path / f"{name.lower()}.json").as_posix()
                    LOGGER.info(f"Write {filename} as json to file: {secret_file}")
                    try:
                        with open(secret_file, "w+") as f:
                            json.dump(var, f)
                        secret_path_env = f"{name}"
                        os.environ[secret_path_env] = secret_file
                        LOGGER.info(
                            f"Set secret path {secret_path_env} = {secret_file}"
                        )
                    except Exception as e:
                        LOGGER.info(f"Could not write secret {name} to file. {str(e)}")
                        pass
                else:
                    os.environ[name] = str(var)
                    LOGGER.info(f"Env variable {name} as text")
            except Exception as e:
                var = str(file.read().strip())
                os.environ[name] = var.strip()
                LOGGER.info(f"Env variable {name} as text. {str(e)}")
    except Exception as e:
        LOGGER.info(f"Could not set env variable from file {filename}. {str(e)}")


def set_env(env_path: str = None) -> None:
    env_local = None
    if env_path:
        env_local = env_path
    else:
        env_local = os.environ.get("ENV_LOCAL", "/var/run/secrets/nais.io/vault")

    if env_local and os.path.isdir(env_local):
        # use local env directory
        LOGGER.info(f"Set env variables from directory: {env_local}")
        for filename in os.listdir(env_local):
            if filename.endswith(".env"):
                iterate_env_file(env_local, filename)
                continue
            else:
                write_to_json_file(env_local, filename)
                continue
    else:
        LOGGER.info(f"{env_local} is not directory")
