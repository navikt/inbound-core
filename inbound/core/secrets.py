import json
import os
import re
from pathlib import Path

from google.cloud import secretmanager
from google.oauth2 import service_account

from inbound.core.logging import LOGGER
from inbound.core.settings import Settings


def set_env_variables_from_secrets(settings: Settings = None) -> None:

    if (
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") is not None
        or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON") is not None
    ):
        set_env_variables_from_secret_manager(settings)

    secrets_path = os.environ.get("INBOUND_SECRETS_PATH", settings.spec.secrets_path)

    set_environment_variables_from_directory(secrets_path)


def set_env_variables_from_secret_manager(settings: Settings = None) -> None:

    try:
        project_id = settings.spec.gcp.project_id
        os.environ["GOOGLE_CLOUD_PROJECT"] = project_id
    except Exception as e:
        LOGGER.info(f"Error setting 'GOOGLE_CLOUD_PROJECT' {e}")

    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") is not None:
        try:
            LOGGER.info(
                "get secret manager client using GOOGLE_APPLICATION_CREDENTIALS"
            )
            secret_manager_client = secretmanager.SecretManagerServiceClient()

        except Exception as e:
            LOGGER.info(
                f"Error accessing secret manager with default credentials. Error {e}"
            )
    elif os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON") is not None:
        LOGGER.info(
            "get secret manager client using GOOGLE_APPLICATION_CREDENTIALS_JSON"
        )
        try:
            json_account_info = json.loads(
                os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"]
            )
            credentials = service_account.Credentials.from_service_account_info(
                json_account_info
            )
            secret_manager_client = secretmanager.SecretManagerServiceClient(
                credentials=credentials
            )
        except Exception as e:
            LOGGER.info(
                f"""
                Error accessing secret manager with GOOGLE_APPLICATION_CREDENTIALS_JSON.
                Error {e}
                """
            )

    set_secrets_from_secret_manager(settings, secret_manager_client)


def set_secrets_from_secret_manager(settings: Settings, secret_manager_client):
    try:
        project_id = settings.spec.gcp.project_id
        os.environ["GOOGLE_CLOUD_PROJECT"] = project_id
    except Exception as e:
        LOGGER.info(f"Error setting 'GOOGLE_CLOUD_PROJECT' {e}")

    if secret_manager_client is not None and settings is not None:
        try:
            for secret in settings.spec.gcp.secrets:
                secret_id = secret["name"]
                version_id = secret["version"]

                # Set resource name
                name = (
                    f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
                )

                LOGGER.info(f"get secrets from {name}")

                # Access the secret version.
                response = secret_manager_client.access_secret_version(name=name)

                # Decoded payload.
                secrets = response.payload.data.decode("UTF-8")

                # Set secrets as environment variables
                for key, value in json.loads(secrets).items():
                    os.environ[key.upper()] = value

                LOGGER.info(
                    "Environment variables set from secrets stored in secret manager"
                )
        except Exception as e:
            LOGGER.info(
                f"Error accessing secret manager with settings from context. Error {e}"
            )


def set_environment_variables_from_directory(secrets_path: str) -> None:

    LOGGER.info(f"Get secrets from local directory {secrets_path}")

    if os.path.isdir(secrets_path):
        LOGGER.info(f"Set env variables from directory: {secrets_path}")
        for filename in os.listdir(secrets_path):
            if filename.endswith(".env"):
                iterate_environment_variables_in_file(secrets_path, filename)
                continue
            else:
                process_secrets_file(secrets_path, filename)
                continue
    else:
        LOGGER.info(f"{secrets_path} is not directory")


def get_environment_variable(t):
    try:
        v = os.environ.get(t)

        try:
            val = re.sub(r"(\\n)|(\\\n)|(\\\\n)", r"\n", v)
            js = json.loads(val, strict=False)
            secret_path = write_json_env_var_value_to_file(t, js)
            return secret_path
        except Exception:
            pass

        return v
    except Exception as e:
        LOGGER.info(f"Required environment variable {t} could not be found. Error {e}")
    return t


def write_json_env_var_value_to_file(key: str, val: dict) -> str:

    secrets_path = os.environ.get(
        "INBOUND_SECRETS_PATH", "/var/run/secrets/nais.io/vault"
    )

    path = Path(secrets_path)

    try:
        path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        LOGGER.info(f"Could not open secrets directory {path}. {str(e)}")

    secret_file = (path / f"{key.lower()}.json").as_posix()
    LOGGER.info(f"Write secret {key} as json to file: {secret_file}")
    try:
        with open(secret_file, "w+") as f:
            json.dump(val, f)
        secret_path_env = f"{key}"
        os.environ[secret_path_env] = secret_file
        LOGGER.info(f"Set secret path {secret_path_env} to {secret_file}")
        return secret_file
    except Exception as e:
        LOGGER.info(f"Could not write secret {key} to file. {e}")
        pass


def iterate_environment_variables_in_file(secrets_path: str, filename: str) -> None:
    try:
        if Path(f"{secrets_path}/{filename}").is_file():
            with open(f"{secrets_path}/{filename}") as file:
                for line in file:
                    if line.startswith("#") or not line.strip():
                        continue
                    name, var = line.strip().split("=", 1)
                    LOGGER.info(f"Set env variable from env file: {name}: {var}")
                    os.environ[name] = var
        else:
            LOGGER.info(f"{secrets_path}/{filename} is not a file")
    except Exception as e:
        LOGGER.info(f"Could not set env variable from file {filename}. {e}")


def process_secrets_file(secrets_path: str, filename: str) -> None:
    try:
        if Path(f"{secrets_path}/{filename}").is_file():
            with open(f"{secrets_path}/{filename}") as file:
                name = filename.strip()
                try:  # try to load as json
                    var = json.load(file)
                    if isinstance(var, dict):
                        path = Path(secrets_path) / "secrets"
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
                            LOGGER.info(
                                f"Could not write secret {name} to file. {str(e)}"
                            )
                            pass
                    else:
                        os.environ[name] = str(var)
                        LOGGER.info(f"Env variable {name} set as as text")
                except Exception as e:
                    var = str(file.read().strip())
                    os.environ[name] = var.strip()
                    LOGGER.error(f"Error setting variable {name} as text. {e}")
        else:
            LOGGER.info(f"{secrets_path}/{filename} is not a file")
    except Exception as e:
        LOGGER.info(f"Could not open secrets file {filename}. {e}")
