import os
import sys
from collections import defaultdict
import logging
import traceback
import joblib
import tempfile
import mlflow
from mlflow import MlflowClient
import json


# load_dotenv()


def get_cmd_arg(name):
    d = defaultdict(list)
    for cmd_args in sys.argv[1:]:
        cmd_arg = cmd_args.split('=')
        if len(cmd_arg) == 2:
            d[cmd_arg[0].lstrip('-')].append(cmd_arg[1])

    if name in d:
        return d[name][0]
    else:
        logging.info('Unknown command line arg requested: {}'.format(name))


def get_env_var(name):
    if name in os.environ:
        return os.environ[name]
    else:
        logging.info('Unknown environment variable requested: {}'.format(name))


def set_env_var(name, value):
    os.environ[name] = value


def get_rabbitmq_host():
    return get_env_var('SPRING_RABBITMQ_HOST')


def get_rabbitmq_username():
    return get_env_var('SPRING_RABBITMQ_USERNAME')


def get_rabbitmq_password():
    return get_env_var('SPRING_RABBITMQ_PASSWORD')


def handle_exception(exc_type, exc_value, tb):
    logging.error(f'caught {exc_type} with value {exc_value}\n')
    logging.error(traceback.format_exc())
    sys.exit(1)


def create_temp_file(content):
    with tempfile.NamedTemporaryFile() as f:
        joblib.dump(content, f)
        return f


"""
##########################
MLFlow Utilities
#########################
"""


def prepare_mlflow_run():
    mlflow.set_tags(json.loads(get_env_var("MLFLOW_CURRENT_TAGS")))


def download_mlflow_artifacts(run_tag=None, dst_path=None):
    if dst_path:
        try:
            logging.info("Searching for artifacts (if any)...")
            client = MlflowClient()
            runs = mlflow.search_runs([get_env_var("MLFLOW_EXPERIMENT_ID")], filter_string=f"tags.run_tag = '{run_tag}'")
            for i in range(len(runs)):
                last_run = runs.iloc[i]
                last_run_id = last_run.run_id
                artifacts = client.list_artifacts(last_run_id)
                if len(artifacts) == 0:
                    continue
                for artifact in artifacts:
                    logging.info(f"Downloading artifact...{artifact.path}")
                    mlflow.artifacts.download_artifacts(last_run.artifact_uri + f"/{artifact.path}", dst_path=dst_path)
                    logging.info(f"Artifact {artifact.path} downloaded successfully.")
                break
        except BaseException as e:
            logging.error("Could not download artifacts", exc_info=True)
    else:
        logging.error(f"To download artifacts, run tag and download path are required "
                      f"(provided run tag = {run_tag}, download path={dst_path})")
