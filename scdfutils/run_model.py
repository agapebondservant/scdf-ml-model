import os
import sys
import mlflow
import logging
from mlflow.tracking import MlflowClient
from mlflow.utils.logging_utils import eprint

try:
    logging.getLogger().setLevel(logging.INFO)

    logging.info("Start driver script...")

    git_sync_repo = sys.argv[1]

    logging.info(f"Executing: {git_sync_repo}")

    with mlflow.start_run() as active_run:
        run_id = mlflow.active_run().info.run_id

        submitted_run = mlflow.run(git_sync_repo, 'main', version='main', env_manager='local')

        submitted_run_metadata = MlflowClient().get_run(submitted_run.run_id)

        logging.info(f"Submitted Run: {submitted_run}\nSubmitted Run Metadata: {submitted_run_metadata}")

except Exception as e:
    logging.info('Could not complete execution - error occurred: ', exc_info=True)

logging.info("End driver script.")
