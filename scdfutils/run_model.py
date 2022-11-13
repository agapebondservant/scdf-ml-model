import os
import sys
import mlflow
import logging
from mlflow.tracking import MlflowClient
from scdfutils import utils

try:
    logging.getLogger().setLevel(logging.INFO)

    logging.info("Start driver script...")

    git_sync_repo, command, inbound, inbound_queue, outbound, outbound_binding, currentapp = \
        sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7]

    logging.info(f"Executing: {git_sync_repo} {inbound} {outbound} {currentapp}")

    utils.set_env_var('INBOUND_PORT', inbound)

    utils.set_env_var('INBOUND_PORT_QUEUE', f"{inbound_queue}")

    utils.set_env_var('OUTBOUND_PORT', outbound)

    utils.set_env_var('OUTBOUND_PORT_BINDING', f"{outbound_binding}")

    utils.set_env_var('CURRENT_APP', currentapp)

    with mlflow.start_run() as active_run:
        submitted_run = mlflow.run(git_sync_repo, 'main', version='main', env_manager='local')

        submitted_run_metadata = MlflowClient().get_run(submitted_run.run_id)

        logging.info(f"Submitted Run: {submitted_run}\nSubmitted Run Metadata: {submitted_run_metadata}")

except Exception as e:
    logging.info('Could not complete execution - error occurred: ', exc_info=True)

logging.info("End driver script.")
