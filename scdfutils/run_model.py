import mlflow
import logging
from mlflow.tracking import MlflowClient
from scdfutils import utils
from scdfutils.http_status_server import HttpHealthServer

try:
    # HttpHealthServer.run_thread()

    logging.getLogger().setLevel(logging.INFO)

    logging.info("Start driver script...")

    utils.prepare_mlflow_experiment()

    with mlflow.start_run(experiment_id=utils.get_env_var("MLFLOW_EXPERIMENT_ID")) as active_run:

        utils.prepare_mlflow_run()

        submitted_run = mlflow.run(utils.get_env_var('GIT_SYNC_REPO'), f'{utils.get_env_var("MODEL_ENTRY")}', version='main', env_manager='local')

        submitted_run_metadata = MlflowClient().get_run(submitted_run.run_id)

        logging.info(f"Submitted Run: {submitted_run}\nSubmitted Run Metadata: {submitted_run_metadata}")

except mlflow.exceptions.RestException as e:
    logging.info('REST exception occurred (platform will retry based on pre-configured retry policy): ', exc_info=True)
    # HttpHealthServer.stop_thread()

logging.info("End driver script.")

