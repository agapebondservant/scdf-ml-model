import mlflow
import logging
from mlflow import MlflowClient
from scdfutils import utils
from scdfutils.http_status_server import HttpHealthServer
import traceback
import ray
import os
logging.getLogger().setLevel(logging.INFO)
logging.info("Initializing ray...")
ray.init(runtime_env={'working_dir': ".", 'pip': "requirements.txt",
                      'env_vars': dict(os.environ),
                      'excludes': ['*.jar', '.git*/', 'jupyter/']}) if not ray.is_initialized() else True

try:
    # HttpHealthServer.run_thread()

    logging.info("Start driver script...")

    utils.prepare_mlflow_experiment()

    with mlflow.start_run(experiment_id=utils.get_env_var("MLFLOW_EXPERIMENT_ID"), run_name='training', nested=True) as active_run:

        utils.prepare_mlflow_run(active_run)

        submitted_run = mlflow.run(utils.get_env_var('GIT_SYNC_REPO'), f'{utils.get_env_var("MODEL_ENTRY")}', version='main', env_manager='local')

        submitted_run_metadata = MlflowClient().get_run(submitted_run.run_id)

        logging.info(f"Submitted Run: {submitted_run}\nSubmitted Run Metadata: {submitted_run_metadata}")

except mlflow.exceptions.RestException as e:
    logging.info('REST exception occurred (platform will retry based on pre-configured retry policy): ', exc_info=True)
    traceback.print_exc()
    # HttpHealthServer.stop_thread()

except mlflow.exceptions.ExecutionException as ee:
    logging.info("An ExecutionException occurred...", exc_info=True)
    logging.info(str(ee))
    traceback.print_exc()
    # raise be


except BaseException as be:
    logging.info("An unexpected error occurred...", exc_info=True)
    logging.info(str(be))
    traceback.print_exc()
    # raise be

logging.info("End driver script.")

