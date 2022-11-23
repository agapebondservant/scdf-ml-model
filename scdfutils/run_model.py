import sys
import mlflow
import logging
from mlflow.tracking import MlflowClient
from scdfutils import utils

try:
    logging.getLogger().setLevel(logging.INFO)

    logging.info("Start driver script...")

    git_sync_repo, command, inbound, inbound_queue, outbound, outbound_binding, current_app, \
    prometheus_host, prometheus_port, current_experiment_name, run_tag = [*sys.argv[1:]]

    logging.info(f"Executing: {git_sync_repo} {inbound} {outbound} {current_app}")

    utils.set_env_var('INBOUND_PORT', inbound)

    utils.set_env_var('INBOUND_PORT_QUEUE', f"{inbound_queue}")

    utils.set_env_var('OUTBOUND_PORT', outbound)

    utils.set_env_var('OUTBOUND_PORT_BINDING', f"{outbound_binding}")

    utils.set_env_var('CURRENT_APP', current_app)

    utils.set_env_var('PROMETHEUS_HOST', prometheus_host)

    utils.set_env_var('PROMETHEUS_PORT', prometheus_port)

    utils.set_env_var('CURRENT_EXPERIMENT', current_experiment_name)

    current_experiment = mlflow.get_experiment_by_name(current_experiment_name)

    current_experiment_id = current_experiment.experiment_id if current_experiment and current_experiment.lifecycle_stage == 'active' else mlflow.create_experiment(current_experiment_name)

    with mlflow.start_run(experiment_id=current_experiment_id) as active_run:

        mlflow.set_tags({'step': current_app, 'run_tag': run_tag})

        submitted_run = mlflow.run(git_sync_repo, 'main', version='main', env_manager='local')

        submitted_run_metadata = MlflowClient().get_run(submitted_run.run_id)

        logging.info(f"Submitted Run: {submitted_run}\nSubmitted Run Metadata: {submitted_run_metadata}")

except mlflow.exceptions.RestException as e:
    logging.info('REST exception occurred (platform will retry based on pre-configured retry policy): ', exc_info=True)

logging.info("End driver script.")
