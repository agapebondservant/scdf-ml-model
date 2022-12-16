import mlflow
import logging
from scdfutils import utils, ports
from scdfutils.http_status_server import HttpHealthServer

try:

    HttpHealthServer.run_thread()

    logging.getLogger().setLevel(logging.INFO)

    logging.info("Start monitoring driver script...")

    utils.prepare_mlflow_experiment()

    with mlflow.start_run(experiment_id=utils.get_env_var("MLFLOW_EXPERIMENT_ID"), run_name='monitoring') as active_run:

        utils.prepare_mlflow_run()

        # monitoring_port = ports.get_inbound_rabbitmq_monitoring_port(utils.get_env_var('MONITORED_PORT'),
        #                                                             receive_callback=utils.generate_mlflow_data_monitoring_current_dataset)
        logging.info(f"Listening on port - :{utils.get_env_var('INBOUND_PORT')}")
        monitoring_port = ports.get_inbound_rabbitmq_monitoring_port(f":{utils.get_env_var('INBOUND_PORT')}")

except mlflow.exceptions.RestException as e:
    logging.info('REST exception occurred (platform will retry based on pre-configured retry policy): ', exc_info=True)
    HttpHealthServer.stop_thread()

logging.info("End driver script.")
