import logging
from scdfutils import utils, ports
from pipeline_task.main.pipeline_task import PipelineTask
from pipeline_task.main.environments.ray_environment import RayEnvironment
from pipeline_task.main.parameter_servers.ray_parameter_server import RayParameterServer
import inspect
import pika
from datetime import datetime
import sys
import json
from prodict import Prodict

# sys.excepthook = utils.handle_exception
logger = logging.getLogger('scdf-adapter')
logger.setLevel(logging.INFO)


mlparams = Prodict()

def scdf_adapter(environment=None):
    """
    Decorator used to configure specific aspects of the SCDF infrastructure used to run the ML model

    Parameters
    ----------
    environment : str
        The remote environment where the ML Model app will be launched.
        Current options supported:
            ray: Will run in a Ray environment identified by the RAY_ADDRESS environment variable,
            or the local environment if RAY_ADDRESS is not configured
            None: Will run in the local SCDF environment
    """

    logger.info(f"In scdf_adapter decorator...")

    def adapter(func):
        logger.info(f"In adapter method...")

        def wrapper(*args, **kwargs):
            outputs = None
            logger.info(f"In scdf_adapter wrapper...")

            logger.info("Activating inbound and outbound ports...")

            inbound_port = ports.get_inbound_control_port()
            outbound_port = ports.get_outbound_control_port()
            global mlparams

            while True:
                for inputs in inbound_port:
                    if inputs is not None:
                        inputs = json.loads(inputs)
                        if environment == 'ray':
                            address = utils.get_env_var('RAY_ADDRESS')
                            logger.info(f"Preparing to run on Ray remote environment - address {address}")

                            # Set up Ray environment
                            ray_environment = RayEnvironment(params={}, runnable_class=PipelineTask, host=address)
                            ray_parameter_server = RayParameterServer()

                            # Set up mlparams
                            inputs = ray_parameter_server.get(inputs)
                            mlparams = Prodict.from_dict({**inputs})
                            logger.info(f"Input params...{inputs}\nmlparams...{mlparams}\n")

                            # Invoke ML command on Ray
                            outputs = ray_environment.run_worker(func=func,
                                                                 input_args=tuple(
                                                                     [eval("f'{}'".format(arg)) for arg in args]),
                                                                 input_kwargs={k: eval("f'{}'".format(v)) for k, v in
                                                                               kwargs.items()})

                            # Merge method outputs with mlparams
                            mlparams = Prodict.from_dict({**mlparams, **{utils.get_env_var('CURRENT_APP'): outputs}})
                            mlparams_ref = ray_parameter_server.put(mlparams)
                            logger.info(f"Newly set params...{mlparams} {mlparams_ref}")

                        else:
                            logger.info(f"No supported adapter for environment {environment}")
                            logger.info(f"Running in local environment...{args} {kwargs} {inputs}")

                            # Set up mlparams
                            mlparams = Prodict.from_dict({**inputs})
                            logger.info(f"Input params...{inputs}\nmlparams...{mlparams}\nhttp..{mlparams.http}")

                            # Invoke ML command locally
                            ml_args = tuple([eval("f'{}'".format(arg)) for arg in args])
                            ml_kwargs = {k: eval("f'{}'".format(v)) for k, v in kwargs.items()}
                            outputs = func(*ml_args, **ml_kwargs)

                            # Merge ML command outputs with mlparams
                            mlparams = Prodict.from_dict({**mlparams, **{utils.get_env_var('CURRENT_APP'): outputs}})
                            logger.info(f"Newly set params...{mlparams}")

                        outbound_port.send_data(mlparams)
            return mlparams

        return wrapper

    return adapter
