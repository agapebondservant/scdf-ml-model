import ray
import os
ray.init(runtime_env={'working_dir': ".", 'pip': "requirements.txt",
                      'env_vars': dict(os.environ),
                      'excludes': ['*.jar', '.git*/', 'jupyter/']}) if not ray.is_initialized() else True
import logging
import mlflow
from mlflow import MlflowClient
from mlflow.models import MetricThreshold
import pandas as pd
import joblib
from scdfutils import utils
from datetime import datetime

#######################################################
# REMOTE code
#######################################################
logger = logging.getLogger('scaledtasks')


@ray.remote(num_cpus=2, memory=40 * 1024 * 1024)
class ScaledTaskController:

    def log_model(self, parent_run_id, model, flavor, **kwargs):
        logger.info(f"In log_model...run id = {parent_run_id}")
        mlflow.set_tags({'mlflow.parentRunId': parent_run_id})

        getattr(mlflow, flavor).log_model(model, **kwargs)

        logger.info("Logging was successful.")

    def load_model(self, parent_run_id, flavor, model_uri=None, **kwargs):
        try:
            logger.info(f"In load_model...run id = {parent_run_id}")
            mlflow.set_tags({'mlflow.parentRunId': parent_run_id})

            model = getattr(mlflow, flavor).load_model(model_uri)
            logger.info("Model loaded.")

            return model
        except Exception as e:
            logging.info(f'Could not complete execution for load_model - {model_uri}- error occurred: ', exc_info=True)

    def log_dict(self, parent_run_id, dataframe=None, dict_name=None):
        logger.info(f"In log_dict...run id = {parent_run_id}")
        mlflow.set_tags({'mlflow.parentRunId': parent_run_id})

        dataframe.index = dataframe.index.astype('str')
        MlflowClient().log_dict(parent_run_id, dataframe.to_dict(), dict_name)

        logger.info("Logging was successful.")

    def log_artifact(self, parent_run_id, artifact, local_path, **kwargs):
        utils.mlflow_log_artifact(parent_run_id, artifact, local_path, **kwargs)

    def load_artifact(self, parent_run_id, artifact_name, **kwargs):
        return utils.mlflow_load_artifact(parent_run_id, artifact_name, **kwargs)

    def log_text(self, parent_run_id, **kwargs):
        logger.info(f"In log_text...run id = {parent_run_id}, {kwargs}")
        mlflow.set_tags({'mlflow.parentRunId': parent_run_id})

        MlflowClient().log_text(parent_run_id, **kwargs)

        logger.info("Logging was successful.")

    def log_metric(self, parent_run_id, **kwargs):
        logger.info(f"In log_text...run id = {parent_run_id}, {kwargs}")
        mlflow.set_tags({'mlflow.parentRunId': parent_run_id})

        utils.mlflow_log_metric(parent_run_id, **kwargs)

        logger.info("Logging was successful.")

    def load_text(self, parent_run_id, **kwargs):
        try:
            logger.info(f"In load_text...{kwargs}")
            mlflow.set_tags({'mlflow.parentRunId': parent_run_id})

            text = mlflow.artifacts.load_text(**kwargs)
            logger.info(f"Text...{text}")

            return text
        except Exception as e:
            logging.info(f'Could not complete execution for load_text - {kwargs}- error occurred: ', exc_info=True)

    def get_dataframe_from_dict(self, parent_run_id=None, artifact_name=None):
        if parent_run_id and artifact_name:
            pd.DataFrame.from_dict(mlflow.artifacts.load_dict(f"runs:/{parent_run_id}/{artifact_name}"))
        else:
            logger.error(
                f"Could not load dict with empty parent_run_id or artifact_name (run_id={parent_run_id}, artifact_name={artifact_name}")

    def generate_autolog_metrics(self, flavor=None):
        utils.mlflow_generate_autolog_metrics(flavor=flavor)

    def evaluate_models(self, parent_run_id, flavor, baseline_model=None, candidate_model=None, data=None,
                        version=None):
        mlflow.set_tags({'mlflow.parentRunId': parent_run_id})
        logger.info(f"In evaluate_models...run id = {parent_run_id}")
        try:
            client = MlflowClient()

            mlflow.evaluate(
                candidate_model.model_uri,
                data,
                targets="target",
                model_type="regressor",
                validation_thresholds={
                    "r2_score": MetricThreshold(
                        threshold=0.5,
                        min_absolute_change=0.05,
                        min_relative_change=0.05,
                        higher_is_better=True
                    ),
                },
                baseline_model=baseline_model.model_uri,
            )

            logger.info("Candidate model passed evaluation; promoting to Staging...")

            client.transition_model_version_stage(
                name="baseline_model",
                version=version,
                stage="Staging"
            )

            logger.info("Candidate model promoted successfully.")

            logging.info("Updating baseline model...")
            self.log_model(candidate_model,
                           parent_run_id,
                           registered_model_name='baseline_model',
                           await_registration_for=None)

            logger.info("Evaluation complete.")
            return True
        except BaseException as e:
            logger.error(
                "Candidate model training failed to satisfy configured thresholds...could not promote. Retaining baseline model.")
            return False
