import os
import sys
from collections import defaultdict
import logging
import traceback
import joblib
import tempfile
import mlflow
import json
import pandas as pd
from os.path import exists
from datetime import datetime, timedelta, timezone
import pytz
from evidently.test_suite import TestSuite
from evidently.test_preset import DataQualityTestPreset
from mlmetrics import exporter
import numpy as np
import re
from mlflow import MlflowClient

# load_dotenv()
reference_dataset_name, current_dataset_name = '_reference_data', '_current_data'


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
        value = os.environ[name]
        return int(value) if re.match("\d+$", value) else value
    else:
        logging.info('Unknown environment variable requested: {}'.format(name))


def set_env_var(name, value):
    if value:
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
Dataset Utilities
#########################
"""


def initialize_timeseries_dataframe(rows, schema_path, use_epoch_as_datetime=False):
    # Generate dataset
    with open(schema_path, "r") as f:
        columns = f.read().split(',')
        logging.info(f"Columns in schema: {columns}")
    dataset = append_json_list_to_dataframe(pd.DataFrame(data=[], columns=columns), rows, use_epoch_as_datetime)
    return dataset


def epoch_as_datetime(epoch_time):
    return datetime.utcfromtimestamp(epoch_time).replace(tzinfo=pytz.utc)


def dataframe_record_as_json_string(row, date_index, orientation):
    msg = json.loads(row.to_json(orient=orientation))
    msg.insert(0, datetime.strftime(date_index, '%Y-%m-%d %H:%M:%S%z'))
    msg = json.dumps(msg)
    return msg


def get_dataframe_row_as_data_and_index(rows, columns, use_epoch_as_datetime=True):
    data, index = [], []
    for row in rows:
        row_data = row[1:]
        row_index = epoch_as_datetime(row[0]) if use_epoch_as_datetime else pd.to_datetime(row[0], errors='ignore')
        row_data = {columns[col]: row_data[col] for col in range(len(columns))}
        data.append(row_data)
        index.append(row_index)
    return data, index


def append_json_list_to_dataframe(df, json_list, use_epoch_as_datetime=True):
    df2_columns = df.columns
    df2_data, df2_index = get_dataframe_row_as_data_and_index(json_list, df2_columns, use_epoch_as_datetime)
    if pd.isnull(pd.to_datetime(df2_index, errors='coerce')).any():
        logging.error(f"Could not append row(s) to dataframe: index must be of type DatetimeIndex")
        return df
    df2 = pd.DataFrame(data=df2_data, index=df2_index)
    dataset = pd.concat([df, df2]).apply(pd.to_numeric, errors='ignore')
    return dataset


def get_rolling_windows(current_dataset, reference_dataset, sliding_window_size, sliding_window_period_seconds):
    combined_dataset = pd.concat([reference_dataset, current_dataset])
    new_current_dataset = None
    new_reference_dataset = None

    if sliding_window_size:
        current_end, previous_end = sliding_window_size, sliding_window_size * 2
        new_current_dataset = combined_dataset.iloc[:-current_end]
        new_reference_dataset = combined_dataset.iloc[-previous_end:-current_end]

    elif sliding_window_period_seconds:
        current_end, previous_end = combined_dataset.index[-1] - timedelta(sliding_window_period_seconds), \
                                    combined_dataset.index[-1] - timedelta(sliding_window_period_seconds * 2),
        new_current_dataset = combined_dataset.loc[current_end:]
        new_reference_dataset = combined_dataset.loc[previous_end:current_end]

    return new_current_dataset, new_reference_dataset

"""
##########################
MLFlow Utilities
#########################
"""


def prepare_mlflow_experiment():
    current_experiment_name = get_env_var('CURRENT_EXPERIMENT')
    current_experiment = mlflow.get_experiment_by_name(current_experiment_name)
    current_experiment_id = current_experiment.experiment_id if current_experiment and current_experiment.lifecycle_stage == 'active' else mlflow.create_experiment(
        current_experiment_name)
    mlflow_tags = {'step': get_env_var('CURRENT_APP'), 'run_tag': get_env_var('RUN_TAG')}
    set_env_var("MLFLOW_CURRENT_TAGS", json.dumps(mlflow_tags))
    set_env_var("MLFLOW_EXPERIMENT_ID", current_experiment_id)
    logging.info(
        f"Launching experiment...experiment name={current_experiment_name}, experiment id={current_experiment_id}, tags={mlflow_tags}")


def prepare_mlflow_run(active_run):
    mlflow.set_tags(json.loads(get_env_var("MLFLOW_CURRENT_TAGS")))
    set_env_var('MLFLOW_RUN_ID', active_run.info.run_id)


def prepare_mlflow_artifacts(run_tag=None, artifact_path=None, dst_path=None):
    if dst_path:
        try:
            logging.info("Searching for artifacts (if any)...")
            client = MlflowClient()
            runs = mlflow.search_runs([get_env_var("MLFLOW_EXPERIMENT_ID")],
                                      filter_string=f"tags.run_tag = '{run_tag}'")
            found_artifact = None
            for i in range(len(runs)):
                last_run = runs.iloc[i]
                last_run_id = last_run.run_id
                artifacts = client.list_artifacts(last_run_id)
                for artifact in artifacts:
                    if artifact_path and artifact.path != artifact_path:
                        continue
                    logging.info(f"Downloading artifact...{artifact.path}")
                    mlflow.artifacts.download_artifacts(last_run.artifact_uri + f"/{artifact.path}", dst_path=dst_path)
                    logging.info(f"Artifact {artifact.path} downloaded successfully to {dst_path}.")
                    found_artifact = artifact
                if found_artifact:
                    break
            logging.info(f"Search complete.")
        except BaseException as e:
            logging.error("Could not download artifacts", exc_info=True)
    else:
        logging.error(f"To download artifacts, run tag and download paths are required "
                      f"(provided run tag = {run_tag}, download paths={dst_path})")


def handle_mlflow_data_monitoring(current_dataset, sliding_window_size=None, sliding_window_period_seconds=None,
                                  dst_path=None):
    global reference_dataset_name, current_dataset_name

    # Get reference dataset
    reference_dataset = _get_dataset_for_monitoring(dataset_name=reference_dataset_name, dst_path=dst_path)

    # Recompute rolling current, reference windows
    # TODO: Support static reference datasets
    current_dataset, reference_dataset = get_rolling_windows(current_dataset, reference_dataset, sliding_window_size,
                                                             sliding_window_period_seconds)

    # Perform Data Quality Test
    # TODO: Support other data monitoring tests, such as data drift, etc
    if len(current_dataset) and len(reference_dataset):
        tests = TestSuite(tests=[
            DataQualityTestPreset()
        ])
        tests.run(reference_data=reference_dataset, current_data=current_dataset)
        tests_results_json = tests.json()
        logging.info(f"Evidently generated results...{tests_results_json}")

        # Store Evidently reports
        mlflow.log_dict(json.loads(tests_results_json), '_monitoring_test_results.json')
        tests.save_html('/tmp/test_results.html')
        mlflow.log_artifact("/tmp/test_results.html")

        # Store newly generated artifacts
        _store_dataset_for_monitoring(dataset_name=current_dataset_name, dst_path=dst_path, dataset=current_dataset)
        _store_dataset_for_monitoring(dataset_name=reference_dataset_name, dst_path=dst_path, dataset=reference_dataset)

        # TODO: Publish ML metrics
        # exporter.prepare_histogram('mldata_monitoring', 'ML Data Monitoring', get_env_var('MLFLOW_CURRENT_TAGS'), tests_results_json)
        logging.info("Publishing log metrics...")
        exporter.prepare_histogram('mldata_monitoring', 'ML Data Monitoring', get_env_var('MLFLOW_CURRENT_TAGS'),
                                   np.random.randint(0, 100))
    else:
        logging.info(f"Both reference data and current data are required for monitoring: "
                     f" (provided reference data length={len(reference_dataset)}, current data length={len(current_dataset)})")


def generate_mlflow_data_monitoring_current_dataset(self, _, msg):
    global current_dataset_name
    dataset = _get_dataset_for_monitoring(dataset_name=current_dataset_name,
                                          dst_path=f"/parent/{get_env_var('MONITOR_DATASET_ROOT_PATH')}",
                                          alt_dst_path=f"/parent/{get_env_var('MONITOR_SCHEMA_PATH')}")

    if dataset is not None and msg is not None:
        dataset = append_json_list_to_dataframe(dataset, json.loads(msg), use_epoch_as_datetime=False)
        handle_mlflow_data_monitoring(dataset,
                                      sliding_window_size=get_env_var('MONITOR_SLIDING_WINDOW_SIZE'),
                                      sliding_window_period_seconds=get_env_var(
                                          'MONITOR_SLIDING_WINDOW_PERIOD_SECONDS'),
                                      dst_path=f"/parent/{get_env_var('MONITOR_DATASET_ROOT_PATH')}")
    else:
        logging.error(f'ERROR: Could not generate active dataset. '
                      f'Ensure that the monitoring schema was provided and that the destination path is valid.'
                      f'Provided: '
                      f'msg={msg}, schema = {get_env_var("MONITOR_SCHEMA_PATH")}, destination path={get_env_var("MONITOR_DATASET_ROOT_PATH")}')


def get_latest_model_version(name=None, stages=[]):
    try:
        client = MlflowClient()
        has_versions = len(client.search_model_versions(f"name = '{name}'"))
        if has_versions:
            latest_versions = client.get_latest_versions(name, stages=stages)
            return next(iter(map(lambda mv: mv.version, latest_versions)), None)
        else:
            return None
    except BaseException as e:
        logging.info('Could not retrieve latest version: ', exc_info=True)
        return None


def _get_dataset_for_monitoring(dataset_name='', dst_path='', alt_dst_path=None):
    dataset_path = f'{dst_path}/{dataset_name}'
    dataset_path = f'{alt_dst_path}' if not exists(dataset_path) and alt_dst_path else dataset_path
    with open(dataset_path, 'a+'):
        dataset = pd.read_json(dataset_path) if os.stat(dataset_path).st_size else pd.DataFrame()
        return dataset


def _store_dataset_for_monitoring(dataset_name='', dst_path='', dataset=pd.DataFrame(), orientation='records'):
    dataset_path = f'{dst_path}/{dataset_name}'
    with open(dataset_path, 'a+') as f:
        f.write(dataset.to_json(orient=orientation))


##################################################
# _log_metric: Monkey-patch for mlflow.log_metric
##################################################


def _log_metric(key, value):
    mlflow.log_metric(key, value)
    exporter.prepare_histogram(key, key.capitalize().replace("_", " "), get_env_var('MLFLOW_CURRENT_TAGS'), value)
    exporter.prepare_counter(key, key.capitalize().replace("_", " "), get_env_var('MLFLOW_CURRENT_TAGS'), value)
