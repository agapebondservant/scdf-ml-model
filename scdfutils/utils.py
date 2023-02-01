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
from prodict import Prodict
from multiprocessing import Process, Lock
from filelock import FileLock, Timeout
from mlflow.models import MetricThreshold
import csv

# load_dotenv()
reference_dataset_name, current_dataset_name = '_reference_data', '_current_data'
mutex = Lock()
file_locks = {}


################################################
# Command Line Utils
#
################################################

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


################################################
# Environment Variable Utils
#
################################################
def get_env_var(name):
    if name in os.environ:
        value = os.environ[name]
        return int(value) if re.match("\d+$", value) else value
    else:
        logging.info('Unknown environment variable requested: {}'.format(name))


def set_env_var(name, value):
    if value:
        os.environ[name] = value


################################################
# Thread/Concurrency Utils
#
################################################

def synchronize(target=None, args=(), kwargs={}):
    with mutex:
        p = Process(target=target, args=args, kwargs=kwargs)
        p.start()


def synchronize_file_write(file=None, file_path=None):
    try:
        global file_locks
        key = file_path.replace(os.sep, '_')
        lock = file_locks.get(key) or FileLock(f"{file_path}.lock")
        with lock:
            with open(file_path, "wb") as file_handle:
                joblib.dump(file, file_handle)
    except Exception as e:
        logging.info(f'Synchronized file write to {file} at file_path {file_path} failed - error occurred: ',
                     exc_info=True)


################################################
# Port Utils
#
################################################

def get_rabbitmq_host():
    return get_env_var('SPRING_RABBITMQ_HOST')


def get_rabbitmq_username():
    return get_env_var('SPRING_RABBITMQ_USERNAME')


def get_rabbitmq_password():
    return get_env_var('SPRING_RABBITMQ_PASSWORD')


#######################################
# Exception Utilities
#######################################

def handle_exception(exc_type, exc_value, tb):
    logging.error(f'caught {exc_type} with value {exc_value}\n')
    logging.error(traceback.format_exc())
    sys.exit(1)


#######################################
# File Utilities
#######################################
def create_temp_file(content):
    with tempfile.NamedTemporaryFile() as f:
        joblib.dump(content, f)
        return f


def write_to_csv(file_path, row):
    with open(file_path, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(row)


def get_csv_length(file_path):
    return len(pd.read_csv(file_path)) if exists(file_path) else 0


def get_csv_rows(file_path):
    with open(file_path, newline='') as f:
        reader = csv.reader(f)
        data = list(reader)
    return data


def truncate_file(file_path):
    f = open(file_path, "w+")
    f.close()

#######################################
# SCDF Utilities
#######################################


def initialize_scdf_runtime_params():
    logging.info("Initializing SCDF runtime params...")
    return {'scdf_run_id': get_env_var('MLFLOW_RUN_ID'),
            'scdf_experiment_id': get_env_var('MLFLOW_EXPERIMENT_ID'),
            'scdf_run_tag': get_env_var('RUN_TAG'),
            'scdf_run_step': get_env_var('CURRENT_APP')}


def update_scdf_runtime_params(initialmlrunparams, runtime_inputs):
    mlrunparams = Prodict()
    mlrunparams['scdf_run_id'] = runtime_inputs.get('scdf_run_id') or initialmlrunparams.get('scdf_run_id')
    mlrunparams['scdf_experiment_id'] = runtime_inputs.get('scdf_experiment_id') or initialmlrunparams.get('scdf_experiment_id')
    mlrunparams['scdf_run_tag'] = runtime_inputs.get('scdf_run_tag') or initialmlrunparams.get('scdf_run_tag')
    mlrunparams['scdf_run_step'] = runtime_inputs.get('scdf_run_step') or initialmlrunparams.get('scdf_run_id')
    set_env_var('MLFLOW_RUN_ID', str(mlrunparams['scdf_run_id']))
    set_env_var('MLFLOW_EXPERIMENT_ID', str(mlrunparams['scdf_experiment_id']))
    set_env_var('SCDF_RUN_TAGS', json.dumps({'scdf_run_tag': mlrunparams.get('scdf_run_tag'), 'scdf_run_step': mlrunparams.get('scdf_run_step')}))
    logging.info(f"Run params set for pipeline run=run_id={get_env_var('MLFLOW_RUN_ID')}, experiment={get_env_var('MLFLOW_EXPERIMENT_ID')}, scdf_run_tags={get_env_var('SCDF_RUN_TAGS')}")
    return mlrunparams


##########################
# Dataset Utilities
#########################


def index_as_datetime(data):
    return pd.to_datetime(data.index, format='%Y-%m-%d %H:%M:%S%z', utc=True, errors='coerce')


def datetime_as_utc(dt):
    return pytz.utc.localize(dt)


def datetime_as_offset(dt):
    return int(dt.timestamp())


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
        row_data = {columns[col]: row[col] for col in range(len(columns))}
        row_index = epoch_as_datetime(row[0]) if use_epoch_as_datetime else pd.to_datetime(row[0], errors='ignore')
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


def get_next_rolling_window(current_dataset, num_shifts):
    if not len(current_dataset):
        logging.error("Error: Cannot get the next rolling window for an empty dataset")
    else:
        new_dataset = pd.concat(
            [current_dataset[num_shifts % len(current_dataset):], current_dataset[:num_shifts % len(current_dataset)]])
        new_dataset.index = current_dataset.index + (current_dataset.index.freq * num_shifts)
        return new_dataset


def get_current_datetime():
    return pytz.utc.localize(datetime.now())


##########################
# MLFlow Utilities
#########################
def get_current_run_id():
    last_active_run = mlflow.last_active_run()
    return last_active_run.info.run_id if last_active_run else None


def get_root_run_id(experiment_names=['Default']):
    runs = mlflow.search_runs(experiment_names=experiment_names, filter_string="tags.runlevel='root'", max_results=1,
                              output_format='list')
    logging.debug(f"Parent run is...{runs}")
    root_run_id = runs[0].info.run_id if len(runs) else None
    if root_run_id is not None:
        mlflow.set_tags({'mlflow.parentRunId': root_run_id})
    else:
        mlflow.set_tags({'runlevel': 'root'})
        last_active_run = mlflow.last_active_run()
        root_run_id = last_active_run.info.run_id if last_active_run else None
    return root_run_id


def get_run_for_artifacts(active_run_id):
    experiment_name = os.environ.get('MLFLOW_EXPERIMENT_NAME') or 'Default'
    runs = mlflow.search_runs(experiment_names=[experiment_name], filter_string="tags.mainartifacts='y'", max_results=1,
                              output_format='list')
    if len(runs):
        return runs[0].info.run_id
    else:
        mlflow.set_tags({'mainartifacts': 'y'})
        return active_run_id


def prepare_mlflow_experiment():
    current_experiment_name = get_env_var('CURRENT_EXPERIMENT')
    current_experiment = mlflow.get_experiment_by_name(current_experiment_name)
    current_experiment_id = current_experiment.experiment_id if current_experiment and current_experiment.lifecycle_stage == 'active' else mlflow.create_experiment(
        current_experiment_name)
    mlflow_tags = {'step': get_env_var('CURRENT_APP'), 'run_tag': get_env_var('RUN_TAG')}
    set_env_var("MLFLOW_CURRENT_TAGS", json.dumps(mlflow_tags))
    set_env_var("MLFLOW_EXPERIMENT_ID", current_experiment_id)
    parent_run_id = get_root_run_id(experiment_names=[current_experiment_name])
    """if parent_run_id is not None:
        mlflow.set_tags({'mlflow.parentRunId': parent_run_id})
    else:
        mlflow.set_tags({'runlevel': 'root'})"""
    logging.info(
        f"Launching experiment...experiment name={current_experiment_name}, experiment id={current_experiment_id}, tags={mlflow_tags}")


def prepare_mlflow_run(active_run):
    mlflow.set_tags(json.loads(get_env_var("MLFLOW_CURRENT_TAGS")))
    set_env_var('MLFLOW_RUN_ID', get_current_run_id())

"""
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
"""


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


def mlflow_log_model(parent_run_id, model, flavor, **kwargs):
    logging.info(f"In log_model...run id = {parent_run_id}")
    mlflow.set_tags({'mlflow.parentRunId': parent_run_id})

    getattr(mlflow, flavor).log_model(model, **kwargs)

    logging.info("Logging was successful.")


def mlflow_load_model(parent_run_id, flavor, model_uri=None, **kwargs):
    try:
        logging.info(f"In load_model...run id = {parent_run_id}")
        mlflow.set_tags({'mlflow.parentRunId': parent_run_id})

        model = getattr(mlflow, flavor).load_model(model_uri)
        logging.info("Model loaded.")

        return model
    except Exception as e:
        logging.info(f'Could not complete execution for load_model - {model_uri}- error occurred: ', exc_info=True)


def mlflow_log_dict(parent_run_id, dataframe=None, dict_name=None):
    logging.info(f"In log_dict...run id = {parent_run_id}")
    mlflow.set_tags({'mlflow.parentRunId': parent_run_id})

    dataframe.index = dataframe.index.astype('str')
    MlflowClient().log_dict(parent_run_id, dataframe.to_dict(), dict_name)

    logging.info("Logging was successful.")


def mlflow_log_metric(parent_run_id, **kwargs):
    logging.info(f"In log_metric...run id = {parent_run_id}")
    mlflow.set_tags({'mlflow.parentRunId': parent_run_id})

    MlflowClient().log_metric(parent_run_id, **kwargs)

    logging.info("Logging was successful.")


def mlflow_log_artifact(parent_run_id, artifact, local_path, **kwargs):
    logging.info(f"In log_artifact...run id = {parent_run_id}, local_path")
    mlflow.set_tags({'mlflow.parentRunId': parent_run_id})

    with open(local_path, "wb") as artifact_handle:
        joblib.dump(artifact, artifact_handle)
    # synchronize_file_write(file=artifact, file_path=local_path)
    MlflowClient().log_artifact(parent_run_id, local_path, **kwargs)
    logging.info("Logging was successful.")


def mlflow_load_artifact(parent_run_id, artifact_name, **kwargs):
    try:
        logging.info(f"In load_artifact...run id = {parent_run_id}, {kwargs}")
        mlflow.set_tags({'mlflow.parentRunId': parent_run_id})

        artifact_list = MlflowClient().list_artifacts(parent_run_id)
        artifact_match = next((artifact for artifact in artifact_list if artifact.path == artifact_name), None)
        artifact = None

        if artifact_match:
            download_path = mlflow.artifacts.download_artifacts(**kwargs)
            logging.info(f"Artifact downloaded to...{download_path}")
            with open(f"{download_path}", "rb") as artifact_handle:
                artifact = joblib.load(artifact_handle)
        else:
            logging.info(f"Artifact {artifact_name} cannot be loaded (has not yet been saved).")

        return artifact
    except Exception as e:
        logging.info(f'Could not complete execution for load_artifact - {kwargs}- error occurred: ', exc_info=True)


def mlflow_log_text(parent_run_id, **kwargs):
    logging.info(f"In log_text...run id = {parent_run_id}, {kwargs}")
    mlflow.set_tags({'mlflow.parentRunId': parent_run_id})

    MlflowClient().log_text(parent_run_id, **kwargs)

    logging.info("Logging was successful.")


def mlflow_load_text(parent_run_id, **kwargs):
    try:
        logging.info(f"In load_text...{kwargs}")
        mlflow.set_tags({'mlflow.parentRunId': parent_run_id})

        text = mlflow.artifacts.load_text(**kwargs)
        logging.info(f"Text...{text}")

        return text
    except Exception as e:
        logging.info(f'Could not complete execution for load_text - {kwargs}- error occurred: ', exc_info=True)


def get_dataframe_from_dict(parent_run_id=None, artifact_name=None):
    if parent_run_id and artifact_name:
        pd.DataFrame.from_dict(mlflow.artifacts.load_dict(f"runs:/{parent_run_id}/{artifact_name}"))
    else:
        logging.error(
            f"Could not load dict with empty parent_run_id or artifact_name (run_id={parent_run_id}, artifact_name={artifact_name}")


def mlflow_generate_autolog_metrics(flavor=None):
    getattr(mlflow, flavor).autolog(log_models=False) if flavor is not None else mlflow.autolog(log_models=False)


def mlflow_evaluate_models(parent_run_id, flavor, baseline_model=None, candidate_model=None, data=None,
                           version=None, baseline_model_name='baseline_model'):
    mlflow.set_tags({'mlflow.parentRunId': parent_run_id})
    logging.info(f"In evaluate_models...run id = {parent_run_id}")
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

        logging.info("Candidate model passed evaluation; promoting to Staging...")

        client.transition_model_version_stage(
            name="baseline_model",
            version=version,
            stage="Staging"
        )

        logging.info("Candidate model promoted successfully.")

        logging.info("Updating baseline model...")
        mlflow_log_model(parent_run_id,
                         candidate_model,
                         artifact_path=flavor,
                         registered_model_name=baseline_model_name,
                         await_registration_for=None)

        logging.info("Evaluation complete.")
        return True
    except BaseException as e:
        logging.error(
            "Candidate model training failed to satisfy configured thresholds...could not promote. Retaining baseline model.")


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
