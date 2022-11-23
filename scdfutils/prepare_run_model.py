import os
from scdfutils import utils

os.system("echo Start prepare script...")

MODEL_ENTRY = utils.get_cmd_arg('model_entry')
GIT_SYNC_REPO = utils.get_cmd_arg('git_sync_repo')
RUN_TAG = utils.get_cmd_arg('run_tag') or '1.0.0'
INBOUND_PORT = utils.get_cmd_arg('spring.cloud.stream.bindings.input.destination')
INBOUND_PORT_QUEUE = f"{INBOUND_PORT}.{utils.get_cmd_arg('spring.cloud.stream.bindings.input.group')}"
OUTBOUND_PORT = utils.get_cmd_arg('spring.cloud.stream.bindings.output.destination')
OUTBOUND_PORT_BINDING = f"{OUTBOUND_PORT}.{utils.get_cmd_arg('spring.cloud.stream.bindings.output.producer.requiredGroups')}"
CURRENT_APP = utils.get_cmd_arg('spring.cloud.dataflow.stream.app.label')
PROMETHEUS_HOST = utils.get_cmd_arg('management.metrics.export.prometheus.rsocket.host')
PROMETHEUS_PORT = utils.get_cmd_arg('management.metrics.export.prometheus.rsocket.port')
CURRENT_EXPERIMENT = utils.get_cmd_arg('spring.cloud.dataflow.stream.name')

os.system(f'echo MODEL_ENTRY={MODEL_ENTRY}; '
          f'echo GIT_SYNC_REPO={GIT_SYNC_REPO}; '
          f'mkdir -p git_tmp; git clone {GIT_SYNC_REPO} git_tmp; '
          f'mv git_tmp/* .; ls -altr .; '
          f'python -m scdfutils.run_model {GIT_SYNC_REPO} {MODEL_ENTRY} {INBOUND_PORT} '
          f'{INBOUND_PORT_QUEUE} {OUTBOUND_PORT} {OUTBOUND_PORT_BINDING} {CURRENT_APP} '
          f'{PROMETHEUS_HOST} {PROMETHEUS_PORT} {CURRENT_EXPERIMENT} {RUN_TAG}')

os.system("echo End prepare script.")