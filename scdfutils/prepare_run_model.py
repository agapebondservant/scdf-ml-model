import os
from scdfutils import utils

os.system("echo Start prepare script...")

MODEL_ENTRY = utils.get_cmd_arg('model_entry')
GIT_SYNC_REPO = utils.get_cmd_arg('git_sync_repo')
INBOUND_PORT = utils.get_cmd_arg('spring.cloud.stream.bindings.input.destination')
INBOUND_PORT_QUEUE = f"{INBOUND_PORT}.{utils.get_cmd_arg('spring.cloud.stream.bindings.input.group')}"
OUTBOUND_PORT = utils.get_cmd_arg('spring.cloud.stream.bindings.output.destination')
OUTBOUND_PORT_BINDING = f"{OUTBOUND_PORT}.{utils.get_cmd_arg('spring.cloud.stream.bindings.output.producer.requiredGroups')}"
CURRENT_APP = utils.get_cmd_arg('spring.cloud.dataflow.stream.app.label')

os.system(f'echo MODEL_ENTRY={MODEL_ENTRY}; '
          f'echo GIT_SYNC_REPO={GIT_SYNC_REPO}; '
          f'mkdir -p git_tmp; git clone {GIT_SYNC_REPO} git_tmp; '
          f'mv git_tmp/* .; ls -altr .; '
          f'python -m scdfutils.run_model {GIT_SYNC_REPO} {MODEL_ENTRY} {INBOUND_PORT} {INBOUND_PORT_QUEUE} {OUTBOUND_PORT} {OUTBOUND_PORT_BINDING} {CURRENT_APP}')

os.system("echo End prepare script.")