import os
import mlflow
from scdfutils import utils

os.system("echo Start prepare script...")

MODEL_ENTRY = utils.get_cmd_arg('model_entry')
GIT_SYNC_REPO = utils.get_cmd_arg('git_sync_repo')

os.system(f'echo MODEL_ENTRY={MODEL_ENTRY}; '
          f'echo GIT_SYNC_REPO={GIT_SYNC_REPO}; '
          f'mkdir -p git_tmp; git clone {GIT_SYNC_REPO} git_tmp; '
          f'mv git_tmp/* .; ls -altr .; python -m scdfutils.run_model {GIT_SYNC_REPO} {MODEL_ENTRY}')

os.system("echo End prepare script.")