import os
from scdfutils import utils

os.system("echo Start prepare script...")

utils.set_env_var('GIT_SYNC_REPO', utils.get_cmd_arg('git_sync_repo'))
utils.set_env_var('MODEL_ENTRY', utils.get_cmd_arg('model_entry'))
utils.set_env_var('INBOUND_PORT', utils.get_cmd_arg('spring.cloud.stream.bindings.input.destination'))
utils.set_env_var('INBOUND_PORT_QUEUE', f"{utils.get_cmd_arg('spring.cloud.stream.bindings.input.destination')}.{utils.get_cmd_arg('spring.cloud.stream.bindings.input.group')}")
utils.set_env_var('OUTBOUND_PORT', utils.get_cmd_arg('spring.cloud.stream.bindings.output.destination'))
utils.set_env_var('OUTBOUND_PORT_BINDING', f"{utils.get_cmd_arg('spring.cloud.stream.bindings.output.destination')}.{utils.get_cmd_arg('spring.cloud.stream.bindings.output.producer.requiredGroups')}")
utils.set_env_var('INBOUND_PORT_CONSUMER_GROUP', f"{utils.get_cmd_arg('spring.cloud.stream.bindings.input.group')}")
utils.set_env_var('OUTBOUND_PORT_REQUIRED_GROUP', f"{utils.get_cmd_arg('spring.cloud.stream.bindings.output.producer.requiredGroups')}")
utils.set_env_var('VIRTUAL_HOST', utils.get_cmd_arg('spring.cloud.stream.binders.rabbitBinder.environment.spring.rabbitmq.virtual-host'))
utils.set_env_var('CURRENT_APP', (utils.get_cmd_arg('spring.cloud.dataflow.stream.app.label') or '').replace('-', '_') or None)
utils.set_env_var('PROMETHEUS_HOST', utils.get_cmd_arg('management.metrics.export.prometheus.rsocket.host'))
utils.set_env_var('PROMETHEUS_PORT', utils.get_cmd_arg('management.metrics.export.prometheus.rsocket.port'))
utils.set_env_var('CURRENT_EXPERIMENT', utils.get_cmd_arg('spring.cloud.dataflow.stream.name'))
utils.set_env_var('RUN_TAG', utils.get_cmd_arg('run_tag') or '1.0.0')
utils.set_env_var('MONITOR_APP', utils.get_cmd_arg('monitor_app'))
# utils.set_env_var('MONITORED_PORT', utils.get_cmd_arg('monitored_port'))
utils.set_env_var('MONITOR_SLIDING_WINDOW_SIZE', utils.get_cmd_arg('monitor_sliding_window_size'))
utils.set_env_var('MONITOR_SLIDING_WINDOW_PERIOD_SECONDS', utils.get_cmd_arg('monitor_sliding_window_period_seconds'))
utils.set_env_var('MONITOR_DATASET_ROOT_PATH', 'mldata')
utils.set_env_var('MONITOR_SCHEMA_PATH', utils.get_cmd_arg('monitor_schema_path'))

os.system(f'echo MODEL_ENTRY={utils.get_env_var("MODEL_ENTRY")}; '
          f'echo GIT_SYNC_REPO={utils.get_env_var("GIT_SYNC_REPO")}; '
          f'mkdir -p git_tmp; git clone {utils.get_env_var("GIT_SYNC_REPO")} git_tmp; '
          f'mv git_tmp/* .; ls -altr .; '
          f'python -m {"scdfutils.run_model_monitor" if utils.get_env_var("MONITOR_APP") else "scdfutils.run_model"}')

os.system("echo End prepare script.")
