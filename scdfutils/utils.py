import os
import sys
from collections import defaultdict
import logging
import traceback


# load_dotenv()


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
        return os.environ[name]
    else:
        logging.info('Unknown environment variable requested: {}'.format(name))


def set_env_var(name, value):
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
    raise

