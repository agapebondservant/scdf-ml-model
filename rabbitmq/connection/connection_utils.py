import logging
import traceback
from functools import wraps


def exception_handler(args):
    logging.error(f'caught {args.exc_type} with value {args.exc_value} in thread {args.thread}\n')
    logging.error(traceback.format_exc())


def send_method(_producer):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logging.info(f"{func} {args} {kwargs}")
            return func(*args, **kwargs)
        setattr(_producer, 'on_channel_open', wrapper)
        return wrapper
    return decorator


def receive_method(_consumer):
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            return func(*args, **kwargs)
        setattr(_consumer, 'process_delivery', wrapper)
        return wrapper
    return decorator
