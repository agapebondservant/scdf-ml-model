import logging
import traceback


def exception_handler(args):
    logging.error(f'caught {args.exc_type} with value {args.exc_value} in thread {args.thread}\n')
    logging.error(traceback.format_exc())
