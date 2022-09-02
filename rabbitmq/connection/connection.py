import pika
import logging
import traceback
import threading
from streamlit.scriptrunner.script_run_context import get_script_run_ctx, add_script_run_ctx
from rabbitmq.connection import connection_utils


threading.excepthook = connection_utils.exception_handler


class Connection(threading.Thread):

    def on_connected(self, conn):
        """Called when we are fully connected to RabbitMQ"""
        # Open a channel
        logging.info(f"In on_connected: {conn}")
        self._connection = conn
        self._connection.channel(on_open_callback=lambda ch: self.on_channel_open(ch))

    def on_channel_open(self, _channel):
        """Called when our channel has opened"""
        self.channel = _channel
        self.channel.add_on_close_callback(lambda ch, err: self.on_channel_closed(ch, err))
        logging.info(f"data type: {type(self.data)} {self.data}")

    def on_channel_closed(self, channel, error):
        """Called when our channel has closed"""
        try:
            logging.error(f'Error while attempting to connect...{error} {channel}')
            try:
                if self._connection.is_closing or self._connection.is_closed:
                    self._connection.ioloop.stop()
                else:
                    logging.info('Connection closed, reopening: (%s)', error)
                    # reconnect
                    self._connection = self.init_connection()
                    self.connect(self._connection)
            except Exception as e:
                pass
        except Exception as e:
            logging.error('Could not complete execution - error occurred: ', exc_info=True)
            traceback.print_exc()

    def on_connection_close(self, conn, error):
        """Called when our connection has closed"""
        logging.error(error)
        logging.error(conn)

    def on_connection_error(self, conn, error):
        """Called when an error is encountered while attempting to open a connection"""
        try:
            logging.error(f'Error while attempting to connect...{conn} {error}')
            self.connect(self._connection)
        except Exception as e:
            logging.error('Could not complete execution - error occurred: ', exc_info=True)
            traceback.print_exc()

    def init_connection(self):
        """Called when a connection is first initialized"""
        conn = pika.SelectConnection(self.parameters,
                                     on_open_callback=lambda con: self.on_connected(con),
                                     on_close_callback=lambda con, err: self.on_connection_close(con, err))
        conn.add_on_open_error_callback(self.on_connection_error)
        return conn

    def connect(self, conn):
        """Starts a continuous loop which listens for events on this connection"""
        try:
            # Loop so we can communicate with RabbitMQ
            # exit if number of connection retries exceeds a threshold
            self.conn_retry_count = self.conn_retry_count + 1
            if self.conn_retry_count > 10:
                raise KeyboardInterrupt
            self._connection.ioloop.start()
        except KeyboardInterrupt:
            # Gracefully close the connection
            self._connection.close()
            # Loop until we're fully closed, will stop on its own
            self._connection.ioloop.start()

    # Connect to RabbitMQ using the default parameters
    def run(self):
        """Initializes a connection and starts listening for events"""
        logging.info("In run method...")
        self._connection = self.init_connection()
        self.connect(self._connection)

    # Initialize class
    def __init__(self,
                 host=None):
        """Class initializer"""
        super(Connection, self).__init__()
        self._connection = None
        self.conn_retry_count = 0
        self.channel = None
        add_script_run_ctx(self)
