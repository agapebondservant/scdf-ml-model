import pika
import logging
from rabbitmq.connection import connection


class Publisher(connection.Connection):

    def send_data(self, data_to_send):
        logging.info('In send_data...')
        self.data = data_to_send
        if self._connection is not None:
            self.on_connected(self._connection)

    def on_channel_open(self, _channel):
        """Called when our channel has opened"""
        super(Publisher, self).on_channel_open(_channel)
        self.send_callback(self, _channel)

    def send_messages(self, _channel):
        pass

    def __init__(self,
                 host=None,
                 username='data-user',
                 password='data-password',
                 virtual_host='/',
                 data=None,
                 exchange=None,
                 routing_key=None,
                 send_callback=None):
        """
        Constructor.

        Creates instances of the Publisher class which is used to send messages to a queue.

        Parameters
        ----------
        host : str
            The address of the RabbitMQ queue
        parameters : pika.ConnectionParameters
            The pika.ConnectionParameters used for authentication with the queue (see pika documentation)
        channel : str
            The connection channel
        conn_retry_count: int
            Number of connection retries after an initial connection failure occurs
        data: DataFrame
            Data to send
        exchange: str
            Name of RabbitMQ exchange
        routing_key: str
            Name of routing key to associate with published messages (can be overriden)
        _connection: connection.Connection
            Connection associated with this instance
        """
        super(Publisher, self).__init__()
        self.host = host
        self.parameters = pika.ConnectionParameters(host=host,
                                                    virtual_host=virtual_host,
                                                    credentials=pika.PlainCredentials(username, password))
        self.channel = None
        self.conn_retry_count = 0
        self.data = data
        self.exchange = exchange
        self.routing_key = routing_key
        self._connection = None
        self.send_callback = send_callback or self.send_messages
