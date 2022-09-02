import pika
import logging
import traceback
from rabbitmq.connection import connection


class Subscriber(connection.Connection):

    def read_data(self, offset=None):
        self.set_offset(offset)
        if self._connection is None or self._connection.is_closing or self._connection.is_closed:
            self.run()
        else:
            self.on_connected(self._connection)

    def on_channel_open(self, new_channel):
        """Called when our channel has opened"""
        logging.info("In on_channel_open...")
        self.channel = new_channel
        self.channel.add_on_close_callback(lambda ch, err: self.on_channel_closed(ch, err))
        self.channel.queue_declare(queue=self.queue, durable=True,
                                   callback=lambda frame: self.on_queue_declared(frame),
                                   passive=True,
                                   arguments=self.queue_arguments)

    def on_queue_declared(self, frame):
        """Called when RabbitMQ has told us our Queue has been declared, frame is the response from RabbitMQ"""
        logging.info("In on_queue_declared...")
        try:
            if not frame:
                logging.info("Queue should be predeclared")
            self.channel.basic_qos(prefetch_count=self.prefetch_count)
            self.channel.basic_consume(self.queue,
                                       on_message_callback=lambda ch, m, h, body: self.handle_delivery(ch, m, h, body),
                                       arguments=self.consumer_arguments)
        except Exception as e:
            logging.error('Could not complete execution - error occurred: ', exc_info=True)
            traceback.print_exc()

    def handle_delivery(self, _channel, method, header, body):
        """Called when we receive a message from RabbitMQ"""
        try:
            self.receive_callback(self, header, body)
            self.channel = _channel
            self.channel.basic_ack(method.delivery_tag, True)
            # cb = functools.partial(self.ack_message, self.channel, method.delivery_tag, True)
            # self.connection.add_callback_threadsafe(cb)
        except Exception as e:
            logging.error('Could not complete execution - error occurred: ', exc_info=True)

    def process_delivery(self, header, body):
        pass

    def ack_message(self, _channel, delivery_tag, multiple):
        """Note that `ch` must be the same pika channel instance via which
        the message being ACKed was retrieved (AMQP protocol constraint).
        """
        if _channel.is_open:
            _channel.basic_ack(delivery_tag, multiple)
        else:
            pass

    def set_offset(self, offset=None):
        self.offset = offset
        if self.queue_arguments.get('x-queue-type') == 'stream' and offset is not None:
            self.consumer_arguments['x-stream-offset'] = offset

    def __init__(self,
                 host=None,
                 username='data-user',
                 password='data-password',
                 virtual_host='/',
                 receive_callback=None,
                 queue='rabbitanalytics4-stream',
                 queue_arguments={'x-queue-type': 'stream'},
                 consumer_arguments={},
                 offset=None,
                 prefetch_count=1000,
                 conn_retry_count=0):

        """
        Constructor.

        Creates instances of the Subscriber class which is used to consume messages from a queue.

        Parameters
        ----------
        host : str
            The address of the RabbitMQ queue
        parameters : pika.ConnectionParameters
            The pika.ConnectionParameters used for authentication with the queue (see pika documentation)
        queue : str
            Name of the queue
        queue_arguments: dict
            Optional queue arguments to pass to the basic_consume operation (see AMQP documentation)
        consumer_arguments: dict
            Optional consumer arguments to pass to the basic_consume operation (see AMQP documentation)
        offset: str or int
            Value of offset to start consuming from (applies to stream-backed queue)
        prefetch_count: int
            Maximum number of unacknowledged messages that are permitted on the channel at a time; after this sender will block
            until at least one of the messages is acknowledged (see AMQP documentation)
        channel: connection.channel
            Channel used for consuming from queue
        _connection: connection.Connection
            Connection associated with this instance
        """
        super(Subscriber, self).__init__()
        self.parameters = pika.ConnectionParameters(host=host,
                                                    virtual_host=virtual_host,
                                                    credentials=pika.PlainCredentials(username, password))
        self.host = host
        self.receive_callback = receive_callback or self.process_delivery
        self.queue = queue
        self.queue_arguments = queue_arguments
        self.consumer_arguments = consumer_arguments
        self.set_offset(offset)
        self.prefetch_count = prefetch_count
        self.conn_retry_count = conn_retry_count
        self.channel = None
        self._connection = None
