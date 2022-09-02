from rabbitmq.connection import subscriber


class RabbitMQConsumer(subscriber.Subscriber):
    def __init__(self,
                 host=None,
                 username=None,
                 password=None,
                 virtual_host='/',
                 receive_callback=None,
                 queue=None,
                 queue_arguments={'x-queue-type': 'stream'},
                 consumer_arguments={},
                 offset=None,
                 prefetch_count=1000,
                 conn_retry_count=0):

        """
        Constructor.

        Creates instances of the RabbitMQConsumer class which is used to consume messages from a queue.

        Parameters
        ----------
        host : str
            The address of the RabbitMQ queue
        username: str
            The username used for authentication
        password: str
            The password used for authentication
        virtual_host: str
            The virtual host. Default: /
        receive_callback:
            The callback handler invoked when a message is received from the queue
        queue : str
            Name of the queue
        queue_arguments: dict
            Optional queue arguments to pass to the basic_consume operation (see AMQP documentation)
        consumer_arguments: dict
            Optional consumer arguments to pass to the basic_consume operation (see AMQP documentation)
        offset: str or int
            Value of offset to start consuming from (applies to stream-backed queue)
        prefetch_count: int
            Maximum number of unacknowledged messages that are permitted on the channel at a time; after the maximum
             is reached, sender will block
            until at least one of the messages is acknowledged. Default: 1000 (see AMQP documentation)
        """

        super(RabbitMQConsumer, self).__init__(host, username, password, virtual_host, receive_callback, queue,
                                               queue_arguments, consumer_arguments, offset, prefetch_count,
                                               conn_retry_count)
