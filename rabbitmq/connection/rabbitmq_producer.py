from rabbitmq.connection import publisher


class RabbitMQProducer(publisher.Publisher):

    def __init__(self,
                 host=None,
                 username=None,
                 password=None,
                 virtual_host='/',
                 data=None,
                 exchange=None,
                 routing_key=None,
                 send_callback=None):

        """
        Constructor.

        Creates instances of the RabbitMQProducer class which is used to consume messages from a queue.

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
        data: object
            The message that will be published, can be overriden in the send_callback
        exchange:
            The exchange being published to, can be overriden in the send_callback
        routing_key:
            The routing key used for the message, can be overriden in the send_callback
        send_callback:
            The callback handler invoked to publish messages to the queue
        """

        super(RabbitMQProducer, self).__init__(host, username, password, virtual_host, data, exchange, routing_key, send_callback)
