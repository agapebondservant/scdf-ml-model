# RabbitMQProducer Documentation
_**host**_: The address of the RabbitMQ queue

_**username**_: The username used for authentication

_**password**_: The password used for authentication

_**virtual_host**_: The virtual host. Default: /
 
_**data**_: The message that will be published, can be overriden in the send_callback

_**exchange**_: The exchange being published to, can be overriden in the send_callback

_**routing_key**_: The routing key used for the message, can be overriden in the send_callback

_**send_callback**_: The callback handler invoked to publish messages to the queue

# RabbitMQConsumer Documentation
_**host**_: The address of the RabbitMQ queue

_**username**_: The username used for authentication

_**password**_: The password used for authentication

_**virtual_host**_: The virtual host. Default: /

_**receive_callback**_: The callback handler invoked when a message is received from the queue

_**queue**_: Name of the queue

_**queue_arguments**_: Optional queue arguments to pass to the basic_consume operation (see AMQP documentation)

_**consumer_arguments**_: Optional consumer arguments to pass to the basic_consume operation (see AMQP documentation)

_**offset**_: Value of offset to start consuming from (applies to stream-backed queue)

_**prefetch_count**_: 
Maximum number of unacknowledged messages that are permitted on the channel at a time; after the maximum
is reached, sender will block until at least one of the messages is acknowledged. Default: 1000 (see AMQP documentation)

# Sample Usage

See [hello.py](app/hello.py) for sample usage.