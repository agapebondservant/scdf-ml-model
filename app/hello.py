from rabbitmq.connection.rabbitmq_producer import RabbitMQProducer
from rabbitmq.connection.rabbitmq_consumer import RabbitMQConsumer
import pika
from datetime import datetime
import logging
import time


#######################################################
# Producer code:
#######################################################


def on_send(self, _channel):
    """ Publishes data """
    logging.info("in on_send...")
    self.channel.basic_publish(self.exchange, self.routing_key, self.data,
                               pika.BasicProperties(content_type='text/plain',
                                                    delivery_mode=pika.DeliveryMode.Persistent,
                                                    timestamp=int(datetime.now().timestamp())))


producer = RabbitMQProducer(host='toad.rmq.cloudamqp.com',
                            data='Hello World from ML Models!!!',
                            username='qcirbflj',
                            password='lEuhDZXFf1m5O5XoXUHyqa5FYe8FEaZm',
                            virtual_host='qcirbflj',
                            exchange='test-exchange',
                            routing_key='test',
                            send_callback=on_send)

# Start publishing messages
producer.start()

time.sleep(5)

# Can use to send more data
producer.send_data('Hello again!')  # Can use to send more data


#######################################################
# Consumer code:
#######################################################


def on_receive(self, header, body):
    logging.info(f"Received message...{body.decode('ascii')}")


consumer = RabbitMQConsumer(host='toad.rmq.cloudamqp.com',
                            username='qcirbflj',
                            password='lEuhDZXFf1m5O5XoXUHyqa5FYe8FEaZm',
                            virtual_host='qcirbflj',
                            queue='test-queue',
                            queue_arguments={},
                            prefetch_count=0,
                            receive_callback=on_receive)

time.sleep(5)

# Start consuming messages
consumer.start()
