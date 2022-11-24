import os
from enum import Enum
from rabbitmq.connection.rabbitmq_producer import RabbitMQProducer
from rabbitmq.connection.rabbitmq_consumer import RabbitMQConsumer
import pika
from scdfutils import utils
import logging
from datetime import datetime
import json
from mlmetrics import exporter
import asyncio
import mlflow
import nest_asyncio
nest_asyncio.apply()

_property_prefix = 'SCDF_ML_MODEL'
FlowType = Enum('FlowType', ['INBOUND', 'OUTBOUND', 'NONE'])
logger = logging.getLogger('ports')
logger.setLevel(logging.INFO)

"""
Factory which generates instances of "ports", i.e. inbound/outbound channels which
the ML pipeline step uses to interact with third-party sources.

Currently supported ports:
- rabbitmq
- rabbitmq_streams
- prometheus
"""


def get_outbound_control_port(**kwargs):
    if utils.get_env_var('SPRING_RABBITMQ_HOST'):
        logging.info(f"In outbound_control_port: exchange: {utils.get_env_var('OUTBOUND_PORT')}, "
                     f"routing key: {utils.get_env_var('OUTBOUND_PORT_BINDING')}")
        producer = RabbitMQProducer(host=utils.get_env_var('SPRING_RABBITMQ_HOST'),
                                    username=utils.get_env_var('SPRING_RABBITMQ_USERNAME'),
                                    password=utils.get_env_var('SPRING_RABBITMQ_PASSWORD'),
                                    exchange='',
                                    routing_key=utils.get_env_var('OUTBOUND_PORT_BINDING'),
                                    send_callback=send_to_outbound_port)
        producer.__dict__.update(**kwargs)
        producer.start()
        return producer
    else:
        raise ValueError('Invalid binder: Currently supports [rabbitmq]')


def get_inbound_control_port(**kwargs):
    if utils.get_env_var('SPRING_RABBITMQ_HOST'):
        logging.info(f"In inbound_control_port: queue: {utils.get_env_var('INBOUND_PORT_QUEUE')}")
        consumer = RabbitMQConsumer(host=utils.get_env_var('SPRING_RABBITMQ_HOST'),
                                    username=utils.get_env_var('SPRING_RABBITMQ_USERNAME'),
                                    password=utils.get_env_var('SPRING_RABBITMQ_PASSWORD'),
                                    queue=utils.get_env_var('INBOUND_PORT_QUEUE'),
                                    queue_arguments={},
                                    receive_callback=receive_from_inbound_port)
        consumer.__dict__.update(**kwargs)
        consumer.start()
        return consumer
    else:
        raise ValueError('Invalid binder: Currently supports [rabbitmq]')


def send_to_outbound_port(self, _channel ):
    logger.info("in process_outputs...")
    if self.data is not None:
        self.channel.basic_publish(self.exchange, self.routing_key,
                                   json.dumps(self.data),
                                   pika.BasicProperties(content_type='text/plain',
                                                        delivery_mode=pika.DeliveryMode.Persistent,
                                                        timestamp=int(datetime.now().timestamp())))


def receive_from_inbound_port(self, header, body):
    msg = body.decode('ascii')
    logger.info(f"Received message...{msg}")
    return msg


def get_rabbitmq_port(port_name, flow_type, **kwargs):
    if flow_type is FlowType.OUTBOUND:
        producer = RabbitMQProducer(host=_get_port_property(port_name, 'RABBITMQ', 'HOST'),
                                    username=_get_port_property(port_name, 'RABBITMQ', 'USERNAME'),
                                    password=_get_port_property(port_name, 'RABBITMQ', 'PASSWORD'),
                                    virtual_host=_get_port_property(port_name, 'RABBITMQ', 'VIRTUAL_HOST'),
                                    exchange=_get_port_property(port_name, 'RABBITMQ', 'EXCHANGE'),
                                    routing_key=_get_port_property(port_name, 'RABBITMQ', 'ROUTING_KEY'))
        producer.__dict__.update(**kwargs)
        producer.start()
        return producer

    elif flow_type is FlowType.INBOUND:
        consumer = RabbitMQConsumer(host=_get_port_property(port_name, 'RABBITMQ', 'HOST'),
                                    username=_get_port_property(port_name, 'RABBITMQ', 'USERNAME'),
                                    password=_get_port_property(port_name, 'RABBITMQ', 'PASSWORD'),
                                    virtual_host=_get_port_property(port_name, 'RABBITMQ', 'VIRTUAL_HOST'),
                                    queue=_get_port_property(port_name, 'RABBITMQ', 'QUEUE'),
                                    queue_arguments={})
        consumer.__dict__.update(**kwargs)
        consumer.start()
        return consumer
    else:
        raise ValueError('Invalid flow type: Must be one of [outbound, inbound]')


def get_rabbitmq_streams_port(port_name, flow_type=FlowType.NONE, **kwargs):
    if flow_type is FlowType.OUTBOUND:
        producer = RabbitMQProducer(host=_get_port_property(port_name, 'RABBITMQ', 'HOST'),
                                    username=_get_port_property(port_name, 'RABBITMQ', 'USERNAME'),
                                    password=_get_port_property(port_name, 'RABBITMQ', 'PASSWORD'),
                                    virtual_host=_get_port_property(port_name, 'RABBITMQ', 'VIRTUAL_HOST'),
                                    exchange=_get_port_property(port_name, 'RABBITMQ', 'EXCHANGE'),
                                    routing_key=_get_port_property(port_name, 'RABBITMQ', 'ROUTING_KEY'))
        producer.__dict__.update(**kwargs)
        producer.start()
        return producer

    elif flow_type is FlowType.INBOUND:
        consumer = RabbitMQConsumer(host=_get_port_property(port_name, 'RABBITMQ', 'HOST'),
                                    username=_get_port_property(port_name, 'RABBITMQ', 'USERNAME'),
                                    password=_get_port_property(port_name, 'RABBITMQ', 'PASSWORD'),
                                    virtual_host=_get_port_property(port_name, 'RABBITMQ', 'VIRTUAL_HOST'),
                                    queue=_get_port_property(port_name, 'RABBITMQ', 'QUEUE'),
                                    queue_arguments={'x-queue-type': 'stream'})
        consumer.__dict__.update(**kwargs)
        consumer.start()
        return consumer
    else:
        raise ValueError('Invalid flow type: Must be one of [outbound, inbound]')


"""async def get_control_port_rsocket():
    logger.info("Setting up prometheus host and port...")
    prometheus_proxy_host = utils.get_env_var('PROMETHEUS_HOST')
    prometheus_proxy_port = utils.get_env_var('PROMETHEUS_PORT')
    connection = await exporter.get_rsync_connection(prometheus_proxy_host, prometheus_proxy_port)
    asyncio.run(exporter.expose_metrics_rsocket(connection))"""


async def get_mlflow_artifacts_inbound_port(flow_type=FlowType.INBOUND, **kwargs):
    if flow_type is FlowType.INBOUND:
        return utils.download_mlflow_artifacts(mlflow.last_active_run(), kwargs['artifact_name'])
    else:
        raise ValueError('Invalid flow type for this port: supports [inbound]')


def _get_port_property(port_name, port_type, property_name):
    key = f"{port_name.upper()}_{_property_prefix.upper()}_{port_type.upper()}_{property_name}"
    try:
        return os.environ[key]
    except Exception:
        raise ValueError(
            f'Could not get port property for port: {port_name}, property: {property_name}: No environment variable exists named {key}')
