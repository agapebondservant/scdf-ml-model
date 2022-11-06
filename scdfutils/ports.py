import os
from enum import Enum
from rabbitmq.connection.rabbitmq_producer import RabbitMQProducer
from rabbitmq.connection.rabbitmq_consumer import RabbitMQConsumer
import pika
from scdfutils import utils

_property_prefix = 'SCDF_ML_MODEL'
FlowType = Enum('FlowType', ['INBOUND', 'OUTBOUND', 'NONE'])

"""
Factory which generates instances of "ports", i.e. inbound/outbound channels which
the ML pipeline step uses to interact with third-party sources.

Currently supported ports:
- rabbitmq
- rabbitmq_streams
- prometheus
"""


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
        return ValueError('Invalid flow type: Must be one of [outbound, inbound]')


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
        return ValueError('Invalid flow type: Must be one of [outbound, inbound]')


def get_prometheus_port(port_name):
    pass


def _get_port_property(port_name, port_type, property_name):
    key = f"{port_name.upper()}_{_property_prefix.upper()}_{port_type.upper()}_{property_name}"
    try:
        return os.environ[key]
    except Exception:
        raise ValueError(
            f'Could not get port property for port: {port_name}, property: {property_name}: No environment variable exists named {key}')
