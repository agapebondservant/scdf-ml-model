import os
from enum import Enum
from rabbitmq.connection.rabbitmq_producer import RabbitMQProducer
from rabbitmq.connection.rabbitmq_consumer import RabbitMQConsumer
import pika
from scdfutils import utils
import logging
from datetime import datetime
import json
import nest_asyncio

nest_asyncio.apply()

_property_prefix = 'SCDF_ML_MODEL'
FlowType = Enum('FlowType', ['INBOUND', 'OUTBOUND', 'NONE'])
logger = logging.getLogger('ports')
logger.setLevel(logging.INFO)
_MONITORS = {}
"""
Factory which generates instances of "ports", i.e. inbound/outbound channels which
the ML pipeline step uses to interact with third-party sources.

Currently supported ports:
- rabbitmq
- rabbitmq_streams
- monitoring
"""


def get_outbound_control_port(**kwargs):
    if utils.get_env_var('SPRING_RABBITMQ_HOST'):
        logging.info(f"In outbound_control_port: exchange: {utils.get_env_var('OUTBOUND_PORT')}, "
                     f"routing key: {utils.get_env_var('OUTBOUND_PORT_BINDING')}")
        producer = RabbitMQProducer(host=utils.get_env_var('SPRING_RABBITMQ_HOST'),
                                    username=utils.get_env_var('SPRING_RABBITMQ_USERNAME'),
                                    password=utils.get_env_var('SPRING_RABBITMQ_PASSWORD'),
                                    virtual_host=utils.get_env_var('VIRTUAL_HOST'),
                                    # exchange='',
                                    exchange=utils.get_env_var('OUTBOUND_PORT'),
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
                                    virtual_host=utils.get_env_var('VIRTUAL_HOST'),
                                    exchange=utils.get_env_var('INBOUND_PORT'),
                                    binding_key='#',
                                    queue=utils.get_env_var('INBOUND_PORT_QUEUE'),
                                    queue_arguments={},
                                    receive_callback=receive_from_inbound_port)
        consumer.__dict__.update(**kwargs)
        consumer.start()
        return consumer
    else:
        raise ValueError('Invalid binder: Currently supports [rabbitmq]')


def send_to_outbound_port(self, _channel):
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


def get_rabbitmq_port(port_name, flow_type=FlowType.NONE, **kwargs):
    logger.info(f"In get_rabbit_port...")
    basic_properties = _get_port_basic_properties(port_name)
    uses_tap = ':' in port_name

    if flow_type is FlowType.OUTBOUND:
        destination = f"{port_name[1:]}.{utils.get_env_var('OUTBOUND_PORT_REQUIRED_GROUP')} if {uses_tap} else {_get_port_property(port_name, 'RABBITMQ', 'ROUTING_KEY')}"
        logger.info(f"In get_rabbit_port...destination {destination}")
        producer = RabbitMQProducer(host=basic_properties.get('host'),
                                    username=basic_properties.get('username'),
                                    password=basic_properties.get('password'),
                                    virtual_host=basic_properties.get('virtual_host'),
                                    exchange=utils.get_env_var('OUTBOUND_PORT') if uses_tap else _get_port_property(port_name, 'RABBITMQ', 'EXCHANGE'),
                                    routing_key=destination)
        producer.__dict__.update(**kwargs)
        producer.start()
        return producer

    elif flow_type is FlowType.INBOUND:
        queue = f"{port_name[1:]}.{utils.get_env_var('INBOUND_PORT_CONSUMER_GROUP')}" if uses_tap else _get_port_property(port_name, 'RABBITMQ', 'QUEUE')
        logger.info(f"In get_rabbit_port...destination {queue}")
        consumer = RabbitMQConsumer(host=basic_properties.get('host'),
                                    username=basic_properties.get('username'),
                                    password=basic_properties.get('password'),
                                    virtual_host=basic_properties.get('virtual_host'),
                                    exchange=utils.get_env_var('INBOUND_PORT') if uses_tap else _get_port_property(port_name, 'RABBITMQ', 'EXCHANGE'),
                                    binding_key='#',
                                    queue=queue,
                                    queue_arguments={})
        consumer.__dict__.update(**kwargs)
        # if utils.get_env_var('MONITOR_APP'):
        #    _initialize_rabbitmq_monitoring_ports(port_name, consumer)
        consumer.start()
        return consumer
    else:
        raise ValueError('Invalid flow type: Must be one of [outbound, inbound]')


def get_rabbitmq_streams_port(port_name, flow_type=FlowType.NONE, **kwargs):
    uses_tap = ':' in port_name
    basic_properties = _get_port_basic_properties(port_name)
    if flow_type is FlowType.OUTBOUND:
        destination = f"{port_name[1:]}.{utils.get_env_var('OUTBOUND_PORT_REQUIRED_GROUP')}" if uses_tap else _get_port_property(port_name, 'RABBITMQ', 'ROUTING_KEY')
        logger.info(f"In get_rabbit_streams_port...destination {destination}")
        producer = RabbitMQProducer(host=basic_properties.get('host'),
                                    username=basic_properties.get('username'),
                                    password=basic_properties.get('password'),
                                    virtual_host=basic_properties.get('virtual_host'),
                                    # exchange='' if uses_tap else _get_port_property(port_name, 'RABBITMQ', 'EXCHANGE'),
                                    exchange=utils.get_env_var('OUTBOUND_PORT') if uses_tap else _get_port_property(port_name, 'RABBITMQ', 'EXCHANGE'),
                                    routing_key=destination)
        producer.__dict__.update(**kwargs)
        producer.start()
        return producer

    elif flow_type is FlowType.INBOUND:
        queue = f"{port_name[1:]}.{utils.get_env_var('INBOUND_PORT_CONSUMER_GROUP')}" if uses_tap else _get_port_property(port_name, 'RABBITMQ', 'QUEUE')
        logger.info(f"In get_rabbit_streams_port...destination {queue}")
        consumer = RabbitMQConsumer(host=basic_properties.get('host'),
                                    username=basic_properties.get('username'),
                                    password=basic_properties.get('password'),
                                    virtual_host=basic_properties.get('virtual_host'),
                                    exchange=utils.get_env_var('INBOUND_PORT') if uses_tap else _get_port_property(port_name, 'RABBITMQ', 'EXCHANGE'),
                                    binding_key='#',
                                    queue=queue,
                                    queue_arguments={'x-queue-type': 'stream'})
        consumer.__dict__.update(**kwargs)
        # if utils.get_env_var('MONITOR_APP'):
        #    _initialize_rabbitmq_monitoring_ports(port_name, consumer)
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

"""
#####################################
# Monitoring ports (used internally)
#####################################
"""


def get_inbound_rabbitmq_monitoring_port(port_name, **kwargs):
    logger.info(f"In _initialize_rabbitmq_monitoring_ports...port name {port_name}")
    uses_tap = ':' in port_name
    queue = f"{port_name[1:]}.{utils.get_env_var('INBOUND_PORT_CONSUMER_GROUP')}.monitor" if uses_tap else f"{_get_port_property(port_name, 'RABBITMQ', 'QUEUE')}.monitor"
    logger.info(f"will consume from...{queue}")

    if utils.get_env_var('MONITOR_APP'):
        basic_properties = _get_port_basic_properties(port_name)
        consumer_monitor = RabbitMQConsumer(host=basic_properties.get('host'),
                                            username=basic_properties.get('username'),
                                            password=basic_properties.get('password'),
                                            virtual_host=basic_properties.get('virtual_host'),
                                            queue=queue,
                                            exchange=utils.get_env_var('INBOUND_PORT'),
                                            binding_key='#',
                                            queue_arguments={'x-queue-type': 'stream'},
                                            receive_callback=utils.generate_mlflow_data_monitoring_current_dataset,)
        consumer_monitor.__dict__.update(**kwargs)
        consumer_monitor.start()
    else:
        logger.error("ERROR: Access to rabbitmq monitoring port not permitted")


def _initialize_rabbitmq_monitoring_ports(port_name, consumer):
    logger.info(f"In _initialize_rabbitmq_monitoring_ports...port name {port_name}")
    uses_tap = ':' in port_name
    destination = f"{port_name[1:]}.{utils.get_env_var('INBOUND_PORT_CONSUMER_GROUP')}.monitor" if uses_tap else f"{_get_port_property(port_name, 'RABBITMQ', 'QUEUE')}.monitor"
    logger.info(f"will publish to...{destination}")
    global _MONITORS
    basic_properties = _get_port_basic_properties(port_name)
    producer_monitor = RabbitMQProducer(host=basic_properties.get('host'),
                                        username=basic_properties.get('username'),
                                        password=basic_properties.get('password'),
                                        virtual_host=basic_properties.get('virtual_host'),
                                        exchange='',
                                        routing_key=destination)
    producer_monitor.start()
    _MONITORS[port_name] = producer_monitor

    # Normally would use SCDF tapping to consume from the consumer's source, but SCDF does not currently support all queue types
    # Hence, instead this will monkey-patch the consumer's receive_callback to send to the monitoring sink
    consumer.__dict__.update(**{'old_receive_callback': consumer.receive_callback,
                                'port_name': port_name})
    consumer.receive_callback = _receive_callback


def _get_port_basic_properties(port_name):
    uses_tap = ':' in port_name
    host = utils.get_env_var('SPRING_RABBITMQ_HOST') if uses_tap else _get_port_property(port_name, 'RABBITMQ', 'HOST')
    username = utils.get_env_var('SPRING_RABBITMQ_USERNAME') if uses_tap else _get_port_property(port_name, 'RABBITMQ', 'USERNAME')
    password = utils.get_env_var('SPRING_RABBITMQ_PASSWORD') if uses_tap else _get_port_property(port_name, 'RABBITMQ', 'PASSWORD')
    virtual_host = utils.get_env_var('VIRTUAL_HOST') if uses_tap else _get_port_property(port_name, 'RABBITMQ', 'VIRTUAL_HOST')
    properties = {'host': host,
                  'username': username,
                  'password': password,
                  'virtual_host': virtual_host}
    return properties


def _get_port_property(port_name, port_type, property_name):
    key = f"{port_name.upper()}_{_property_prefix.upper()}_{port_type.upper()}_{property_name}"
    try:
        return os.environ[key]
    except Exception:
        raise ValueError(
            f'Could not get port property for port: {port_name}, property: {property_name}: No environment variable exists named {key}')


def _receive_callback(self, _, data):
    logger.info("In monkeypatched receive_callback method (_receive_callback)...")
    if data:
        self.old_receive_callback(_, data)
        if self.port_name and _MONITORS.get(self.port_name):
            logger.info(f"Sending monitoring data for {self.port_name}...")
            _MONITORS[self.port_name].send_data(data)
        else:
            logger.info(f"No monitoring port exists for {self.port_name}")
    else:
        logger.info(f"Data was blank, could not process")
