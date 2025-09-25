import datetime
import logging
import random
import subprocess
import sys
import time

from pyspark.errors import PySparkException
from pyspark.sql.datasource import DataSource, InputPartition, SimpleDataSourceStreamReader
from pyspark.sql.types import StructType, StructField, StringType

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class RangePartition(InputPartition):
    def __init__(self, start, end):
        self.start = start
        self.end = end


class MqttDataSource(DataSource):
    """
    A PySpark DataSource for reading MQTT messages from a broker.
    
    This data source allows you to stream MQTT messages into Spark DataFrames,
    supporting various MQTT broker configurations including authentication,
    SSL/TLS encryption, and different quality of service levels.
    
    Supported options:
    - broker_address: MQTT broker hostname or IP address (required)
    - port: Broker port number (default: 8883)
    - username: Authentication username (optional)
    - password: Authentication password (optional)
    - topic: MQTT topic to subscribe to (default: "#" for all topics)
    - qos: Quality of Service level 0-2 (default: 0)
    - require_tls: Enable SSL/TLS encryption (default: true)
    - keepalive: Keep alive interval in seconds (default: 60)
    
    Example usage:
        spark.readStream.format("mqtt_pub_sub")
            .option("broker_address", "mqtt.example.com")
            .option("topic", "sensors/+/temperature")
            .option("username", "user")
            .option("password", "pass")
            .load()
    """

    @classmethod
    def name(cls):
        """Returns the name of the data source."""
        return "mqtt_pub_sub"

    def __init__(self, options):
        """
        Initialize the MQTT data source with configuration options.
        
        Args:
            options (dict): Configuration options for the MQTT connection.
                          See class docstring for supported options.
        """
        self.options = options

    def schema(self):
        """
        Define the schema of the data source.
        
        Returns:
            StructType: The schema of the data source.
        """
        return StructType([
            StructField("received_time", StringType(), True),
            StructField("topic", StringType(), True),
            StructField("message", StringType(), True),
            StructField("is_duplicate", StringType(), True),
            StructField("qos", StringType(), True),
            StructField("is_retained", StringType(), True)
        ])

    def streamReader(self, schema: StructType):
        """
        Create and return a stream reader for MQTT data.
        
        Args:
            schema (StructType): The schema for the streaming data.
            
        Returns:
            MqttSimpleStreamReader: A stream reader instance configured for MQTT.
        """
        return MqttSimpleStreamReader(schema, self.options)


class MqttSimpleStreamReader(SimpleDataSourceStreamReader):

    def __init__(self, schema, options):
        """
        Initialize the MQTT simple stream reader with configuration options.
        
        Args:
            schema (StructType): The schema for the streaming data.
            options (dict): Configuration options for the MQTT connection.
                          See class docstring for supported options.
        """
        self._install_paho_mqtt()
        super().__init__()
        self.topic = self._parse_topic(options.get("topic", "#"))
        self.broker_address = options.get("broker_address")
        str_tls = options.get("require_tls", True).lower()
        self.require_tls = True if str_tls == "true" else False
        self.port = int(options.get("port", 8883))
        self.username = options.get("username", "")
        self.password = options.get("password", "")
        self.qos = int(options.get("qos", 0))
        self.keep_alive = int(options.get("keepalive", 60))
        self.clean_session = options.get("clean_session", False)
        self.conn_timeout = int(options.get("conn_time", 1))
        self.clean_sesion = options.get("clean_sesion", False)
        self.ca_certs = options.get("ca_certs", None)
        self.certfile = options.get("certfile", None)
        self.keyfile = options.get("keyfile", None)
        self.tls_disable_certs = options.get("tls_disable_certs", None)
        if self.clean_sesion not in [True, False]:
            raise ValueError(f"Unsupported sesion: {self.clean_sesion}")
        self.client_id = f'spark-data-source-mqtt-{random.randint(0, 1000000)}'
        self.current = 0
        self.new_data = []

    def _install_paho_mqtt(self):
        try:
            import paho.mqtt.client
        except ImportError:
            logger.warn("Installing paho-mqtt...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", "paho-mqtt"])
            # importlib.reload(sys.modules[__name__])

    def _parse_topic(self, topic_str: str):
        """
        TODO: add docs, implement parsing of topic string
        """
        return topic_str

    def _configure_tls(self, client):
        """
        Configure TLS settings on the MQTT client based on provided certificate options.
        """
        if self.require_tls:
            # Build tls_set arguments based on provided certificates
            tls_args = {}
            
            if self.ca_certs:
                tls_args['ca_certs'] = self.ca_certs
            
            if self.certfile:
                tls_args['certfile'] = self.certfile
                
            if self.keyfile:
                tls_args['keyfile'] = self.keyfile
            
            # Call tls_set with the appropriate parameters
            if tls_args:
                client.tls_set(**tls_args)
                logger.info(f"TLS configured with certificates: {list(tls_args.keys())}")
            else:
                # Basic TLS without custom certificates
                client.tls_set()
                logger.info("Basic TLS enabled")
        else:
            logger.info("TLS disabled")

    def initialOffset(self):
        return {"offset": 0}

    def latestOffset(self) -> dict:
        """
        Returns the current latest offset that the next microbatch will read to.
        """
        self.current += 1
        return {"offset": self.current}

    def partitions(self, start: dict, end: dict):

        """
        Plans the partitioning of the current microbatch defined by start and end offset. It
        needs to return a sequence of :class:`InputPartition` objects.
        """
        return [RangePartition(start["offset"], end["offset"])]

    def read(self, partition):
        """
        Read MQTT messages from the broker.
        
        Args:
            partition (RangePartition): The partition to read from.
            
        Returns:
            Iterator[list]: An iterator of lists containing the MQTT message data.
            The list contains the following elements:
            - received_time: The time the message was received.
            - topic: The topic of the message.
            - message: The payload of the message.
            - is_duplicate: Whether the message is a duplicate.
            - qos: The quality of service level of the message.
            - is_retained: Whether the message is retained.
            
        Raises:
            Exception: If the connection to the broker fails.
        """
        import paho.mqtt.client as mqttClient

        def _get_mqtt_client():
            return mqttClient.Client(mqttClient.CallbackAPIVersion.VERSION1, self.client_id,
                                     clean_session=self.clean_session)

        client = _get_mqtt_client()
        
        # Configure TLS with certificates if provided
        self._configure_tls(client)
        
        client.username_pw_set(self.username, self.password)

        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                client.subscribe(self.topic, qos=self.qos)
                logger.warning(f"Connected to broker {self.broker_address} on port {self.port} with topic {self.topic}")
            else:
                logger.error(f"Connection failed to broker {self.broker_address} on port {self.port} with topic {self.topic}")

        def on_message(client, userdata, message):
            msg_data = [
                str(datetime.datetime.now()),
                message.topic,
                str(message.payload.decode("utf-8", "ignore")),
                message.dup,
                message.qos,
                message.retain
            ]
            logger.warning(msg_data)
            self.new_data.append(msg_data)

        client.on_connect = on_connect
        client.on_message = on_message

        try:
            client.connect(self.broker_address, self.port, self.keep_alive)
        except Exception as e:
            connection_context = {
                "broker_address": self.broker_address,
                "port": self.port,
                "topic": self.topic,
                "client_id": self.client_id,
                "require_tls": self.require_tls,
                "keepalive": self.keep_alive,
                "qos": self.qos,
                "clean_session": self.clean_session,
                "conn_timeout": self.conn_timeout
            }
            
            error_msg = f"Failed to connect to MQTT broker. Connection details: {connection_context}"
            logger.exception(error_msg, exc_info=e)
            
            # Re-raise with enhanced context
            raise ConnectionError(error_msg) from e
        client.loop_start()  # Use loop_start to run the loop in a separate thread

        time.sleep(self.conn_timeout)  # Wait for messages for the specified timeout

        client.loop_stop()  # Stop the loop after the timeout
        client.disconnect()
        logger.warning("current state of data: %s", self.new_data)

        return (iter(self.new_data))




class MqttSimpleStreamWriter():
    #To be implemented
    def __init__(self, schema, options):
        pass
