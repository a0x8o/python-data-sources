import datetime
import logging
import random
import subprocess
import sys
import time

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
    TODO: add docs
    """

    @classmethod
    def name(cls):
        """Returns the name of the data source."""
        return "mqtt_pub_sub"

    def __init__(self, options):
        """
        TODO: docs
        """
        self.options = options

    def schema(self):
        """
        TODO: add docs
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
        TODO: add docs
        """
        return MqttSimpleStreamReader(schema, self.options)


class MqttSimpleStreamReader(SimpleDataSourceStreamReader):

    def __init__(self, schema, options):
        """
        TODO: add docs
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
        TODO: add docs
        """
        return topic_str

    def initialOffset(self):
        """
        TODO: add docs
        """
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
        # stat_data = []
        # message_count = 0
        # max_messages = 10  # Maximum number of messages to wait for

        import paho.mqtt.client as mqttClient

        def _get_mqtt_client():
            return mqttClient.Client(mqttClient.CallbackAPIVersion.VERSION1, self.client_id,
                                     clean_session=self.clean_session)

        client = _get_mqtt_client()
        if self.require_tls:
            client.tls_set()

        client.username_pw_set(self.username, self.password)

        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                client.subscribe(self.topic, qos=self.qos)
                logger.warning(f"Connected to broker")
            else:
                logger.warning("Connection failed")

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
        # TODO - Might want to double-check if this is necessary
        try:
            client.connect(self.broker_address, self.port, self.keep_alive)
        except Exception as e:
            logger.exception("Failed to connect to broker.", exc_info=e)
            raise
        client.loop_start()  # Use loop_start to run the loop in a separate thread

        time.sleep(self.conn_timeout)  # Wait for messages for the specified timeout

        client.loop_stop()  # Stop the loop after the timeout
        client.disconnect()
        logger.warning("current state of data: %s", self.new_data)

        return (iter(self.new_data))