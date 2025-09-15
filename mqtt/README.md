# MQTT Data Source Connectors for Pyspark
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Enabled-00A1C9?style=for-the-badge)](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
[![Serverless](https://img.shields.io/badge/Serverless-Compute-00C851?style=for-the-badge)](https://docs.databricks.com/en/compute/serverless.html)
# Databricks Python Data Sources

Introduced in Spark 4.x, Python Data Source API allows you to create PySpark Data Sources leveraging long standing python libraries for handling unique file types or specialized interfaces with spark read, readStream, write and writeStream APIs.

| Data Source Name | Purpose |
| --- | --- |
| [MQTT](https://pypi.org/project/paho-mqtt/) | Read MQTT messages from a broker |

---

## Configuration Options

The MQTT data source supports the following configuration options, which can be set via Spark options or environment variables:

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| `broker_address` | Hostname or IP address of the MQTT broker | Yes | - |
| `port` | Port number of the MQTT broker | No | 8883 |
| `username` | Username for broker authentication | No | "" |
| `password` | Password for broker authentication | No | "" |
| `topic` | MQTT topic to subscribe/publish to | No | "#" |
| `qos` | Quality of Service level (0, 1, or 2) | No | 0 |
| `require_tls` | Enable SSL/TLS (true/false) | No | true |
| `keepalive` | Keep alive interval in seconds | No | 60 |
| `clean_session` | Clean session flag (true/false) | No | false |
| `conn_time` | Connection timeout in seconds | No | 1 |
| `ca_certs` | Path to CA certificate file | No | - |
| `certfile` | Path to client certificate file | No | - |
| `keyfile` | Path to client key file | No | - |
| `tls_disable_certs` | Disable certificate verification | No | - |

You can set these options in your PySpark code, for example:
```python
display(
  spark.readStream.format("mqtt_pub_sub")
  .option("topic", "#")
  .option("broker_address", "host")
  .option("username", "secret_user")
  .option("password", "secret_password")
  .option("qos", 2)
  .option("require_tls", False)
  .load()
)
```

---

## Building and Running Tests

* Clone repo
* Create Virtual environment (Python 3.11)
* Ensure Docker/Podman is installed and properly configured 
* Spin up a Docker container for a local MQTT Server:
```yaml
version: "3.7"
services:
  mqtt5:
    userns_mode: keep-id
    image: eclipse-mosquitto
    container_name: mqtt5
    ports:
      - "1883:1883" # default mqtt port
      - "9001:9001" # default mqtt port for websockets
    volumes:
      - ./config:/mosquitto/config:rw
      - ./data:/mosquitto/data:rw
      - ./log:/mosquitto/log:rw
    restart: unless-stopped
```

* Create .env file at the project root directory:
```dotenv
MQTT_BROKER_HOST=
MQTT_BROKER_PORT=
MQTT_USERNAME=
MQTT_PASSWORD=
MQTT_BROKER_TOPIC_PREFIX=
```

* Run tests from project root directory
```shell
make test
```

* Build package
```shell
python -m build
```

---

## Example Usage

```python
spark.dataSource.register(MqttDataSource)

display(
  spark.readStream.format("mqtt_pub_sub")
  .option("topic", "#")
  .option("broker_address", "host")
  .option("username", "secret_user")
  .option("password", "secret_password")
  .option("qos", 2)
  .option("require_tls", False)
  .load()
)

df.writeStream.format("console").start().awaitTermination()
```

---

## Project Support

The code in this project is provided **for exploration purposes only** and is **not formally supported** by Databricks under any Service Level Agreements (SLAs). It is provided **AS-IS**, without any warranties or guarantees.  

Please **do not submit support tickets** to Databricks for issues related to the use of this project.  

The source code provided is subject to the Databricks [LICENSE](https://github.com/databricks-industry-solutions/python-data-sources/blob/main/LICENSE.md) . All third-party libraries included or referenced are subject to their respective licenses set forth in the project license.  

Any issues or bugs found should be submitted as **GitHub Issues** on the project repository. While these will be reviewed as time permits, there are **no formal SLAs** for support.

## 📄 Third-Party Package Licenses

&copy; 2025 Databricks, Inc. All rights reserved. The source in this project is provided subject to the Databricks License [https://databricks.com/db-license-source]. All included or referenced third party libraries are subject to the licenses set forth below.

| Datasource | Package    | Purpose                           | License     | Source                               |
| ---------- | ---------- | --------------------------------- | ----------- | ------------------------------------ |
| paho-mqtt     | paho-mqtt	  | Python api for mqtt        |	EPL-v20	        | https://pypi.org/project/paho-mqtt/   |

## References

- [Paho MQTT Python Client](https://pypi.org/project/paho-mqtt/)
- [Eclipse Mosquitto](https://mosquitto.org/)
- [Databricks Python Data Source API](https://docs.databricks.com/en/data-engineering/data-sources/python-data-sources.html)