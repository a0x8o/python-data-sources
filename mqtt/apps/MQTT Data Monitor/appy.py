from flask import Flask, request, jsonify, render_template_string
import logging
from datetime import datetime, timedelta
import pandas
from databricks import sql
from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
import os
import json
import time
import paho.mqtt.client as mqtt_client


log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

flask_app = Flask(__name__)


client = WorkspaceClient()

# Database connection function
def get_data(query, warehouse_id):
    """Execute query with fallback to demo data"""
    try:
        cfg = Config()
        if warehouse_id and cfg.host:
            with sql.connect(
                server_hostname=cfg.host,
                http_path=f"/sql/1.0/warehouses/{warehouse_id}",
                credentials_provider=lambda: cfg.authenticate
            ) as connection:
                df = pandas.read_sql(query, connection)
                # Convert DataFrame to list of dictionaries
                return df.to_dict('records')
    except Exception as e:
        print(f"Database query failed: {e}")
        # Return empty list on error so we can show a message
        return []


def create_job(client, notebook_path, cluster_id):
    # cluster_id = (
    #     w.clusters.ensure_cluster_is_running(os.environ["DATABRICKS_CLUSTER_ID"]) and os.environ["DATABRICKS_CLUSTER_ID"]
    # )

    created_job = client.jobs.create(
        name=f"mqtt_{time.time_ns()}",
        tasks=[
            jobs.Task(
                description="mqtt",
                notebook_task=jobs.NotebookTask(notebook_path=notebook_path, source=jobs.Source("WORKSPACE")),
                task_key="mqtt",
                timeout_seconds=0,
                existing_cluster_id=cluster_id,
            )
        ],
    )
    return created_job

def run_job(client, created_job, mqtt_config):
    """Run the job with user-provided MQTT configuration"""
    run = client.jobs.run_now(
        job_id=created_job.job_id,
        notebook_params={
            "catalog": mqtt_config.get('catalog', 'dbdemos'),
            "database": mqtt_config.get('schema', 'dbdemos_mqtt'),
            "table": mqtt_config.get('table', 'mqtt_v5'),
            "broker": mqtt_config.get('broker_address', ''),
            "port": mqtt_config.get('port', '8883'),
            "username": mqtt_config.get('username', ''),
            "password": mqtt_config.get('password', ''),
            "topic": mqtt_config.get('topic', '#'),
            "qos": mqtt_config.get('qos', '0'),
            "require_tls": mqtt_config.get('require_tls', 'false'),
            "keepalive": mqtt_config.get('keepalive', '60'),
        }
    )
    return run


def mqtt_remote_client(mqtt_server_config):
    """Test MQTT connection with provided configuration"""
    connection_result = {
        'success': False,
        'message': '',
        'error': None
    }
    try:
        # Callback function for when the client connects
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                print("Connected successfully to MQTT Broker!")
            else:
                print(f"Failed to connect, return code {rc}\n")

        client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1, client_id="mqtt_connection_test", clean_session=True)
        
        # Set username and password if provided
        if mqtt_server_config.get("username") and mqtt_server_config.get("password"):
            client.username_pw_set(
                username=mqtt_server_config["username"], 
                password=mqtt_server_config["password"]
            )
        
        # Set TLS if required
        if mqtt_server_config.get("require_tls") == 'true':
            import ssl
            tls_config = {}
            if mqtt_server_config.get("ca_certs"):
                tls_config['ca_certs'] = mqtt_server_config["ca_certs"]
            if mqtt_server_config.get("certfile"):
                tls_config['certfile'] = mqtt_server_config["certfile"]
            if mqtt_server_config.get("keyfile"):
                tls_config['keyfile'] = mqtt_server_config["keyfile"]
            
            # If disable certs verification is checked
            if mqtt_server_config.get("tls_disable_certs") == 'true':
                tls_config['cert_reqs'] = ssl.CERT_NONE
            
            client.tls_set(**tls_config)
        
        client.on_connect = on_connect
        
        # Attempt connection
        port = int(mqtt_server_config.get("port", 8883))
        keepalive = int(mqtt_server_config.get("keepalive", 60))
        
        client.connect(mqtt_server_config["host"], port, keepalive)
        
        # Start the loop to process network traffic
        client.loop_start()
        
        # Give it a moment to connect
        time.sleep(2)
        
        # Check if connected
        if client.is_connected():
            connection_result['success'] = True
            connection_result['message'] = f'Successfully connected to MQTT broker at {mqtt_server_config["host"]}:{port}'
        else:
            connection_result['message'] = 'Failed to connect to MQTT broker'
            connection_result['error'] = f'Failed to connect to {mqtt_server_config["host"]} and {port} and {mqtt_server_config["username"]} are not working'
        
        # Disconnect
        client.loop_stop()
        client.disconnect()
        
    except Exception as e:
        connection_result['success'] = False
        connection_result['message'] = 'Connection failed'
        connection_result['error'] = str(e)
    
    return connection_result


# Initialize with empty data - will be loaded when user specifies catalog/schema/table and clicks refresh
curr_data = []


def get_mqtt_stats():
    """Get MQTT message statistics from data"""
    if not curr_data or len(curr_data) == 0:
        return {
            'critical': 0,
            'in_progress': 0,
            'resolved_today': 0,
            'avg_response_time': 0
        }
    
    # Count duplicated messages
    duplicated = len([a for a in curr_data if str(a.get('is_duplicate', 'false')).lower() == 'true'])
    # Count QoS 2 messages
    qos2_messages = len([a for a in curr_data if str(a.get('qos', 0)) == '2'])
    # Count unique topics
    unique_topics = len(set([str(a.get('topic', '')) for a in curr_data]))
    # Total row count
    row_count = len(curr_data)
    
    return {
        'critical': duplicated,
        'in_progress': qos2_messages,
        'resolved_today': unique_topics,
        'avg_response_time': row_count
    }


@flask_app.route('/api/test-mqtt-connection', methods=['POST'])
def test_mqtt_connection():
    """API endpoint to test MQTT broker connection"""
    try:
        # Get the configuration from the request
        mqtt_config = request.json
        
        # Validate required fields
        if not mqtt_config.get('broker_address'):
            return jsonify({
                'success': False,
                'error': 'Broker address is required'
            }), 400
        
        # Prepare config for MQTT test
        mqtt_server_config = {
            'host': mqtt_config.get('broker_address'),
            'port': mqtt_config.get('port', '1883'),
            'username': mqtt_config.get('username', ''),
            'password': mqtt_config.get('password', ''),
            'require_tls': mqtt_config.get('require_tls', 'false'),
            'ca_certs': mqtt_config.get('ca_certs', ''),
            'certfile': mqtt_config.get('certfile', ''),
            'keyfile': mqtt_config.get('keyfile', ''),
            'tls_disable_certs': mqtt_config.get('tls_disable_certs', 'false'),
            'keepalive': mqtt_config.get('keepalive', '60')
        }
        # Test the connection
        result = mqtt_remote_client(mqtt_server_config)
        
        if result['success']:
            return jsonify(result), 200
        else:
            return jsonify(result), 400
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': 'Connection test failed',
            'error': str(e)
        }), 500


@flask_app.route('/api/start-mqtt-job', methods=['POST'])
def start_mqtt_job():
    """API endpoint to start the MQTT data ingestion job with user configuration"""
    try:
        # Get the configuration from the request
        mqtt_config = request.json
        
        # Validate required fields
        if not mqtt_config.get('broker_address'):
            return jsonify({
                'success': False,
                'error': 'Broker address is required'
            }), 400
        
        if not mqtt_config.get('catalog') or not mqtt_config.get('schema') or not mqtt_config.get('table'):
            return jsonify({
                'success': False,
                'error': 'Catalog, Schema, and Table name are required'
            }), 400
        
        # Get notebook_path and cluster_id from config
        notebook_path = mqtt_config.get('notebook_path', '/Workspace/Users/jeffery.annor@databricks.com/mqtt/MQTT_data_source_v0')
        cluster_id = mqtt_config.get('cluster_id', '0709-132523-cnhxf2p6')
        
        # Create the job
        created_job = create_job(client, notebook_path, cluster_id)
        
        # Run the job with user configuration
        run = run_job(client, created_job, mqtt_config)
        
        catalog = mqtt_config.get('catalog')
        schema = mqtt_config.get('schema')
        table = mqtt_config.get('table')
        
        return jsonify({
            'success': True,
            'job_id': created_job.job_id,
            'run_id': run.run_id,
            'message': f'MQTT data ingestion job started successfully. Data will be written to {catalog}.{schema}.{table}'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@flask_app.route('/api/refresh-data', methods=['POST'])
def refresh_data():
    """API endpoint to refresh dashboard data from specified table"""
    global curr_data
    try:
        data = request.json
        catalog = data.get('catalog')
        schema = data.get('schema')
        table = data.get('table')
        # warehouse_id = data.get('warehouse_id', DEFAULT_WAREHOUSE_ID)
        
        # Validate required fields
        if not catalog or not schema or not table:
            return jsonify({
                'success': False,
                'error': 'Catalog, Schema, and Table name are required'
            }), 400
        
        # Build the query
        query = f"SELECT message, is_duplicate, qos, topic, received_time FROM {catalog}.{schema}.{table} ORDER BY received_time DESC LIMIT 100"
        
        # Fetch data using get_data function
        curr_data = get_data(query, "4b9b953939869799")
        
        # Calculate stats from refreshed data
        stats = get_mqtt_stats()
        
        return jsonify({
            'success': True,
            'message': f'Data refreshed from {catalog}.{schema}.{table}',
            'row_count': len(curr_data) if curr_data else 0,
            'data': curr_data,  # Return the actual data to update the UI
            'stats': stats  # Return the calculated stats
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@flask_app.route('/')
def dashboard():
    """Main MQTT Data Monitor dashboard page"""
    stats = get_mqtt_stats()
    
    html = '''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>MQTT Data Monitor</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <script>
            tailwind.config = {
                theme: {
                    extend: {
                        colors: {
                            // MQTT Broker Theme - Inspired by HiveMQ & Azure Event Grid
                            'mqtt-primary': '#ffc000',      // HiveMQ Yellow
                            'mqtt-secondary': '#037da5',    // HiveMQ Teal
                            'mqtt-azure': '#0078D4',        // Azure Blue
                            'mqtt-dark': '#1a1a1a',         // Dark Gray
                            'mqtt-success': '#00a86b',      // Success Green
                            'mqtt-warning': '#ff8c00',      // Warning Orange
                            'mqtt-error': '#e74c3c',        // Error Red
                            'mqtt-info': '#0078D4',         // Info Blue
                            'mqtt-bg': '#f8f9fa',           // Light Background
                            'mqtt-card': '#ffffff',         // Card Background
                            'mqtt-border': '#e1e5e9',       // Border Color
                            'mqtt-text': '#2c3e50',         // Text Color
                            'mqtt-text-light': '#6c757d'    // Light Text
                        }
                    }
                }
            }
        </script>
        <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    </head>
    <body class="bg-mqtt-bg min-h-screen flex flex-col">
        <!-- Header -->
        <header class="bg-mqtt-card shadow-sm border-b border-mqtt-border">
            <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
                <div class="flex justify-between items-center py-4">
                    <div class="flex items-center">
                        <div class="flex-shrink-0">
                            <h1 class="text-2xl font-bold text-mqtt-azure">
                                <i class="fas fa-broadcast-tower mr-2"></i>MQTT Data Monitor
                            </h1>
                            <p class="text-sm text-mqtt-text-light">Real-time MQTT Message Monitoring & Analytics</p>
                        </div>
                    </div>
                    <div class="flex items-center space-x-4">
                        <div class="relative">
                            <input type="text" id="searchInput" placeholder="Search MQTT topics..." 
                                   class="w-64 px-4 py-2 border border-mqtt-border rounded-lg focus:ring-2 focus:ring-mqtt-azure focus:border-transparent">
                            <i class="fas fa-search absolute right-3 top-3 text-mqtt-text-light"></i>
                        </div>
                        <button id="refreshDataBtn" class="bg-mqtt-azure text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition-colors">
                            <i class="fas fa-sync mr-2"></i>Refresh
                        </button>
                    </div>
                </div>
            </div>
        </header>

        <!-- Main Content -->
        <main class="flex-1 max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
            <!-- Tab Navigation -->
            <div class="mb-8">
                <nav class="flex space-x-8" aria-label="Tabs">
                    <button id="connectionTab" class="tab-button active border-b-2 border-mqtt-azure text-mqtt-azure py-2 px-1 text-sm font-medium">
                        <i class="fas fa-plug mr-2"></i>MQTT Connection
                    </button>
                    <button id="dashboardTab" class="tab-button border-b-2 border-transparent text-mqtt-text-light hover:text-mqtt-text hover:border-mqtt-border py-2 px-1 text-sm font-medium">
                        <i class="fas fa-chart-line mr-2"></i>Dashboard
                    </button>
                </nav>
            </div>

            <!-- Tab Content -->
            <div id="connectionContent" class="tab-content">
                <!-- MQTT Connection Configuration -->
                <div class="bg-mqtt-card rounded-lg shadow-lg p-8">
                    <div class="mb-6">
                        <h2 class="text-2xl font-bold text-mqtt-text mb-2">
                            <i class="fas fa-broadcast-tower mr-3 text-mqtt-azure"></i>MQTT Broker Configuration
                        </h2>
                        <p class="text-mqtt-text-light">Configure your MQTT broker connection settings to start monitoring messages.</p>
                    </div>

                    <form id="mqttConfigForm" class="space-y-6">
                        <!-- Connection Details Section -->
                        <div class="border-b border-mqtt-border pb-6">
                            <h3 class="text-lg font-medium text-mqtt-text mb-4">Connection Details</h3>
                            <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
                                <div>
                                    <label for="broker_address" class="block text-sm font-medium text-mqtt-text mb-2">
                                        Broker Address <span class="text-mqtt-error">*</span>
                                    </label>
                                    <input type="text" id="broker_address" name="broker_address" required
                                           placeholder="mqtt.example.com or 192.168.1.100"
                                           class="w-full px-3 py-2 border border-mqtt-border rounded-lg focus:ring-2 focus:ring-mqtt-azure focus:border-transparent">
                                </div>
                                <div>
                                    <label for="port" class="block text-sm font-medium text-mqtt-text mb-2">Port</label>
                                    <input type="number" id="port" name="port" value="8883" min="1" max="65535"
                                           class="w-full px-3 py-2 border border-mqtt-border rounded-lg focus:ring-2 focus:ring-mqtt-azure focus:border-transparent">
                                    <p class="text-xs text-mqtt-text-light mt-1">1883 (non-TLS) or 8883 (TLS)</p>
                                </div>
                            </div>
                        </div>

                        <!-- Authentication Section -->
                        <div class="border-b border-mqtt-border pb-6">
                            <h3 class="text-lg font-medium text-mqtt-text mb-4">Authentication</h3>
                            <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
                                <div>
                                    <label for="username" class="block text-sm font-medium text-mqtt-text mb-2">Username</label>
                                    <input type="text" id="username" name="username" 
                                           placeholder="Optional username"
                                           class="w-full px-3 py-2 border border-mqtt-border rounded-lg focus:ring-2 focus:ring-mqtt-azure focus:border-transparent">
                                </div>
                                <div>
                                    <label for="password" class="block text-sm font-medium text-mqtt-text mb-2">Password</label>
                                    <input type="password" id="password" name="password"
                                           placeholder="Optional password"
                                           class="w-full px-3 py-2 border border-mqtt-border rounded-lg focus:ring-2 focus:ring-mqtt-azure focus:border-transparent">
                                </div>
                            </div>
                        </div>

                        <!-- Topic & QoS Section -->
                        <div class="border-b border-mqtt-border pb-6">
                            <h3 class="text-lg font-medium text-mqtt-text mb-4">Topic Configuration</h3>
                            <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
                                <div>
                                    <label for="topic" class="block text-sm font-medium text-mqtt-text mb-2">Topic</label>
                                    <input type="text" id="topic" name="topic" value="#"
                                           placeholder="# (subscribe to all topics)"
                                           class="w-full px-3 py-2 border border-mqtt-border rounded-lg focus:ring-2 focus:ring-mqtt-azure focus:border-transparent">
                                    <p class="text-xs text-mqtt-text-light mt-1">Use # for all topics, + for single level wildcard</p>
                                </div>
                                <div>
                                    <label for="qos" class="block text-sm font-medium text-mqtt-text mb-2">Quality of Service</label>
                                    <select id="qos" name="qos" 
                                            class="w-full px-3 py-2 border border-mqtt-border rounded-lg focus:ring-2 focus:ring-mqtt-azure focus:border-transparent">
                                        <option value="0">0 - At most once</option>
                                        <option value="1">1 - At least once</option>
                                        <option value="2">2 - Exactly once</option>
                                    </select>
                                </div>
                            </div>
                        </div>

                        <!-- Security & TLS Section -->
                        <div class="border-b border-mqtt-border pb-6">
                            <h3 class="text-lg font-medium text-mqtt-text mb-4">Security Settings</h3>
                            <div class="space-y-4">
                                <div class="flex items-center">
                                    <input type="checkbox" id="require_tls" name="require_tls"
                                           class="h-4 w-4 text-mqtt-azure focus:ring-mqtt-azure border-mqtt-border rounded">
                                    <label for="require_tls" class="ml-2 block text-sm text-mqtt-text">
                                        Enable SSL/TLS encryption
                                    </label>
                                </div>
                                <div id="tlsSettings" class="mt-4 hidden">
                                    <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                                        <div>
                                            <label for="ca_certs" class="block text-sm font-medium text-mqtt-text mb-2">CA Certificate Path</label>
                                            <input type="text" id="ca_certs" name="ca_certs"
                                                   placeholder="/path/to/ca.crt"
                                                   class="w-full px-3 py-2 border border-mqtt-border rounded-lg focus:ring-2 focus:ring-mqtt-azure focus:border-transparent">
                                        </div>
                                        <div>
                                            <label for="certfile" class="block text-sm font-medium text-mqtt-text mb-2">Client Certificate</label>
                                            <input type="text" id="certfile" name="certfile"
                                                   placeholder="/path/to/client.crt"
                                                   class="w-full px-3 py-2 border border-mqtt-border rounded-lg focus:ring-2 focus:ring-mqtt-azure focus:border-transparent">
                                        </div>
                                        <div>
                                            <label for="keyfile" class="block text-sm font-medium text-mqtt-text mb-2">Client Key</label>
                                            <input type="text" id="keyfile" name="keyfile"
                                                   placeholder="/path/to/client.key"
                                                   class="w-full px-3 py-2 border border-mqtt-border rounded-lg focus:ring-2 focus:ring-mqtt-azure focus:border-transparent">
                                        </div>
                                    </div>
                                    <div class="flex items-center">
                                        <input type="checkbox" id="tls_disable_certs" name="tls_disable_certs"
                                               class="h-4 w-4 text-mqtt-azure focus:ring-mqtt-azure border-mqtt-border rounded">
                                        <label for="tls_disable_certs" class="ml-2 block text-sm text-mqtt-text">
                                            Disable certificate verification (not recommended for production)
                                        </label>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <!-- Advanced Settings Section -->
                        <div class="pb-6">
                            <h3 class="text-lg font-medium text-mqtt-text mb-4">Data Storage Settings</h3>
                            <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
                                <div>
                                    <label for="catalog" class="block text-sm font-medium text-mqtt-text mb-2">Catalog</label>
                                    <input type="text" id="catalog" name="catalog" value="dbdemos" 
                                           class="w-full px-3 py-2 border border-mqtt-border rounded-lg focus:ring-2 focus:ring-mqtt-azure focus:border-transparent">
                                </div>
                                <div>
                                    <label for="schema" class="block text-sm font-medium text-mqtt-text mb-2">Schema</label>
                                    <input type="text" id="schema" name="schema" value="dbdemos_mqtt" 
                                           class="w-full px-3 py-2 border border-mqtt-border rounded-lg focus:ring-2 focus:ring-mqtt-azure focus:border-transparent">
                                </div>
                                <div>
                                    <label for="table" class="block text-sm font-medium text-mqtt-text mb-2">Table Name</label>
                                    <input type="text" id="table" name="table" value="mqtt_v5" 
                                           class="w-full px-3 py-2 border border-mqtt-border rounded-lg focus:ring-2 focus:ring-mqtt-azure focus:border-transparent">
                                </div>
                            </div>
                        </div>

                        <!-- Advanced Settings Section -->
                        <div class="pb-6">
                            <h3 class="text-lg font-medium text-mqtt-text mb-4">Job Settings</h3>
                            <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
                                <div>
                                    <label for="notebook_path" class="block text-sm font-medium text-mqtt-text mb-2">Notebook Path</label>
                                    <input type="text" id="notebook_path" name="notebook_path" value="/Workspace/Users/jeffery.annor@databricks.com/mqtt/MQTT_data_source_v0"
                                           class="w-full px-3 py-2 border border-mqtt-border rounded-lg focus:ring-2 focus:ring-mqtt-azure focus:border-transparent">
                                </div>
                                <div>
                                    <label for="cluster_id" class="block text-sm font-medium text-mqtt-text mb-2">Cluster ID</label>
                                    <input type="text" id="cluster_id" name="cluster_id" value="0709-132523-cnhxf2p6"
                                           class="w-full px-3 py-2 border border-mqtt-border rounded-lg focus:ring-2 focus:ring-mqtt-azure focus:border-transparent">
                                </div>
                                <div>
                                    <label for="warehouse_id" class="block text-sm font-medium text-mqtt-text mb-2">Warehouse ID</label>
                                    <input type="text" id="warehouse_id" name="warehouse_id" value="4b9b953939869799"
                                           class="w-full px-3 py-2 border border-mqtt-border rounded-lg focus:ring-2 focus:ring-mqtt-azure focus:border-transparent">
                                </div>
                            </div>
                        </div>

                        <!-- Action Buttons -->
                        <div class="flex justify-between items-center pt-6 border-t border-mqtt-border">
                            <button type="button" id="testConnection" 
                                    class="bg-mqtt-secondary text-white px-6 py-2 rounded-lg hover:bg-teal-600 transition-colors">
                                <i class="fas fa-plug mr-2"></i>Test Connection
                            </button>
                            <div class="space-x-4">
                                <button type="button" id="saveConfig"
                                        class="bg-mqtt-warning text-white px-6 py-2 rounded-lg hover:bg-orange-600 transition-colors">
                                    <i class="fas fa-save mr-2"></i>Save Configuration
                                </button>
                                <button type="submit" id="connectAndMonitor"
                                        class="bg-mqtt-azure text-white px-6 py-2 rounded-lg hover:bg-blue-700 transition-colors">
                                    <i class="fas fa-play mr-2"></i>Connect & Monitor
                                </button>
                            </div>
                        </div>
                    </form>
                </div>
            </div>

            <div id="dashboardContent" class="tab-content hidden">
            <!-- Stats Cards -->
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
                <div class="bg-mqtt-card rounded-lg shadow p-6">
                    <div class="flex items-center">
                        <div class="flex-shrink-0">
                            <div class="w-8 h-8 bg-red-100 rounded-full flex items-center justify-center">
                                <i class="fas fa-exclamation-triangle text-mqtt-error"></i>
                            </div>
                        </div>
                        <div class="ml-4">
                            <p class="text-sm font-medium text-mqtt-text-light">Duplicated Messages</p>
                            <p class="text-2xl font-semibold text-mqtt-error">{critical}</p>
                        </div>
                    </div>
                </div>

                <div class="bg-mqtt-card rounded-lg shadow p-6">
                    <div class="flex items-center">
                        <div class="flex-shrink-0">
                            <div class="w-8 h-8 bg-yellow-100 rounded-full flex items-center justify-center">
                                <i class="fas fa-clock text-mqtt-warning"></i>
                            </div>
                        </div>
                        <div class="ml-4">
                            <p class="text-sm font-medium text-mqtt-text-light">QOS=2 Messages</p>
                            <p class="text-2xl font-semibold text-mqtt-warning">{in_progress}</p>
                        </div>
                    </div>
                </div>

                <div class="bg-mqtt-card rounded-lg shadow p-6">
                    <div class="flex items-center">
                        <div class="flex-shrink-0">
                            <div class="w-8 h-8 bg-green-100 rounded-full flex items-center justify-center">
                                <i class="fas fa-sitemap text-mqtt-success"></i>
                            </div>
                        </div>
                        <div class="ml-4">
                            <p class="text-sm font-medium text-mqtt-text-light">Unique Topics</p>
                            <p class="text-2xl font-semibold text-mqtt-success">{resolved_today}</p>
                        </div>
                    </div>
                </div>

                <div class="bg-mqtt-card rounded-lg shadow p-6">
                    <div class="flex items-center">
                        <div class="flex-shrink-0">
                            <div class="w-8 h-8 bg-blue-100 rounded-full flex items-center justify-center">
                                <i class="fas fa-chart-line text-mqtt-azure"></i>
                            </div>
                        </div>
                        <div class="ml-4">
                            <p class="text-sm font-medium text-mqtt-text-light">Total Messages</p>
                            <p class="text-2xl font-semibold text-mqtt-azure">{avg_response_time}</p>
                        </div>
                    </div>
                </div>
            </div>

            <!-- MQTT Messages Table -->
            <div class="bg-mqtt-card rounded-lg shadow overflow-hidden">
                <div class="px-6 py-4 border-b border-mqtt-border">
                    <h3 class="text-lg font-medium text-mqtt-text">Recent MQTT Messages</h3>
                </div>
                <div class="overflow-x-auto">
                    <table class="min-w-full divide-y divide-mqtt-border">
                        <thead class="bg-mqtt-bg">
                            <tr>
                                <th class="px-6 py-3 text-left text-xs font-medium text-mqtt-text-light uppercase tracking-wider">
                                    Message
                                </th>
                                <th class="px-6 py-3 text-left text-xs font-medium text-mqtt-text-light uppercase tracking-wider">
                                    Is Duplicate
                                </th>
                                <th class="px-6 py-3 text-left text-xs font-medium text-mqtt-text-light uppercase tracking-wider">
                                    QoS
                                </th>
                                <th class="px-6 py-3 text-left text-xs font-medium text-mqtt-text-light uppercase tracking-wider">
                                    Topic
                                </th>
                                <th class="px-6 py-3 text-left text-xs font-medium text-mqtt-text-light uppercase tracking-wider">
                                    Received Time
                                </th>
                            </tr>
                        </thead>
                        <tbody id="messageTableBody" class="bg-mqtt-card divide-y divide-mqtt-border">
                            <!-- No results message (hidden by default) -->
                            <tr id="noResultsRow" style="display: none;">
                                <td colspan="5" class="px-6 py-8 text-center">
                                    <div class="text-mqtt-text-light">
                                        <i class="fas fa-search text-2xl mb-2"></i>
                                        <p class="text-sm">No MQTT messages found matching your search.</p>
                                        <p class="text-xs mt-1">Search by topic (source) only.</p>
                                    </div>
                                </td>
                            </tr>
    '''
    
    # Add message if no data loaded yet
    if not curr_data or len(curr_data) == 0:
        html += '''
                            <tr>
                                <td colspan="5" class="px-6 py-12 text-center">
                                    <div class="text-mqtt-text-light">
                                        <i class="fas fa-database text-4xl mb-4 text-mqtt-azure"></i>
                                        <p class="text-lg font-medium text-mqtt-text mb-2">No Data Loaded</p>
                                        <p class="text-sm">Please specify your Catalog, Schema, and Table in the <strong>MQTT Connection</strong> tab,</p>
                                        <p class="text-sm">then click <strong>"Refresh Dashboard Data"</strong> to load messages.</p>
                                    </div>
                                </td>
                            </tr>
        '''
    
    # Add data rows
    for row in curr_data:
        # Get duplicate badge color
        is_dup = str(row.get('is_duplicate', 'false')).lower()
        dup_color = 'bg-red-100 text-red-800' if is_dup == 'true' else 'bg-green-100 text-green-800'
        dup_text = 'Yes' if is_dup == 'true' else 'No'
        
        # Get QoS badge color
        qos_value = str(row.get('qos', 0))
        qos_color = 'bg-blue-100 text-blue-800' if qos_value == '2' else 'bg-yellow-100 text-yellow-800' if qos_value == '1' else 'bg-gray-100 text-gray-800'
        
        # Format message (truncate if too long)
        message = str(row.get('message', ''))
        message_display = message[:100] + '...' if len(message) > 100 else message
        
        # Format topic
        topic = str(row.get('topic', 'N/A'))
        
        # Format timestamp
        received_time = str(row.get('received_time', 'N/A'))
        
        html += f'''
                            <tr class="mqtt-hover" data-topic="{topic.lower()}" data-qos="{qos_value}" data-duplicate="{is_dup}">
                                <td class="px-6 py-4">
                                    <div class="text-sm text-mqtt-text max-w-md">
                                        <div class="truncate" title="{message}">{message_display}</div>
                                    </div>
                                </td>
                                <td class="px-6 py-4 whitespace-nowrap">
                                    <span class="inline-flex px-2 py-1 text-xs font-semibold rounded-full {dup_color}">
                                        {dup_text}
                                    </span>
                                </td>
                                <td class="px-6 py-4 whitespace-nowrap">
                                    <span class="inline-flex px-2 py-1 text-xs font-semibold rounded-full {qos_color}">
                                        QoS {qos_value}
                                    </span>
                                </td>
                                <td class="px-6 py-4 whitespace-nowrap text-sm text-mqtt-text">
                                    <span class="font-mono text-xs bg-mqtt-bg px-2 py-1 rounded">{topic}</span>
                                </td>
                                <td class="px-6 py-4 whitespace-nowrap text-sm text-mqtt-text-light">
                                    {received_time}
                                </td>
                            </tr>
        '''
    
    html += '''
                        </tbody>
                    </table>
                </div>
            </div>
            </div>
        </main>

        <!-- Footer -->
        <footer class="bg-mqtt-card border-t border-mqtt-border">
            <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
                <div class="text-center text-mqtt-text-light">
                    <p>&copy; 2025 MQTT Data Monitor. Real-time Message Processing & Analytics Platform.</p>
                </div>
            </div>
        </footer>

        <!-- Notification Modal -->
        <div id="notificationModal" class="hidden fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
            <div id="notificationContent" class="bg-white rounded-lg shadow-xl max-w-md w-full mx-4 p-6 transform transition-all">
                <div class="flex items-start">
                    <div id="notificationIcon" class="flex-shrink-0">
                        <!-- Icon will be inserted here -->
                    </div>
                    <div class="ml-3 flex-1">
                        <h3 id="notificationTitle" class="text-lg font-medium"></h3>
                        <p id="notificationMessage" class="mt-2 text-sm"></p>
                    </div>
                    <button onclick="closeNotification()" class="ml-4 text-gray-400 hover:text-gray-600">
                        <i class="fas fa-times"></i>
                    </button>
                </div>
                <div class="mt-4">
                    <button onclick="closeNotification()" id="notificationButton" class="w-full px-4 py-2 rounded-lg text-white font-medium">
                        Close
                    </button>
                </div>
            </div>
        </div>

        <style>
            /* MQTT Theme Status Colors */
            .status-new { background-color: #e3f2fd; color: #0078D4; }
            .status-in_progress { background-color: #fff3e0; color: #ff8c00; }
            .status-resolved { background-color: #e8f5e8; color: #00a86b; }
            /* MQTT Theme Priority Colors */
            .severity-critical { background-color: #ffebee; color: #e74c3c; }
            .severity-warning { background-color: #fff3e0; color: #ff8c00; }
            .severity-info { background-color: #e3f2fd; color: #0078D4; }
            /* MQTT Hover Effects */
            .mqtt-hover:hover { background-color: #f8f9fa; transition: all 0.2s ease; }
            /* Tab Styles */
            .tab-button.active {
                border-color: #0078D4 !important;
                color: #0078D4 !important;
            }
            .tab-content.hidden {
                display: none;
            }
        </style>

        <script>
            console.log('MQTT Data Monitor loaded successfully!');
            console.log('Theme: HiveMQ & Azure Event Grid inspired');
            
            // Function to update stat counters
            function updateStatsCards(stats) {
                console.log('Updating stats cards with:', stats);
                
                // Update Duplicated Messages
                const duplicatedElement = document.querySelector('.text-2xl.font-semibold.text-mqtt-error');
                if (duplicatedElement) {
                    duplicatedElement.textContent = stats.critical || 0;
                }
                
                // Update QOS=2 Messages
                const qos2Element = document.querySelector('.text-2xl.font-semibold.text-mqtt-warning');
                if (qos2Element) {
                    qos2Element.textContent = stats.in_progress || 0;
                }
                
                // Update Unique Topics
                const topicsElement = document.querySelector('.text-2xl.font-semibold.text-mqtt-success');
                if (topicsElement) {
                    topicsElement.textContent = stats.resolved_today || 0;
                }
                
                // Update Total Messages
                const totalElement = document.querySelector('.text-2xl.font-semibold.text-mqtt-azure');
                if (totalElement) {
                    totalElement.textContent = stats.avg_response_time || 0;
                }
            }
            
            // Function to update table with new data
            function updateTableWithData(data) {
                const tableBody = document.getElementById('messageTableBody');
                
                // Clear existing rows (except the "no results" row)
                const noResultsRow = document.getElementById('noResultsRow');
                tableBody.innerHTML = '';
                if (noResultsRow) {
                    tableBody.appendChild(noResultsRow);
                }
                
                // If no data, show empty state
                if (!data || data.length === 0) {
                    tableBody.innerHTML = `
                        <tr>
                            <td colspan="5" class="px-6 py-12 text-center">
                                <div class="text-mqtt-text-light">
                                    <i class="fas fa-database text-4xl mb-4 text-mqtt-azure"></i>
                                    <p class="text-lg font-medium text-mqtt-text mb-2">No Data Found</p>
                                    <p class="text-sm">No messages found in the specified table.</p>
                                </div>
                            </td>
                        </tr>
                    `;
                    return;
                }
                
                // Build HTML for each row
                data.forEach(row => {
                    const isDup = String(row.is_duplicate || 'false').toLowerCase();
                    const dupColor = isDup === 'true' ? 'bg-red-100 text-red-800' : 'bg-green-100 text-green-800';
                    const dupText = isDup === 'true' ? 'Yes' : 'No';
                    
                    const qosValue = String(row.qos || 0);
                    const qosColor = qosValue === '2' ? 'bg-blue-100 text-blue-800' : 
                                   qosValue === '1' ? 'bg-yellow-100 text-yellow-800' : 
                                   'bg-gray-100 text-gray-800';
                    
                    const message = String(row.message || '');
                    const messageDisplay = message.length > 100 ? message.substring(0, 100) + '...' : message;
                    
                    const topic = String(row.topic || 'N/A');
                    const receivedTime = String(row.received_time || 'N/A');
                    
                    const rowHtml = `
                        <tr class="mqtt-hover" data-topic="${topic.toLowerCase()}" data-qos="${qosValue}" data-duplicate="${isDup}">
                            <td class="px-6 py-4">
                                <div class="text-sm text-mqtt-text max-w-md">
                                    <div class="truncate" title="${message}">${messageDisplay}</div>
                                </div>
                            </td>
                            <td class="px-6 py-4 whitespace-nowrap">
                                <span class="inline-flex px-2 py-1 text-xs font-semibold rounded-full ${dupColor}">
                                    ${dupText}
                                </span>
                            </td>
                            <td class="px-6 py-4 whitespace-nowrap">
                                <span class="inline-flex px-2 py-1 text-xs font-semibold rounded-full ${qosColor}">
                                    QoS ${qosValue}
                                </span>
                            </td>
                            <td class="px-6 py-4 whitespace-nowrap text-sm text-mqtt-text">
                                <span class="font-mono text-xs bg-mqtt-bg px-2 py-1 rounded">${topic}</span>
                            </td>
                            <td class="px-6 py-4 whitespace-nowrap text-sm text-mqtt-text-light">
                                ${receivedTime}
                            </td>
                        </tr>
                    `;
                    
                    tableBody.insertAdjacentHTML('beforeend', rowHtml);
                });
                
                // Re-apply search filter if active
                const searchInput = document.getElementById('searchInput');
                if (searchInput && searchInput.value) {
                    filterTable();
                }
            }
            
            // Notification Functions
            function showNotification(type, title, message) {
                const modal = document.getElementById('notificationModal');
                const icon = document.getElementById('notificationIcon');
                const titleEl = document.getElementById('notificationTitle');
                const messageEl = document.getElementById('notificationMessage');
                const button = document.getElementById('notificationButton');
                
                // Set content
                titleEl.textContent = title;
                messageEl.textContent = message;
                
                // Set icon and colors based on type
                if (type === 'success') {
                    icon.innerHTML = '<div class="w-12 h-12 bg-green-100 rounded-full flex items-center justify-center"><i class="fas fa-check-circle text-2xl text-mqtt-success"></i></div>';
                    titleEl.className = 'text-lg font-medium text-mqtt-success';
                    button.className = 'w-full px-4 py-2 rounded-lg text-white font-medium bg-mqtt-success hover:bg-green-700';
                } else if (type === 'error') {
                    icon.innerHTML = '<div class="w-12 h-12 bg-red-100 rounded-full flex items-center justify-center"><i class="fas fa-times-circle text-2xl text-mqtt-error"></i></div>';
                    titleEl.className = 'text-lg font-medium text-mqtt-error';
                    button.className = 'w-full px-4 py-2 rounded-lg text-white font-medium bg-mqtt-error hover:bg-red-700';
                } else if (type === 'info') {
                    icon.innerHTML = '<div class="w-12 h-12 bg-blue-100 rounded-full flex items-center justify-center"><i class="fas fa-info-circle text-2xl text-mqtt-azure"></i></div>';
                    titleEl.className = 'text-lg font-medium text-mqtt-azure';
                    button.className = 'w-full px-4 py-2 rounded-lg text-white font-medium bg-mqtt-azure hover:bg-blue-700';
                }
                // Show modal
                modal.classList.remove('hidden');
                
                // Auto-close after 5 seconds
                setTimeout(() => {
                    closeNotification();
                }, 5000);
            }
            function closeNotification() {
                const modal = document.getElementById('notificationModal');
                modal.classList.add('hidden');
            }
            // Tab Switching Functionality
            document.addEventListener('DOMContentLoaded', function() {
                const connectionTab = document.getElementById('connectionTab');
                const dashboardTab = document.getElementById('dashboardTab');
                const connectionContent = document.getElementById('connectionContent');
                const dashboardContent = document.getElementById('dashboardContent');
                
                // Tab switching function
                function switchTab(activeTab, activeContent, inactiveTab, inactiveContent) {
                    // Update tab appearance
                    activeTab.classList.add('active', 'border-mqtt-azure', 'text-mqtt-azure');
                    activeTab.classList.remove('border-transparent', 'text-mqtt-text-light');
                    
                    inactiveTab.classList.remove('active', 'border-mqtt-azure', 'text-mqtt-azure');
                    inactiveTab.classList.add('border-transparent', 'text-mqtt-text-light');
                    
                    // Show/hide content
                    activeContent.classList.remove('hidden');
                    inactiveContent.classList.add('hidden');
                }
                // Connection tab click handler
                connectionTab.addEventListener('click', function() {
                    switchTab(connectionTab, connectionContent, dashboardTab, dashboardContent);
                });
                
                // Dashboard tab click handler
                dashboardTab.addEventListener('click', function() {
                    switchTab(dashboardTab, dashboardContent, connectionTab, connectionContent);
                });
                
                // MQTT Configuration Form Handlers
                const mqttForm = document.getElementById('mqttConfigForm');
                const testConnectionBtn = document.getElementById('testConnection');
                const saveConfigBtn = document.getElementById('saveConfig');
                const connectAndMonitorBtn = document.getElementById('connectAndMonitor');
                
                console.log('✅ Form elements found:', 'mqttForm:', !!mqttForm, 'testConnectionBtn:', !!testConnectionBtn, 'saveConfigBtn:', !!saveConfigBtn, 'connectAndMonitorBtn:', !!connectAndMonitorBtn);
                
                if (!testConnectionBtn) {
                    console.error('❌ Test Connection button not found!');
                    return;
                }
                console.log('🔧 Attaching event listener to Test Connection button...');
                
                // Test Connection Handler
                testConnectionBtn.addEventListener('click', async function(e) {
                    console.log('🔌 Test Connection button clicked!');
                    e.preventDefault();
                    e.stopPropagation();
                    
                    const brokerAddress = document.getElementById('broker_address').value;
                    console.log('Broker address:', brokerAddress);
                    
                    if (!brokerAddress) {
                        showNotification('error', 'Missing Broker Address', 'Please enter a broker address first.');
                        return;
                    }
                    // Show loading state
                    const originalHTML = this.innerHTML;
                    this.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i>Testing Connection...';
                    this.disabled = true;
                    
                    try {
                        // Get form data
                        const formData = new FormData(mqttForm);
                        const config = {};
                        // Convert FormData to object
                        for (const [key, value] of formData.entries()) {
                            config[key] = value;
                        }
                        // Handle checkboxes
                        config['require_tls'] = document.getElementById('require_tls').checked ? 'true' : 'false';
                        config['tls_disable_certs'] = document.getElementById('tls_disable_certs').checked ? 'true' : 'false';
                        
                        console.log('🔍 Testing MQTT connection with config:', config);
                        console.log('📤 Sending request to /api/test-mqtt-connection');
                        
                        // Send test request to backend API
                        const response = await fetch('/api/test-mqtt-connection', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify(config)
                        });
                        
                        console.log('📥 Response status:', response.status);
                        const result = await response.json();
                        console.log('📊 Response data:', result);
                        
                        if (result.success) {
                            // Success
                            this.innerHTML = '<i class="fas fa-check mr-2"></i>Connection Successful!';
                            this.classList.remove('bg-mqtt-secondary');
                            this.classList.add('bg-mqtt-success');
                            
                            // Show success modal
                            showNotification('success', 'Connection Successful!', result.message);
                            
                            // Reset button after delay
                            setTimeout(() => {
                                this.innerHTML = originalHTML;
                                this.classList.remove('bg-mqtt-success');
                                this.classList.add('bg-mqtt-secondary');
                                this.disabled = false;
                            }, 3000);
                        } else {
                            // Failure
                            throw new Error(result.error || result.message || 'Connection failed');
                        }
                    } catch (error) {
                        console.error('MQTT connection test failed:', error);
                        
                        // Show error state
                        this.innerHTML = '<i class="fas fa-times mr-2"></i>Connection Failed';
                        this.classList.remove('bg-mqtt-secondary');
                        this.classList.add('bg-mqtt-error');
                        
                        // Show error modal
                        showNotification('error', 'Connection Failed', error.message);
                        
                        // Reset button after delay
                        setTimeout(() => {
                            this.innerHTML = originalHTML;
                            this.classList.remove('bg-mqtt-error');
                            this.classList.add('bg-mqtt-secondary');
                            this.disabled = false;
                        }, 3000);
                    }
                });
                
                console.log('✅ Test Connection event listener attached successfully!');
                
                // Save Configuration Handler
                saveConfigBtn.addEventListener('click', function() {
                    const formData = new FormData(mqttForm);
                    const config = Object.fromEntries(formData);
                    
                    // Save to localStorage (in real app, this would be saved to backend)
                    localStorage.setItem('mqttConfig', JSON.stringify(config));
                    
                    this.innerHTML = '<i class="fas fa-check mr-2"></i>Saved!';
                    this.classList.remove('bg-mqtt-warning');
                    this.classList.add('bg-mqtt-success');
                    
                    setTimeout(() => {
                        this.innerHTML = '<i class="fas fa-save mr-2"></i>Save Configuration';
                        this.classList.remove('bg-mqtt-success');
                        this.classList.add('bg-mqtt-warning');
                    }, 2000);
                });
                
                // Connect and Monitor Handler
                connectAndMonitorBtn.addEventListener('click', async function(e) {
                    e.preventDefault();
                    
                    const brokerAddress = document.getElementById('broker_address').value;
                    if (!brokerAddress) {
                        alert('Please enter a broker address first.');
                        return;
                    }
                    // Show loading state
                    const originalHTML = this.innerHTML;
                    this.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i>Starting MQTT Job...';
                    this.disabled = true;
                    
                    try {
                        // Get form data
                        const formData = new FormData(mqttForm);
                        const config = {};
                        // Convert FormData to object, handling checkboxes properly
                        for (const [key, value] of formData.entries()) {
                            config[key] = value;
                        }
                        // Handle checkboxes (they won't be in formData if unchecked)
                        config['require_tls'] = document.getElementById('require_tls').checked ? 'true' : 'false';
                        config['tls_disable_certs'] = document.getElementById('tls_disable_certs').checked ? 'true' : 'false';
                        
                        // Save configuration locally
                        localStorage.setItem('mqttConfig', JSON.stringify(config));
                        
                        console.log('📤 Sending MQTT Configuration to server:', config);
                        console.log('📊 Data will be written to:', `${config.catalog}.${config.schema}.${config.table}`);
                        
                        // Send configuration to backend API
                        const response = await fetch('/api/start-mqtt-job', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify(config)
                        });
                        
                        const result = await response.json();
                        
                        if (result.success) {
                            // Success - show confirmation and switch to dashboard
                            this.innerHTML = '<i class="fas fa-check mr-2"></i>Job Started!';
                            this.classList.remove('bg-mqtt-azure');
                            this.classList.add('bg-mqtt-success');
                            
                            showNotification('success', 'Job Started Successfully!', result.message || `Job ID: ${result.job_id}, Run ID: ${result.run_id}`);
                            
                            // Switch to dashboard tab after a short delay
                            setTimeout(() => {
                                switchTab(dashboardTab, dashboardContent, connectionTab, connectionContent);
                                this.innerHTML = originalHTML;
                                this.classList.remove('bg-mqtt-success');
                                this.classList.add('bg-mqtt-azure');
                                this.disabled = false;
                            }, 2000);
                        } else {
                            // Error from server
                            throw new Error(result.error || 'Unknown error occurred');
                        }
                    } catch (error) {
                        console.error('Error starting MQTT job:', error);
                        alert(`Failed to start MQTT job: ${error.message}`);
                        
                        // Reset button state
                        this.innerHTML = originalHTML;
                        this.disabled = false;
                    }
                });
                
                // Refresh Dashboard Data Handler
                const refreshDataBtn = document.getElementById('refreshDataBtn');
                console.log('Refresh button element:', refreshDataBtn);
                if (refreshDataBtn) {
                    refreshDataBtn.addEventListener('click', async function() {
                        console.log('Refresh button clicked!');
                        const catalog = document.getElementById('catalog').value;
                        const schema = document.getElementById('schema').value;
                        const table = document.getElementById('table').value;
                        
                        console.log('Form values:', { catalog, schema, table });
                        
                        if (!catalog || !schema || !table) {
                            showNotification('error', 'Missing Information', 'Please enter catalog, schema, and table name.');
                            return;
                        }
                        
                        // Show loading state
                        const originalHTML = this.innerHTML;
                        this.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i>Refreshing...';
                        this.disabled = true;
                        
                        try {
                            console.log('Sending refresh request...');
                            const response = await fetch('/api/refresh-data', {
                                method: 'POST',
                                headers: {
                                    'Content-Type': 'application/json',
                                },
                                body: JSON.stringify({
                                    catalog: catalog,
                                    schema: schema,
                                    table: table,
                                })
                            });
                            
                            console.log('Response status:', response.status);
                            const result = await response.json();
                            console.log('Response data:', result);
                            
                            if (result.success) {
                                showNotification('success', 'Data Refreshed!', `Loaded ${result.row_count} rows from ${catalog}.${schema}.${table}`);
                                
                                // Update the table with new data
                                console.log('Updating table with data:', result.data);
                                updateTableWithData(result.data);
                                
                                // Update the stats cards
                                if (result.stats) {
                                    console.log('Updating stats:', result.stats);
                                    updateStatsCards(result.stats);
                                }
                                
                                // Reset button state
                                this.innerHTML = originalHTML;
                                this.disabled = false;
                            } else {
                                throw new Error(result.error || 'Failed to refresh data');
                            }
                        } catch (error) {
                            console.error('Error refreshing data:', error);
                            showNotification('error', 'Refresh Failed', error.message);
                            
                            // Reset button state
                            this.innerHTML = originalHTML;
                            this.disabled = false;
                        }
                    });
                }
                
                // Stealth Auto-Refresh Function (runs every 30 seconds in the background)
                async function stealthRefresh() {
                    const catalog = document.getElementById('catalog').value;
                    const schema = document.getElementById('schema').value;
                    const table = document.getElementById('table').value;
                    
                    // Only refresh if all fields are filled
                    if (!catalog || !schema || !table) {
                        return;
                    }
                    
                    try {
                        console.log('Auto-refreshing data in background...');
                        const response = await fetch('/api/refresh-data', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify({
                                catalog: catalog,
                                schema: schema,
                                table: table,
                            })
                        });
                        
                        const result = await response.json();
                        
                        if (result.success) {
                            console.log(`Auto-refresh successful: ${result.row_count} rows loaded`);
                            // Update the table silently without notification
                            updateTableWithData(result.data);
                            // Update the stats cards
                            if (result.stats) {
                                updateStatsCards(result.stats);
                            }
                        } else {
                            console.error('Auto-refresh failed:', result.error);
                        }
                    } catch (error) {
                        console.error('Auto-refresh error:', error);
                    }
                }
                
                // Start auto-refresh interval (every 30 seconds)
                setInterval(stealthRefresh, 30000);
                console.log('Auto-refresh enabled: Data will refresh every 30 seconds');
                
                // TLS Settings Toggle Handler
                const requireTlsCheckbox = document.getElementById('require_tls');
                const tlsSettings = document.getElementById('tlsSettings');
                const tlsDisableCertsCheckbox = document.getElementById('tls_disable_certs');
                const certificateFields = document.querySelectorAll('#ca_certs, #certfile, #keyfile');
                
                // Function to toggle TLS settings visibility
                function toggleTlsSettings() {
                    const portField = document.getElementById('port');
                    
                    if (requireTlsCheckbox.checked) {
                        tlsSettings.classList.remove('hidden');
                        // Auto-update port to TLS default if it's still at non-TLS default
                        if (portField.value === '1883') {
                            portField.value = '8883';
                        }
                    } else {
                        tlsSettings.classList.add('hidden');
                        // Auto-update port to non-TLS default if it's still at TLS default
                        if (portField.value === '8883') {
                            portField.value = '1883';
                        }
                    }
                }
                // Function to toggle certificate fields based on disable verification checkbox
                function toggleCertificateFields() {
                    certificateFields.forEach(field => {
                        if (tlsDisableCertsCheckbox.checked) {
                            field.disabled = true;
                            field.classList.add('bg-gray-100', 'text-gray-400');
                            field.value = '';
                        } else {
                            field.disabled = false;
                            field.classList.remove('bg-gray-100', 'text-gray-400');
                        }
                    });
                }
                // Event listeners for TLS settings
                requireTlsCheckbox.addEventListener('change', toggleTlsSettings);
                tlsDisableCertsCheckbox.addEventListener('change', toggleCertificateFields);
                
                // Initialize TLS settings visibility
                toggleTlsSettings();
                toggleCertificateFields();
                
                // Load saved configuration on page load
                const savedConfig = localStorage.getItem('mqttConfig');
                if (savedConfig) {
                    const config = JSON.parse(savedConfig);
                    Object.keys(config).forEach(key => {
                        const element = document.getElementById(key);
                        if (element) {
                            if (element.type === 'checkbox') {
                                element.checked = config[key] === 'on';
                            } else {
                                element.value = config[key];
                            }
                        }
                    });
                    // Re-initialize TLS settings after loading config
                    toggleTlsSettings();
                    toggleCertificateFields();
                }
            });
            
            // MQTT Message Search Functionality
            document.addEventListener('DOMContentLoaded', function() {
                const searchInput = document.getElementById('searchInput');
                const tableBody = document.getElementById('messageTableBody');
                const rows = tableBody.getElementsByTagName('tr');
                
                // Convert HTMLCollection to Array and filter out the "no results" row
                const rowsArray = Array.from(rows).filter(row => row.id !== 'noResultsRow');
                
                // Function to update stats counters based on visible rows
                function updateStatsCounters(visibleRows) {
                    let critical = 0;
                    let processing = 0; 
                    let processed = 0;
                    
                    visibleRows.forEach(function(row) {
                        const severity = row.getAttribute('data-severity') || '';
                        const status = row.getAttribute('data-status') || '';
                        
                        // Count critical messages that are not resolved
                        if (severity.toLowerCase() === 'critical' && status !== 'resolved') {
                            critical++;
                        }
                        // Count in progress messages
                        if (status === 'in_progress') {
                            processing++;
                        }
                        // Count resolved messages
                        if (status === 'resolved') {
                            processed++;
                        }
                    });
                    
                    // Update the counter displays
                    const criticalElement = document.querySelector('.text-2xl.font-semibold.text-mqtt-error');
                    const processingElement = document.querySelector('.text-2xl.font-semibold.text-mqtt-warning');
                    const processedElement = document.querySelector('.text-2xl.font-semibold.text-mqtt-success');
                    
                    if (criticalElement) criticalElement.textContent = critical;
                    if (processingElement) processingElement.textContent = processing;
                    if (processedElement) processedElement.textContent = processed;
                }
                searchInput.addEventListener('input', function() {
                    const searchTerm = this.value.toLowerCase().trim();
                    console.log('🔍 Search triggered with term:', searchTerm);
                    
                    rowsArray.forEach(function(row) {
                        if (searchTerm === '') {
                            // Show all rows when search is empty
                            row.style.display = '';
                        } else {
                            // Get searchable data from row attributes (only topic/source)
                            const topic = row.getAttribute('data-topic') || '';
                            
                            // Search only in the topic/source field
                            const searchableText = topic;
                            
                            // Debug logging for any search term
                            if (searchTerm.length > 0) {
                                console.log('Row topic: ' + topic + ', searchTerm: ' + searchTerm + ', match: ' + searchableText.includes(searchTerm));
                            }
                            // Show/hide row based on search match
                            if (searchableText.includes(searchTerm)) {
                                row.style.display = '';
                            } else {
                                row.style.display = 'none';
                            }
                        }
                    });
                    
                    // Update counters for empty search (show all data)
                    if (searchTerm === '') {
                        updateStatsCounters(rowsArray);
                    }
                    // Update search results count and show/hide no results message
                    const visibleRows = rowsArray.filter(row => row.style.display !== 'none').length;
                    const noResultsRow = document.getElementById('noResultsRow');
                    
                    if (searchTerm !== '' && visibleRows === 0) {
                        noResultsRow.style.display = '';
                    } else {
                        noResultsRow.style.display = 'none';
                    }
                    // Update stats counters based on visible rows
                    updateStatsCounters(rowsArray.filter(row => row.style.display !== 'none'));
                    
                    console.log(`Search: "${searchTerm}" - ${visibleRows} results found`);
                });
                
                // Add search shortcut (Ctrl/Cmd + K)
                document.addEventListener('keydown', function(e) {
                    if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
                        e.preventDefault();
                        searchInput.focus();
                        searchInput.select();
                    }
                });
                
                console.log('🔍 MQTT Search functionality initialized');
                console.log('💡 Tip: Use Ctrl/Cmd + K to focus search box');
                console.log('📊 Found ' + rowsArray.length + ' searchable rows');
                
                // Test search functionality immediately
                console.log('🧪 Testing search functionality...');
                rowsArray.forEach(function(row, index) {
                    const topic = row.getAttribute('data-topic') || '';
                    console.log('Row ' + (index + 1) + ' topic: ' + topic);
                });
            });
        </script>
    </body>
    </html>
    '''
    
    # Replace placeholders with actual values
    html = html.replace('{critical}', str(stats['critical']))
    html = html.replace('{in_progress}', str(stats['in_progress']))
    html = html.replace('{resolved_today}', str(stats['resolved_today']))
    html = html.replace('{avg_response_time}', str(stats['avg_response_time']))
    
    return html

def get_alert_icon(alert_type):
    if 'IoT Sensor' in alert_type:
        return 'fa-thermometer-half'
    elif 'Device Status' in alert_type:
        return 'fa-wifi'
    elif 'System Metrics' in alert_type:
        return 'fa-chart-bar'
    elif 'Security' in alert_type:
        return 'fa-shield-alt'
    else:
        return 'fa-broadcast-tower'

def get_status_color(status):
    if status == 'new':
        return 'bg-blue-100 text-blue-800'
    elif status == 'in_progress':
        return 'bg-yellow-100 text-yellow-800'
    elif status == 'resolved':
        return 'bg-green-100 text-green-800'
    else:
        return 'bg-gray-100 text-gray-800'

def get_severity_color(severity):
    if severity == 'CRITICAL':
        return 'bg-red-100 text-red-800'
    elif severity == 'WARNING':
        return 'bg-yellow-100 text-yellow-800'
    elif severity == 'INFO':
        return 'bg-blue-100 text-blue-800'
    else:
        return 'bg-gray-100 text-gray-800'

if __name__ == '__main__':
    print("🚀 Starting MQTT Data Monitor Dashboard")
    print("📡 MQTT Message Processing & Analytics Platform")
    print("=" * 50)
    flask_app.run(debug=True, host='0.0.0.0', port=8001)
