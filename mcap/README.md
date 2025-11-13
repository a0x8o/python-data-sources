# MCAP Spark Data Source

A custom Apache Spark data source for reading MCAP (ROS 2 bag) files.

## Overview

This data source allows you to read MCAP files directly into Spark DataFrames, making it easy to process and analyze robotics data at scale using Spark SQL and DataFrame APIs.

## Features

- ✅ Read MCAP files with multiple message types (protobuf, JSON, etc.)
- ✅ Automatic message decoding
- ✅ Partitioned reading for parallel processing
- ✅ Support for glob patterns to read multiple files
- ✅ JSON output for flexible schema handling
- ✅ Compatible with Delta Lake and Parquet formats

## Installation

### Requirements

```bash
pip install pyspark
pip install mcap
pip install mcap-protobuf-support
pip install protobuf
```

### Files

- `mcap_datasource.py` - The main data source implementation
- `mcap_spark_example.py` - Example usage script
- `mcap_reader` - Standalone reader (non-Spark)

## Usage

### Basic Usage

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MCAP Reader") \
    .getOrCreate()

# Read a single MCAP file
df = spark.read.format("mcap") \
    .option("path", "/path/to/file.mcap") \
    .load()

df.show()
```

### Reading Multiple Files

```python
# Read all MCAP files in a directory
df = spark.read.format("mcap") \
    .option("path", "/path/to/mcap/directory") \
    .option("pathGlobFilter", "*.mcap") \
    .load()
```

### Filtering by Topic at Read Time

For better performance, filter by topic during the read operation instead of after loading:

```python
# Read only "pose" topic messages (more efficient than df.filter())
df = spark.read.format("mcap") \
    .option("path", "/path/to/file.mcap") \
    .option("topicFilter", "pose") \
    .load()

# Read all topics (default behavior)
df = spark.read.format("mcap") \
    .option("path", "/path/to/file.mcap") \
    .option("topicFilter", "*") \
    .load()

# Or simply omit topicFilter to read all topics
df = spark.read.format("mcap") \
    .option("path", "/path/to/file.mcap") \
    .load()
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `path` | *(required)* | Path to MCAP file(s) or directory |
| `pathGlobFilter` | `*.mcap` | Glob pattern for file matching |
| `numPartitions` | `4` | Number of partitions for parallel processing |
| `recursiveFileLookup` | `false` | Recursively search subdirectories |
| `topicFilter` | *(none)* | Filter by specific topic name. Use `*` or omit to read all topics |

### Schema

The data source produces a DataFrame with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `sequence` | BIGINT | Sequential message number within partition (starts at 0) |
| `topic` | STRING | The message topic (e.g., "pose", "camera_jpeg") |
| `schema` | STRING | The schema name (e.g., "foxglove.PoseInFrame") |
| `encoding` | STRING | The encoding type (protobuf, json, etc.) |
| `log_time` | BIGINT | The message timestamp in nanoseconds |
| `data` | STRING | JSON string containing all message fields |

### Working with JSON Data

Extract specific fields from the JSON `data` column:

```python
from pyspark.sql.functions import get_json_object, col

# Extract position coordinates from pose messages
pose_df = df.filter(col("topic") == "pose") \
    .select(
        "log_time",
        get_json_object(col("data"), "$.position.x").alias("pos_x"),
        get_json_object(col("data"), "$.position.y").alias("pos_y"),
        get_json_object(col("data"), "$.position.z").alias("pos_z"),
        get_json_object(col("data"), "$.orientation.w").alias("orient_w")
    )

pose_df.show()
```

### Filtering by Topic

```python
# Get only camera images
camera_df = df.filter(col("topic") == "camera_jpeg")

# Get only microphone data
audio_df = df.filter(col("topic") == "microphone")

# Multiple topics
events_df = df.filter(col("topic").isin(["mouse", "keyboard"]))

# Order by sequence to maintain message order
ordered_df = df.orderBy("sequence")
```

### Aggregations

```python
# Count messages by topic
df.groupBy("topic").count().show()

# Get time range per topic
from pyspark.sql.functions import min, max

df.groupBy("topic") \
    .agg(
        min("log_time").alias("start_time"),
        max("log_time").alias("end_time")
    ).show()
```

### Saving to Delta Lake / Parquet

```python
# Save as Parquet (partitioned by topic)
df.write.mode("overwrite") \
    .partitionBy("topic") \
    .parquet("/path/to/output/parquet")

# Save as Delta Lake (if Delta is configured)
df.write.mode("overwrite") \
    .format("delta") \
    .partitionBy("topic") \
    .save("/path/to/output/delta")
```

## Example: Complete Pipeline

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, from_unixtime

spark = SparkSession.builder \
    .appName("MCAP Analysis") \
    .getOrCreate()

# Read MCAP file
df = spark.read.format("mcap") \
    .option("path", "/path/to/demo.mcap") \
    .option("numPartitions", "8") \
    .load()

# Add human-readable timestamp
df = df.withColumn(
    "timestamp", 
    from_unixtime(col("log_time") / 1e9)
)

# Process pose data
pose_df = df.filter(col("topic") == "pose") \
    .select(
        "timestamp",
        get_json_object(col("data"), "$.position.x").cast("double").alias("x"),
        get_json_object(col("data"), "$.position.y").cast("double").alias("y"),
        get_json_object(col("data"), "$.position.z").cast("double").alias("z")
    )

# Calculate statistics
pose_df.describe().show()

# Save results
pose_df.write.mode("overwrite") \
    .parquet("/path/to/output/pose_data")

spark.stop()
```

## Architecture

### Components

1. **MCAPDataSource**: Main data source class
   - Implements the Spark DataSource interface
   - Defines schema and creates readers

2. **MCAPDataSourceReader**: Reader implementation
   - Handles file discovery and partitioning
   - Coordinates parallel reading across executors

3. **Partition Functions**: 
   - `_path_handler`: Discovers files matching glob patterns
   - `_read_mcap_partition`: Reads MCAP files in a partition
   - `_read_mcap_file`: Decodes individual MCAP files

4. **Decoders**:
   - `decode_protobuf_message`: Handles protobuf messages
   - `decode_json_message`: Handles JSON messages
   - `decode_fallback`: Handles unknown formats

### Data Flow

```
MCAP Files → File Discovery → Partitioning → Parallel Read → Decode → JSON → DataFrame
```

## Performance Tips

1. **Topic Filtering at Read Time**: Use `topicFilter` option for best performance
   ```python
   # BEST: Filter during read (skips unwanted messages early)
   df = spark.read.format("mcap") \
       .option("path", "/path/to/file.mcap") \
       .option("topicFilter", "pose") \
       .load()
   
   # GOOD: Filter after read (still efficient with predicate pushdown)
   df = spark.read.format("mcap") \
       .option("path", "/path/to/file.mcap") \
       .load() \
       .filter(col("topic") == "pose")
   ```

2. **Partitioning**: Adjust `numPartitions` based on cluster size
   ```python
   .option("numPartitions", "16")  # For larger clusters
   ```

3. **Select Only Needed Fields**: Extract JSON fields early
   ```python
   df.select("log_time", get_json_object(col("data"), "$.position"))
   ```

4. **Persist for Multiple Actions**: Cache if reusing DataFrame
   ```python
   df = df.filter(...).persist()
   ```

## Troubleshooting

### "No MCAP files found"
- Check that the path exists and contains `.mcap` files
- Verify the `pathGlobFilter` option matches your files
- Check file permissions

### Decoder Errors
- Messages with unknown encodings fall back to hex-encoded `raw_data`
- Check protobuf dependencies if protobuf decoding fails



## 📄 Third-Party Package Licenses

&copy; 2025 Databricks, Inc. All rights reserved. The source in this project is provided subject to the Databricks License [https://databricks.com/db-license-source]. All included or referenced third party libraries are subject to the licenses set forth below.

| Package | Purpose | License | Source |
| ------- | ------- | ------- | ------ |
| mcap | MCAP file format reader | MIT | https://pypi.org/project/mcap/ |
| mcap-protobuf-support | Protobuf decoder for MCAP | MIT | https://pypi.org/project/mcap-protobuf-support/ |
| protobuf | Protocol Buffers serialization | BSD-3-Clause | https://pypi.org/project/protobuf/ |
| pyspark | Apache Spark Python API | Apache-2.0 | https://pypi.org/project/pyspark/ |

## References

- [MCAP Format Specification](https://mcap.dev/)
- [Apache Spark Data Source API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataSource.html)
- [ROS 2 Documentation](https://docs.ros.org/)
- [Foxglove Studio](https://foxglove.dev/)

