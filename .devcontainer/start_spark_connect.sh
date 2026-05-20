#!/usr/bin/env bash
# scripts/start_spark_connect.sh
set -e

echo "========================================================================"
echo "📡 DevContainer Lifecycle: Warming up Spark Connect Loopback Daemon..."
echo "========================================================================"

# 1. Dynamically locate the correct internal OpenJDK path inside the container
export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:/bin/java::")
echo "☕ Active Java Runtime: $JAVA_HOME"

# 2. Extract the true string element path out of PySpark's package list wrapper
export SPARK_HOME=$(python3 -c "import pyspark; print(pyspark.__path__[0])")

echo "📦 Active PySpark Home: $SPARK_HOME"

# 3. Spin up the background gRPC server bound to local loopback space
# --- Update Step 3 inside .devcontainer/start_spark_connect.sh ---
echo "🚀 Booting background Spark Connect server on port 15002..."
# Unsetting this variable prevents spark-submit from executing a client loop
(
  unset SPARK_REMOTE
  /opt/venv/bin/spark-submit \
    --master "local[*]" \
    --class org.apache.spark.sql.connect.service.SparkConnectServer \
    --jars /opt/venv/share/spark-jars/* \
    --conf spark.sql.shuffle.partitions=1 \
    --conf spark.default.parallelism=1 \
    --conf spark.driver.bindAddress=127.0.0.1 \
    --conf spark.driver.host=127.0.0.1 \
    pyspark-shell > /tmp/spark-connect.log 2>&1 &
)

# 4. Give the JVM exactly 2 seconds to bind its local tcp socket interface
sleep 2

if ps aux | grep -v grep | grep -q "SparkConnectServer"; then
  echo "========================================================================"
  echo "✅ Spark Connect Server is online and listening on 127.0.0.1:15002"
  echo "========================================================================"
else
  echo "❌ CRITICAL: Spark Connect failed to start! Checking logs..."
  cat /tmp/spark-connect.log
  exit 1
fi
