#!/usr/bin/env bash
# .devcontainer/start_spark_connect.sh
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
echo "🚀 Booting background Spark Connect server on port 15002..."
(
  unset SPARK_REMOTE
  # nohup keeps the process alive after the parent lifecycle script exits
  nohup /opt/venv/bin/spark-submit \
    --master "local[*]" \
    --class org.apache.spark.sql.connect.service.SparkConnectServer \
    --jars /opt/venv/share/spark-jars/* \
    --conf spark.sql.shuffle.partitions=1 \
    --conf spark.default.parallelism=1 \
    --conf spark.driver.bindAddress=127.0.0.1 \
    --conf spark.driver.host=127.0.0.1 \
    </dev/null >/tmp/spark-connect.log 2>&1 &
)

# 4. Synchronous Network Gauntlet: Wait for the port to open before letting VS Code finish
echo "⏳ Waiting for Spark Connect to bind to 127.0.0.1:15002..."
MAX_ATTEMPTS=20
ATTEMPT=0

while ! cat < /dev/null > /dev/tcp/127.0.0.1/15002 2>/dev/null; do
  ATTEMPT=$((ATTEMPT + 1))
  if [ $ATTEMPT -ge $MAX_ATTEMPTS ]; then
    echo "❌ CRITICAL: Port 15002 timed out! Printing startup logs:"
    echo "------------------------------------------------------------------------"
    cat /tmp/spark-connect.log
    echo "------------------------------------------------------------------------"
    exit 1
  fi
  sleep 0.5
done

# 5. Final validation check on the process namespace
if ps aux | grep -v grep | grep -q "SparkConnectServer"; then
  echo "========================================================================"
  echo "✅ Spark Connect Server is online and verified on 127.0.0.1:15002"
  echo "========================================================================"
else
  echo "❌ CRITICAL: Process disappeared right after port binding!"
  cat /tmp/spark-connect.log
  exit 1
fi
