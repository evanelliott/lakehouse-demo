#!/usr/bin/env bash
# .devcontainer/start_spark_connect.sh
set -e

echo "========================================================================"
echo "📡 DevContainer Lifecycle: Warming up Spark Connect Loopback Daemon..."
echo "========================================================================"

export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:/bin/java::")
export SPARK_HOME=$(python3 -c "import pyspark; print(pyspark.__path__[0])")

echo "🚀 Booting background Spark Connect server on port 15002..."
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

# --- NEW SYNCHRONOUS NETWORK GAUNTLET ---
echo "⏳ Waiting for Spark Connect to bind to 127.0.0.1:15002..."
MAX_ATTEMPTS=15
ATTEMPT=0

while ! cat < /dev/null > /dev/tcp/127.0.0.1/15002 2>/dev/null; do
  ATTEMPT=$((ATTEMPT + 1))
  if [ $ATTEMPT -ge $MAX_ATTEMPTS ]; then
    echo "❌ CRITICAL: Port 15002 timed out! Printing startup logs:"
    cat /tmp/spark-connect.log
    exit 1
  fi
  sleep 0.5
done

echo "========================================================================"
echo "✅ Spark Connect Server is online and verified on 127.0.0.1:15002"
echo "========================================================================"
