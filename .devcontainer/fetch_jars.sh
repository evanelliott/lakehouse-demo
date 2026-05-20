#!/usr/bin/env bash
# .devcontainer/fetch_jars.sh
set -e

# Drops the cache folder cleanly inside the .devcontainer space
JAR_CACHE_DIR="$(dirname "$0")/.cache-jars"
mkdir -p "$JAR_CACHE_DIR"

echo "========================================================================"
echo "📥 DevContainer Lifecycle: Pre-fetching Spark & Lakehouse JARs..."
echo "========================================================================"

ARTIFACT_URLS=(
    "https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/3.5.1/spark-connect_2.12-3.5.1.jar"
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar"
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
)

# Loop through the array URLs natively
for MAVEN_URL in "${ARTIFACT_URLS[@]}"; do
    # Extract the trailing filename using standard string parsing
    JAR_NAME="${MAVEN_URL##*/}"
    TARGET_PATH="$JAR_CACHE_DIR/$JAR_NAME"
    
    if [ -f "$TARGET_PATH" ]; then
        echo "✅ $JAR_NAME is already cached locally."
    else
        echo "📥 Downloading $JAR_NAME from Maven Central..."
        curl -s -S -f -o "$TARGET_PATH" "$MAVEN_URL"
    fi
done

echo "========================================================================"
echo "⚡ Offline assets secured. Handing over control to the Docker engine..."
echo "========================================================================"
