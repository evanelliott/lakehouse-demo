# =========================================================================
# STAGE 1: BUILDER
# =========================================================================
FROM python:3.12-bookworm AS builder
WORKDIR /app

# Pull the blazing fast astral uv binary
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Build inside an isolated virtualenv for clean multi-stage copying
RUN uv venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy package specifications
COPY pyproject.toml ./

# Generate lockfile and compile the isolated python environment
RUN uv lock
RUN uv pip install --requirement pyproject.toml --all-extras --group dev

# OFFLINE INJECTION: Copy pre-cached host jars directly into image layers
# This completely eliminates maven download latency during test container spin-ups
RUN mkdir -p /opt/venv/share/spark-jars
COPY .devcontainer/.cache-jars/*.jar /opt/venv/share/spark-jars/


# =========================================================================
# STAGE 2: DEVELOPMENT (Your DevContainer Engine & Spark Connect Server)
# =========================================================================
FROM python:3.12-bookworm AS development
WORKDIR /app

# 1. Core OS tools & Java installation
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    docker.io \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# 2. Extract the cleanly compiled Python virtual environment
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"
ENV PYTHONPATH="/app"

# 3. Handle Playwright system dependencies
RUN playwright install --with-deps chromium

# 4. Target Debian java path mappings
ENV JAVA_HOME="/usr/lib/jvm/default-java"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# 5. Provision sandbox workspace permissions
RUN useradd -m -s /bin/bash vscode && \
    chown -R vscode:vscode /app /opt/venv

# 6. Symlink the baked JARs directly into PySpark's default site-packages classpath
# This handles local executor rollbacks seamlessly without custom spark-submit arguments
RUN LN_TARGET=$(python3 -c "import pyspark; print(pyspark.__path__[0] + '/jars')") && \
    ln -s /opt/venv/share/spark-jars/* "$LN_TARGET/"

# Cache barrier isolation: Application code mounts as a volume, keeping builds fast
USER vscode
CMD ["/bin/bash"]


# =========================================================================
# STAGE 3: RUNTIME (Lean Production Airflow Layer)
# =========================================================================
FROM python:3.12-slim-bookworm AS runtime
WORKDIR /app

# 1. Minimum production runtime footprint (Headless Java JVM execution engine)
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# 2. Extract Python environment and establish path mappings
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENV JAVA_HOME="/usr/lib/jvm/default-java"
ENV PYTHONPATH="/app"

# 3. Symlink baked JARs for production inline execution tasks
RUN LN_TARGET=$(python3 -c "import pyspark; print(pyspark.__path__[0] + '/jars')") && \
    ln -s /opt/venv/share/spark-jars/* "$LN_TARGET/"

# 4. Bake compiled codebase into production container image layer
COPY . .

CMD ["python"]
