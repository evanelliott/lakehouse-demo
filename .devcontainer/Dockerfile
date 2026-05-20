# ==========================================
# STAGE 1: BUILDER
# ==========================================
FROM python:3.12-bookworm AS builder
WORKDIR /app

# Pull the blazing fast astral uv binary
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Build inside an isolated virtual environment for bulletproof multi-stage copying
RUN uv venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy only your pyproject.toml (Completely removes host uv.lock dependencies)
COPY pyproject.toml ./

# Let uv generate a deterministic lockfile inside the container layer
RUN uv lock

# Install packages based on the freshly generated internal lock tree
RUN uv pip install --requirement pyproject.toml --all-extras --group dev


# ==========================================
# STAGE 2: DEVELOPMENT (Your DevContainer Engine)
# ==========================================
FROM python:3.12-bookworm AS development
WORKDIR /app

# 1. Core OS tools & Java installation
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    docker.io \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# 2. Pull the cleanly isolated Python environment from Stage 1
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"
ENV PYTHONPATH="/app"

# 3. Install Playwright & bake system-level Chromium dependencies directly into the layer
RUN playwright install --with-deps chromium

# 4. Target Debian's native 'default-java' path wrapper to eliminate multi-architecture friction
ENV JAVA_HOME="/usr/lib/jvm/default-java"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN useradd -m -s /bin/bash vscode && \
    chown -R vscode:vscode /app /opt/venv

# === THE CACHE FIX ===
# Move this to the absolute bottom of the stage. Code changes will no longer 
# invalidate the heavy OS, virtualenv, or Playwright layers above it.
COPY . .

CMD ["/bin/bash"]


# ==========================================
# STAGE 3: RUNTIME (Lean Production Layer)
# ==========================================
FROM python:3.12-slim-bookworm AS runtime
WORKDIR /app

# 1. Install lean headless Java runner for production PySpark executors
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# 2. Mirror paths and fetch optimized production environment from Stage 1
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENV JAVA_HOME="/usr/lib/jvm/default-java"
ENV PYTHONPATH="/app"

# Move this to the absolute bottom here as well
COPY . .

CMD ["python"]
