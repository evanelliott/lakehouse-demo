# --- STAGE 1: BUILDER ---
FROM python:3.12-bookworm AS builder
WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Use standard pip install logic - it is the most robust for multi-stage
COPY pyproject.toml uv.loc[k] ./
RUN uv pip install --system --requirement pyproject.toml --all-extras --group dev

# --- STAGE 2: DEVELOPMENT ---
FROM python:3.12-bookworm AS development
ARG TARGETARCH
WORKDIR /app

# 1. OS tools
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless docker.io curl git && \
    rm -rf /var/lib/apt/lists/*

# 2. Copy environment
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# 3. THE FIX: Set these BEFORE running playwright so it can find itself
ENV PYTHONPATH="/usr/local/lib/python3.12/site-packages"
ENV PATH="/usr/local/bin:${PATH}"

# 4. Standard Playwright setup
RUN playwright install --with-deps chromium

# 5. Final Configs
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-universal"
RUN ln -s /usr/lib/jvm/java-17-openjdk-${TARGETARCH} $JAVA_HOME
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYTHONPATH="/app:/usr/local/lib/python3.12/site-packages"

COPY . .

CMD ["/bin/bash"]


# --- STAGE 3: RUNTIME (Lean Production Layer) ---
FROM python:3.12-slim-bookworm AS runtime
ARG TARGETARCH
WORKDIR /app

# 1. Install bare necessities
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# 2. Copy system packages and source
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY . .

# 3. Dynamic Java path for production stability
RUN ln -s /usr/lib/jvm/java-17-openjdk-${TARGETARCH:-amd64} /usr/lib/jvm/java-17-openjdk-universal
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-universal"
ENV PYTHONPATH="/app"
ENV PATH="${JAVA_HOME}/bin:$PATH"

CMD ["python"]