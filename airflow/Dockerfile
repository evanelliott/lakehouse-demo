# 1. Global Scope: Declare the argument for the base image
# This allows the version to be controlled entirely from your .env
ARG AIRFLOW_IMAGE

# 2. Use the argument to pull the specific Airflow version
FROM ${AIRFLOW_IMAGE}

# 3. Switch to root to handle system-level setup and permissions
USER root

# Install any OS-level dependencies if needed (e.g., build-essential, libpq-dev)
# For this demo, we keep it lean to save memory on your 16GB Mac
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 4. Prepare the requirements file
# Copy from the local directory into the container's root
COPY requirements.txt /requirements.txt

# 5. Switch to the 'airflow' user to install Python packages
# NEVER install pip packages as root in a production-grade container
USER airflow

# Install providers and dependencies defined in requirements.txt
# --no-cache-dir keeps the image size small
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# 6. Set the working directory
WORKDIR /opt/airflow
