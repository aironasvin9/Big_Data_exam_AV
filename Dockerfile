# ============================================================
# AIS Collision Detection — Docker Image
# ============================================================
#
# Base image:
#   python:3.11-slim-bookworm
#
# Why bookworm?
#   Debian trixie no longer provides openjdk-17-jre-headless.
#   PySpark 3.5.x is safest with Java 17, so we pin to bookworm.
#
# Runtime expectation:
#   - AIS CSV files are mounted/provided in /app/data
#   - Results are written to /app/output
#
# Optional:
#   - Set AUTO_DOWNLOAD=1 to run src/download_data.py before analysis.
#     This requires src/download_data.py to exist and /app/data to be writable.
#
# ============================================================

FROM python:3.11-slim-bookworm

# ─────────────────────────────────────────────────────────────
# Environment
# ─────────────────────────────────────────────────────────────

ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python3

ENV DATA_DIR=/app/data
ENV OUTPUT_DIR=/app/output

# By default, do not download data inside the container.
# The normal workflow is to mount extracted CSV files into /app/data.
ENV AUTO_DOWNLOAD=0

# ─────────────────────────────────────────────────────────────
# System dependencies
# ─────────────────────────────────────────────────────────────

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        ca-certificates \
        curl \
        wget \
    && rm -rf /var/lib/apt/lists/*

# Configure JAVA_HOME robustly across CPU architectures.
# On arm64 and amd64 the Java path differs, so derive it from `which java`.
RUN JAVA_HOME_REAL="$(dirname "$(dirname "$(readlink -f "$(which java)")")")" \
    && ln -s "${JAVA_HOME_REAL}" /usr/local/openjdk-17

ENV JAVA_HOME=/usr/local/openjdk-17
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ─────────────────────────────────────────────────────────────
# Working directory
# ─────────────────────────────────────────────────────────────

WORKDIR /app

# ─────────────────────────────────────────────────────────────
# Python dependencies
# ─────────────────────────────────────────────────────────────

COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt

# ─────────────────────────────────────────────────────────────
# Application source
# ─────────────────────────────────────────────────────────────

COPY src/ /app/src/

# ─────────────────────────────────────────────────────────────
# Runtime directories
# ─────────────────────────────────────────────────────────────

RUN mkdir -p /app/data /app/output

# ─────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────
#
# Default behavior:
#   Run collision detection using CSV files already present in DATA_DIR.
#
# Optional behavior:
#   If AUTO_DOWNLOAD=1, run src/download_data.py first.
#
# Notes:
#   - If /app/data is mounted read-only, AUTO_DOWNLOAD must remain 0.
#   - docker-compose.yml mounts ./data as read-only by default.
#
# ─────────────────────────────────────────────────────────────

CMD ["sh", "-c", "\
    if [ \"${AUTO_DOWNLOAD}\" = \"1\" ]; then \
        if [ -f /app/src/download_data.py ]; then \
            echo 'AUTO_DOWNLOAD=1: running data downloader...'; \
            python /app/src/download_data.py --data-dir \"${DATA_DIR}\"; \
        else \
            echo 'ERROR: AUTO_DOWNLOAD=1 but /app/src/download_data.py was not found.'; \
            exit 1; \
        fi; \
    else \
        echo 'AUTO_DOWNLOAD=0: expecting AIS CSV files to already exist in DATA_DIR.'; \
    fi; \
    echo 'Starting AIS collision detection pipeline...'; \
    python /app/src/collision_detection.py \
"]
