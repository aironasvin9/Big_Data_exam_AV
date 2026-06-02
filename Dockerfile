# ============================================================
# AIS Collision Detection — Docker Image
# ============================================================
# Base: official Python 3.11 slim image
# Includes: Java 17 (required by PySpark), PySpark, matplotlib,
#           contextily (map tiles), pyproj, pandas
# ============================================================

FROM python:3.11-slim

# ---------- System dependencies ----------
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        curl \
        wget \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Java home required by PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ---------- Working directory ----------
WORKDIR /app

# ---------- Python dependencies ----------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---------- Application source ----------
COPY src/ ./src/

# ---------- Data and output directories ----------
# DATA_DIR is where the AIS CSV files should be mounted / placed.
# OUTPUT_DIR is where results and the trajectory map are written.
RUN mkdir -p /app/data /app/output

ENV DATA_DIR=/app/data
ENV OUTPUT_DIR=/app/output

# Suppress PySpark's verbose Ivy dependency download logs
ENV PYSPARK_PYTHON=python3
ENV PYTHONUNBUFFERED=1

# ---------- Entrypoint ----------
# Step 1: Download data (skipped if files already exist)
# Step 2: Run collision detection
CMD ["bash", "-c", \
     "python src/download_data.py --data-dir ${DATA_DIR} && \
      python src/collision_detection.py"]
