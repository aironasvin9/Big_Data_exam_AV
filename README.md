# AIS Collision Detection — December 2021

> **Assignment 4** — Big Data Processing with PySpark  
> Builds directly on the patterns established in **Assignment 3** (MongoDB sharded cluster)  
> and the **Shadow Fleet Detection** project.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Repository Structure](#repository-structure)
3. [Methodology](#methodology)
4. [Data Quality & Noise Handling](#data-quality--noise-handling)
5. [Computational Strategy](#computational-strategy)
6. [Results](#results)
7. [Docker Hub & Rebuild](#docker-hub--rebuild)
8. [Configuration Reference](#configuration-reference)

---

## Quick Start

### Prerequisites

- Docker ≥ 24 and Docker Compose v2
- ~20 GB free disk space (31 daily AIS CSV files, ~500–800 MB each)
- Internet access (data download + basemap tiles)

### Option A — Docker Compose (recommended)

```bash
git clone https://github.com/<your-username>/ais-collision-detection.git
cd ais-collision-detection

# Builds image, downloads all 31 CSV files automatically, then runs analysis
docker compose up --build

# Results:
#   ./output/trajectory_map.png   ← trajectory visualisation
#   stdout                        ← MMSI, vessel names, timestamp, coordinates
```

### Option B — Pre-built image from Docker Hub

```bash
docker pull <your-dockerhub-username>/ais-collision-detection:latest

mkdir -p data output
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/output:/app/output" \
  <your-dockerhub-username>/ais-collision-detection:latest
```

### Option C — Provide your own data

If you already have the December 2021 files from `https://web.ais.dk/aisdata/`  
(named `aisdk-2021-12-01.csv` … `aisdk-2021-12-31.csv`), place them in `./data/`  
and run `docker compose up --build`.  The download step is automatically skipped.

---

## Repository Structure

```
ais-collision-detection/
├── Dockerfile                  ← container definition (Python 3.11 + Java 17 + PySpark)
├── docker-compose.yml          ← one-command orchestration
├── requirements.txt            ← pinned Python dependencies
├── README.md                   ← this file (also serves as the written report)
├── src/
│   ├── collision_detection.py  ← main PySpark pipeline (8 stages)
│   └── download_data.py        ← downloads all 31 daily AIS CSV files
├── data/                       ← AIS CSVs (git-ignored; populated at runtime)
└── output/                     ← results (git-ignored; written at runtime)
    └── trajectory_map.png
```

---

## Methodology

### Continuity with Previous Assignments

This pipeline deliberately reuses the patterns we established in prior work:

| Pattern | Origin | Application here |
|---------|--------|-----------------|
| 6-category noise taxonomy | Assignment 3 (MongoDB pipeline) | `filter_noise()` — same category numbering |
| Streaming / no full-load-to-RAM | Assignment 3 + Shadow Fleet | Spark's lazy evaluation achieves the same goal |
| Teleportation detection | Shadow Fleet project | `remove_teleportation()` — implied-speed > 50 kts |
| Haversine distance | Shadow Fleet project | Exact distance for radius filter + collision measure |
| Partitioned pairwise analysis | Shadow Fleet project | Spatial-temporal bucketing replaces MMSI partitioning |
| Docker containerisation | Both previous assignments | Same `Dockerfile` + `docker-compose.yml` pattern |

### Pipeline Overview

```
load_raw()
    ↓  31 daily CSVs → single Spark DataFrame (all columns as strings)
filter_noise()
    ↓  6-category filter (nulls → MMSI → coords → transponder type → stationary → geo/time)
remove_teleportation()
    ↓  Window function per MMSI: drop pings implying > 50 kts
    ↓  .cache()  ← DataFrame reused for collision search + trajectory extraction
generate_candidates()
    ↓  Spatial-temporal bucketing → self-join → Haversine on candidates only
find_collision()
    ↓  Minimum-distance pair = collision event
extract_trajectories()
    ↓  Both vessels, ±10 min window
plot_trajectories()
    ↓  output/trajectory_map.png
print_results()
    ↓  stdout: MMSI, names, timestamp, coordinates, distance
```

---

## Data Quality & Noise Handling

### Category 1 — Null / missing essential fields
Drop any row where MMSI, latitude, longitude, timestamp, or SOG is null.
These records cannot be positioned in space or time.

### Category 2 — Invalid MMSI patterns
- MMSI must be a 9-digit number in `[100_000_000, 999_999_999]`
- Exclude Danish base-station prefix `992xxxxxx`
- Exclude repeated-digit patterns (`000000000`, `111111111`, …)

*(Same rules as Assignment 3's `ais_data → ais_filtered` pipeline.)*

### Category 3 — Invalid / sentinel coordinates
- Latitude must be in `[−90, 90]`, longitude in `[−180, 180]`
- `(0.0, 0.0)` "Null Island" — a well-known AIS transponder default value — excluded

### Category 4 — Non-vessel transponders
- `Type of mobile` field: exclude base stations, AtoN, SART devices
- Prevents infrastructure broadcasts from appearing as vessel pings

### Category 5 — Stationary vessels
- `SOG < 0.5 knots` → excluded (at anchor, moored, adrift)
- `Navigational status` ∈ `{At anchor, Moored}` → excluded

*Rationale:* two vessels permanently moored side-by-side would register sub-metre distance indefinitely — this is not a collision.

### Category 6 — Temporal + geographic bounds
Applied **last** (and in cheapest-first order within this category):
1. Timestamp filter: `2021-12-01 ≤ ts < 2022-01-01`
2. Bounding-box pre-filter (cheap arithmetic): eliminates ~90 % of remaining rows
3. Exact Haversine ≤ 50 nm radius: UDF applied only to the bbox survivors

### Teleportation / GPS spike removal
*(From Shadow Fleet project — same logic applied here)*

Each vessel's pings are ordered by timestamp using a Spark window function.  
The implied speed between consecutive pings is computed as:

```
implied_speed_kts = haversine(prev, curr) / elapsed_seconds / 0.5144
```

Any ping where `implied_speed_kts > 50` is dropped.  This catches the classic AIS error where a vessel's GPS position "teleports" hundreds of kilometres in a single report interval — exactly what would create a false collision signal with a distant vessel.

---

## Computational Strategy

### Why not a Cartesian product?

A naïve join of all vessel pings against all other pings is O(n²).  
For 10 M rows that is 10¹⁴ pairs — completely impractical.

### Spatial-temporal bucketing

We use the same strategy as the Shadow Fleet project's MMSI partitioning, extended to 2D space:

1. **Time bucket**: `floor(unix_ts / 60)` — bins pings into 1-minute windows.
2. **Grid cell**: `floor(lon / 0.05)`, `floor(lat / 0.05)` — bins pings into ~3–5 km cells.
3. **Self-join** on `(minute_bucket, grid_x, grid_y)` with `mmsi_a < mmsi_b`.
4. **Neighbour expansion**: A's cell is also tested against its 8 adjacent cells (3×3 kernel), ensuring no pair straddling a cell boundary is missed.
5. **Exact Haversine** applied only to the surviving candidate pairs.

Effective complexity: **O(n × k²)** where k is the mean vessels per bucket  
(< 3 in open Baltic water) — a reduction of many orders of magnitude.

| Decision | Implementation | Benefit |
|----------|---------------|---------|
| No `inferSchema` | Manual `cast()` | Avoids double full-scan of 31 CSVs |
| Bbox pre-filter | Arithmetic on lat/lon | Eliminates ~90 % before expensive UDF |
| `.cache()` after denoising | Explicit materialisation | Avoids recomputing the pipeline twice |
| `shuffle.partitions=200` | SparkSession config | Balances parallelism vs overhead |
| `autoBroadcastJoinThreshold=50mb` | SparkSession config | Small DataFrames broadcast automatically |

---

## Results

> Results are printed to stdout when the container runs.

```
==============================================================
   AIS COLLISION DETECTION — FINAL RESULT
==============================================================
   Vessel A MMSI     :  <printed at runtime>
   Vessel A Name     :  <printed at runtime>
   Vessel B MMSI     :  <printed at runtime>
   Vessel B Name     :  <printed at runtime>
   Collision Time    :  <printed at runtime>
   Latitude          :  <printed at runtime> °N
   Longitude         :  <printed at runtime> °E
   Closest Distance  :  <printed at runtime> m
==============================================================
```

The trajectory visualisation is saved to `./output/trajectory_map.png`.

---

## Docker Hub & Rebuild

```bash
# Build image
docker build -t <your-dockerhub-username>/ais-collision-detection:latest .

# Local test
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/output:/app/output" \
  <your-dockerhub-username>/ais-collision-detection:latest

# Push to Docker Hub
docker login
docker push <your-dockerhub-username>/ais-collision-detection:latest
```

---

## Configuration Reference

All thresholds are defined as named constants at the top of `src/collision_detection.py`.

| Constant | Default | Description |
|----------|---------|-------------|
| `CENTER_LAT` | `55.225` | Area-of-interest centre latitude |
| `CENTER_LON` | `14.245` | Area-of-interest centre longitude |
| `RADIUS_NM` | `50.0` | Search radius in nautical miles |
| `GRID_SIZE` | `0.05` | Spatial bucket size in degrees (~3–5 km) |
| `MIN_SOG_KTS` | `0.5` | Minimum SOG to be considered moving |
| `MAX_SOG_KTS` | `50.0` | Maximum plausible speed (teleportation threshold) |
| `COLLISION_M` | `500.0` | Proximity threshold for collision (metres) |
| `TRAJ_WINDOW` | `10` | Minutes before/after collision to visualise |

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `DATA_DIR` | `/app/data` | AIS CSV file directory |
| `OUTPUT_DIR` | `/app/output` | Output directory |

---

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| PySpark | 3.5.1 | Distributed data processing |
| pandas | 2.2.2 | Post-processing & trajectory extraction |
| pyproj | 3.6.1 | Coordinate reprojection (WGS-84 → Web Mercator) |
| contextily | 1.6.0 | CartoDB Positron basemap tiles |
| matplotlib | 3.9.0 | Trajectory visualisation |
| Pillow | 10.3.0 | Image handling (contextily dependency) |
| requests | 2.32.3 | HTTP for tile fetcher & data downloader |

Java 17 (OpenJDK headless) is installed in the Docker image as required by PySpark.

---

*Danish AIS data © Danish Maritime Authority — published under open data terms.*
