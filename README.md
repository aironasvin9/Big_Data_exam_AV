# AIS Collision Detection — December 2021

> **Assignment 4** — Big Data Processing with PySpark  
> Danish AIS collision-like trajectory intersection detection  
> Fully containerized with Docker

---

## Table of Contents

1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [Repository Structure](#repository-structure)
4. [Methodology](#methodology)
5. [Data Quality & Noise Handling](#data-quality--noise-handling)
6. [Computational Strategy](#computational-strategy)
7. [Candidate Validation](#candidate-validation)
8. [Results](#results)
9. [Docker Hub & Image Export](#docker-hub--image-export)
10. [Configuration Reference](#configuration-reference)
11. [Dependencies](#dependencies)
12. [Limitations](#limitations)
13. [Data Source](#data-source)

---

## Overview

This project implements a PySpark pipeline for detecting a **collision-like AIS trajectory intersection** in Danish AIS data.

The analysis is restricted to:

- **Time period:** December 1, 2021 to December 31, 2021
- **Geographic area:** 50 nautical mile radius around:
  - Latitude: `55.225000`
  - Longitude: `14.245000`
- **Vessel state:** moving vessels only
- **Data source:** Danish AIS data from [http://aisdata.ais.dk/](http://aisdata.ais.dk/)

The pipeline uses PySpark DataFrame transformations for large-scale processing and only uses Pandas after the candidate set has been reduced to final trajectory extraction/plotting scale.

The final output includes:

- MMSI numbers of the two selected vessels
- Vessel names from AIS
- Vessel ship types from AIS
- Collision-like event timestamp
- Event coordinates
- Validation metrics
- A trajectory map over exactly ±10 minutes around the selected event

> **Important interpretation note:**  
> AIS data alone cannot legally prove physical collision. This project detects the strongest AIS-derived collision-like trajectory intersection after applying noise filtering, spatial-temporal candidate generation, and trajectory validation.

---

## Quick Start

### Prerequisites

- Docker ≥ 24 and Docker Compose v2
- Around 20 GB free disk space for the December 2021 Danish AIS CSV files
- Internet access if you want basemap tiles in the generated plot
- Extracted Danish AIS CSV files for December 2021

Expected data files:

```text
data/aisdk-2021-12-01.csv
data/aisdk-2021-12-02.csv
...
data/aisdk-2021-12-31.csv
```

---

### Option A — Docker Compose

```bash
git clone https://github.com/<your-username>/ais-collision-detection.git
cd ais-collision-detection

mkdir -p data output

# Place the 31 extracted December 2021 CSV files in ./data first.
docker compose up --build
```

Results:

```text
output/trajectory_map.png
stdout with selected MMSI pair, vessel names, timestamp, coordinates, and validation metrics
```

---

### Option B — Docker run

```bash
docker build -t ais-collision-detection:latest .

mkdir -p data output

docker run --rm \
  -e DATA_DIR=/app/data \
  -e OUTPUT_DIR=/app/output \
  -v "$(pwd)/data:/app/data:ro" \
  -v "$(pwd)/output:/app/output" \
  ais-collision-detection:latest
```

---

### Option C — Local Python run

If running outside Docker:

```bash
python -m venv venv311
source venv311/bin/activate

pip install -r requirements.txt

DATA_DIR=./data OUTPUT_DIR=./output python src/collision_detection.py
```

---

## Repository Structure

```text
ais-collision-detection/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── README.md
├── REPORT.md                         # optional written report, if included
├── src/
│   └── collision_detection.py         # main PySpark pipeline
├── data/                              # AIS CSVs, git-ignored
└── output/                            # generated outputs, git-ignored
    └── trajectory_map.png
```

If the repository includes an optional downloader script, it may also contain:

```text
src/download_data.py
```

However, the main pipeline assumes the December CSV files are already present in `DATA_DIR`.

---

## Methodology

The pipeline is organized into eight stages:

```text
load_raw()
    ↓
filter_noise()
    ↓
remove_teleportation()
    ↓
generate_candidates()
    ↓
find_collision()
    ↓
extract_trajectories()
    ↓
plot_trajectories()
    ↓
print_results()
```

---

### Stage 1 — Data Loading

The pipeline reads all CSV files from:

```text
DATA_DIR/*.csv
```

Schema inference is disabled:

```python
.option("inferSchema", "false")
```

This avoids a costly double scan over hundreds of millions of AIS rows. Columns are read as strings and explicitly cast during preprocessing.

Column names are normalized by lowercasing and replacing spaces, dashes, and dots with underscores.

---

### Stage 2 — Basic Cleaning and Assignment Filters

The cleaning stage applies:

1. malformed row filtering;
2. invalid/special MMSI filtering;
3. invalid coordinate filtering;
4. non-vessel/AtoN/base-station filtering;
5. stationary row filtering;
6. December 2021 time filtering;
7. 50 nautical mile geographic filtering.

The geographic filtering is performed in two steps:

1. **Bounding box pre-filter** using cheap latitude/longitude comparisons;
2. **Exact Haversine radius filter** for points within 50 nautical miles.

---

### Stage 3 — Teleportation / GPS Spike Removal

AIS data frequently contains GPS jumps. A single incorrect position can create a false collision candidate.

The pipeline removes isolated GPS spikes using a two-sided per-MMSI window:

```text
previous point → current point → next point
```

A point is considered an isolated spike when:

- movement from previous to current is implausibly fast;
- movement from current to next is implausibly fast;
- movement from previous to next is plausible.

This avoids the common mistake of removing a good point immediately after a bad GPS jump.

Default thresholds:

| Parameter | Default |
|---|---:|
| `TELEPORTATION_SPEED_KNOTS` | `60.0` |
| `GPS_SPIKE_MIN_DISTANCE_KM` | `1.0` |

---

### Stage 4 — Candidate Generation

A naive Cartesian product of all AIS pings is computationally infeasible. The pipeline instead uses spatial-temporal blocking.

For each AIS point, it creates:

- epoch timestamp in seconds;
- time bucket;
- spatial grid cell;
- optional shifted time buckets;
- optional shifted spatial grid cells.

Candidate pairs are generated by a self-join on:

```text
time_bucket
grid_x
grid_y
spatial_shift_id
time_shift_id
```

with:

```text
mmsi_a < mmsi_b
```

Then exact filters are applied:

- absolute ping time difference ≤ `PING_TIME_TOLERANCE_SEC`;
- exact Haversine distance ≤ `COLLISION_KM`.

This avoids a full Cartesian product while still allowing close-in-time AIS pings to be compared.

---

### Stage 5 — Candidate Validation

Close AIS pings alone are not sufficient. Tug operations, harbour manoeuvres, ferries, dock adjacency, and GPS noise can all create close-pair false positives.

The validation stage evaluates each unique vessel pair over a ±10 minute trajectory window.

For each candidate pair, the pipeline computes:

- pre-event distance;
- near-event distance;
- post-event distance where available;
- distance from selected event point to each vessel’s near-event trajectory;
- approach magnitude;
- divergence magnitude;
- estimated approach/divergence rate in knots;
- movement span for each vessel;
- post-event normal-continuation flag;
- near-event consistency flag.

The final event is selected by a validation score that rewards:

- strong approach;
- meaningful divergence or post-impact anomaly;
- movement by both vessels;
- close event distance;
- consistency between the raw candidate point and surrounding trajectory.

The score penalizes:

- tug/tow/pilot/rescue-like vessels;
- stationary pairs;
- pairs that are always close;
- implausible relative speeds;
- near-event inconsistency;
- ordinary close-passes where both vessels continue normally.

---

### Stage 6 — Trajectory Extraction

The final two vessels are extracted over exactly:

```text
collision time - 10 minutes
collision time + 10 minutes
```

The extraction is done using epoch seconds rather than Python timestamp literals to avoid timezone conversion issues between Spark, Pandas, Docker, and the host environment.

---

### Stage 7 — Visualization

The pipeline saves:

```text
output/trajectory_map.png
```

The plot shows:

- both vessel trajectories;
- start markers;
- end markers;
- selected collision-like closest-approach point;
- basemap when tile access is available.

---

### Stage 8 — Result Printing

The final selected event is printed to stdout, including:

- MMSI numbers;
- vessel names;
- ship types;
- timestamp;
- latitude/longitude;
- closest distance;
- validation metrics.

---

## Data Quality & Noise Handling

### Invalid MMSIs

The pipeline keeps normal ship MMSIs matching:

```text
^[2-7][0-9]{8}$
```

This excludes special/non-vessel AIS ranges such as:

| MMSI pattern | Meaning |
|---|---|
| `00xxxxxxx` | coast stations |
| `111xxxxxx` | SAR aircraft / special AIS use |
| `970xxxxxx` | AIS-SART |
| `972xxxxxx` | MOB |
| `974xxxxxx` | EPIRB-AIS |
| `99xxxxxxx` | AtoN / aids to navigation |

It also excludes repeated or known invalid dummy patterns such as:

```text
000000000
111111111
123456789
999999999
```

---

### Invalid Coordinates

The following are removed:

- latitude outside `[-90, 90]`;
- longitude outside `[-180, 180]`;
- positions close to `(0.0, 0.0)`.

---

### Non-Vessel Transponders

The pipeline removes rows whose `type_of_mobile` indicates:

- base station;
- AtoN;
- aid to navigation.

---

### Anchored, Moored, and Stationary Rows

Rows are removed when:

```text
navigational_status in {At anchor, Moored}
```

or:

```text
SOG < 0.5 knots
```

Pair-level validation is still used later to reject vessels that remain constantly close or stationary over the event window.

---

### Passenger / HSC / Ferry Near-Passes

Scheduled passenger, HSC, and ferry vessels can produce many close but normal encounters. These are excluded by default:

```text
EXCLUDE_PASSENGER_HSC_PAIRS=1
```

To disable this filter:

```bash
EXCLUDE_PASSENGER_HSC_PAIRS=0 python src/collision_detection.py
```

---

### Tug / Tow / Pilot / Rescue-Like Operations

The pipeline rejects candidate pairs whose AIS name or ship type matches patterns such as:

```text
tug
tow
towing
pusher
pilot
lods
assist
rescue
sar
```

This is intended to remove deliberate close-contact operations.

---

### Law-Enforcement / KBV Note

The final configuration **does not manually exclude** AIS-reported law-enforcement or KBV vessels. Vessel names and ship types are taken directly from the AIS data.

This is intentional: the pipeline treats the event as an AIS-derived collision-like trajectory intersection and does not manually override AIS vessel identity.

---

## Computational Strategy

### Avoiding Cartesian Products

The raw dataset contains hundreds of millions of AIS rows. A naive all-vs-all comparison would be impossible:

```text
O(n²)
```

Instead, the pipeline uses:

- time bucketing;
- spatial bucketing;
- shifted time buckets;
- optional shifted spatial buckets;
- exact Haversine distance only after candidate pruning.

This reduces the number of candidate comparisons by many orders of magnitude.

---

### Spark Optimization Choices

| Decision | Benefit |
|---|---|
| `inferSchema=false` | Avoids expensive second scan of all CSVs |
| explicit casts | Controlled schema handling |
| bounding-box pre-filter | Reduces rows before exact Haversine |
| Spark SQL Haversine expression | Avoids slow Python UDF for most distance work |
| `persist()` after denoising | Reuses cleaned data for candidate search and plotting |
| spatial-temporal self-join | Avoids full Cartesian product |
| broadcast event set during validation | Efficient validation of reduced candidate set |
| epoch-second trajectory extraction | Avoids timezone mismatch |

---

## Candidate Validation

The selected event must pass several checks.

### Moving Vessel Check

Both vessels must show movement over the ±10 minute window based on:

- movement span; or
- average SOG.

---

### Near-Event Consistency

The near-event trajectory must support the raw closest point. The pipeline checks that:

- the near-event average distance is not too large;
- the near-event distance is not wildly larger than the raw closest distance;
- the selected event point is close to each vessel’s near-event local trajectory.

---

### Physical Plausibility

The pipeline estimates the relative approach and divergence speed. Events implying impossible vessel motion are rejected.

Default maximum:

```text
MAX_VALIDATION_REL_SPEED_KNOTS = 80.0
```

---

### Post-Impact Anomaly

A normal near-pass often has both vessels continue normally after closest approach. A collision-like event is expected to show some abnormality:

- one vessel slows;
- one vessel stops;
- one vessel disappears from AIS;
- or the pair does not both continue normally.

This is controlled by:

```text
REQUIRE_POST_IMPACT_ANOMALY=1
```

To disable:

```bash
REQUIRE_POST_IMPACT_ANOMALY=0 python src/collision_detection.py
```

---

## Results

The final result is printed to stdout when the container or script runs.

Example final output format:

```text
========================================================================
   AIS COLLISION DETECTION — FINAL RESULT
========================================================================
   Vessel A MMSI              :  <printed at runtime>
   Vessel A Name              :  <printed at runtime>
   Vessel A Ship Type         :  <printed at runtime>
   Vessel B MMSI              :  <printed at runtime>
   Vessel B Name              :  <printed at runtime>
   Vessel B Ship Type         :  <printed at runtime>
   Collision Time             :  <printed at runtime>
   Latitude                   :  <printed at runtime> °N
   Longitude                  :  <printed at runtime> °E
   Closest Distance           :  <printed at runtime> m
   Pre-event Distance         :  <printed at runtime> m
   Near-event Distance        :  <printed at runtime> m
   Post-event Distance        :  <printed at runtime> m
   A Star-to-Near-Track Dist  :  <printed at runtime> m
   B Star-to-Near-Track Dist  :  <printed at runtime> m
   Approach Magnitude         :  <printed at runtime> m
   Divergence Magnitude       :  <printed at runtime> m
   Approach Rate Estimate     :  <printed at runtime> kn
   Divergence Rate Estimate   :  <printed at runtime> kn
   Near-event Consistent      :  <printed at runtime>
   Both Continue Normally     :  <printed at runtime>
   Post-impact Anomaly        :  <printed at runtime>
   Tug/Tow/Pilot Like         :  <printed at runtime>
   Validation Score           :  <printed at runtime>
========================================================================
```

The trajectory visualization is saved to:

```text
output/trajectory_map.png
```

If final artifacts are copied after execution, they may also appear as:

```text
output/final/trajectory_map_final.png
output/final/final_run.log
output/final/final_result.md
```

---

## Docker Hub & Image Export

### Build Image

```bash
docker build -t ais-collision-detection:latest .
```

---

### Local Test

```bash
docker run --rm \
  -e DATA_DIR=/app/data \
  -e OUTPUT_DIR=/app/output \
  -v "$(pwd)/data:/app/data:ro" \
  -v "$(pwd)/output:/app/output" \
  ais-collision-detection:latest
```

---

### Tag for Docker Hub

```bash
docker tag ais-collision-detection:latest <your-dockerhub-username>/ais-collision-detection:latest
```

---

### Push to Docker Hub

```bash
docker login
docker push <your-dockerhub-username>/ais-collision-detection:latest
```

---

### Export Docker Image as `.tar`

```bash
docker save ais-collision-detection:latest -o ais-collision-detection.tar
```

---

## Configuration Reference

Most thresholds can be changed through environment variables.

| Environment Variable | Default | Description |
|---|---:|---|
| `DATA_DIR` | `/app/data` | Input AIS CSV directory |
| `OUTPUT_DIR` | `/app/output` | Output directory |
| `GRID_SIZE` | `0.05` | Spatial grid size in degrees |
| `COLLISION_KM` | `0.5` | Maximum candidate proximity in km |
| `MIN_SOG_KNOTS` | `0.5` | Minimum row-level SOG to keep |
| `PING_TIME_TOLERANCE_SEC` | `90` | Max time difference between compared AIS pings |
| `TIME_BLOCK_SEC` | `180` | Time bucket size |
| `USE_SHIFTED_TIME_BLOCKING` | `1` | Enable shifted time buckets |
| `USE_SHIFTED_SPATIAL_BLOCKING` | `0` | Enable shifted spatial grid |
| `CANDIDATE_PAIR_LIMIT` | `5000` | Unique vessel-pair candidates validated |
| `GPS_SPIKE_MIN_DISTANCE_KM` | `1.0` | Minimum jump distance for GPS spike logic |
| `MAX_VALIDATION_REL_SPEED_KNOTS` | `80.0` | Max plausible relative validation speed |
| `MAX_NEAR_EVENT_DISTANCE_KM` | `0.75` | Max allowed near-event vessel separation |
| `MAX_NEAR_MINUS_CANDIDATE_KM` | `0.75` | Max near-distance minus raw candidate distance |
| `MAX_COLLISION_TO_NEAR_CENTROID_KM` | `1.25` | Max selected point distance to near-event trajectory centroid |
| `EXCLUDE_PASSENGER_HSC_PAIRS` | `1` | Exclude passenger/HSC/ferry close-passes |
| `REQUIRE_POST_IMPACT_ANOMALY` | `1` | Require abnormal post-event behaviour |
| `POST_CONTINUE_SOG_KNOTS` | `5.0` | SOG threshold for normal post-event continuation |
| `POST_CONTINUE_MIN_PINGS` | `3` | Minimum post-event pings to classify normal continuation |
| `RUN_SANITY_CHECK` | `0` | Run optional name search diagnostic |

Example:

```bash
USE_SHIFTED_SPATIAL_BLOCKING=1 \
CANDIDATE_PAIR_LIMIT=30000 \
MAX_NEAR_EVENT_DISTANCE_KM=1.00 \
python src/collision_detection.py
```

---

## Dependencies

Typical Python dependencies:

| Package | Purpose |
|---|---|
| PySpark | Distributed processing |
| pandas | Final trajectory extraction / plotting data handling |
| matplotlib | Plot generation |
| contextily | Basemap tiles |
| pyproj | Coordinate projection |
| numpy | Numerical dependency |
| shapely/geopandas | Optional geospatial dependencies if included in environment |

Java 17 or another Java runtime compatible with PySpark is required inside the Docker image.

---

## Limitations

1. **AIS is observational data.**  
   It can indicate a collision-like trajectory intersection but cannot legally prove impact.

2. **AIS can contain missing or delayed pings.**  
   The true closest approach may occur between reported AIS messages.

3. **Small vessels may have sparse AIS coverage.**  
   Pleasure craft and fishing vessels may transmit less consistently than large commercial ships.

4. **Operational close contact can resemble collision.**  
   Tug, rescue, pilot, and ferry patterns are filtered, but no rule-based system can perfectly distinguish every operational manoeuvre from a true collision.

5. **Post-impact anomaly is a modelling assumption.**  
   If a vessel stops transmitting after a close approach, this may indicate an incident, equipment loss, AIS coverage gap, or deliberate AIS silence.

6. **The final selected event should be interpreted as the strongest AIS-derived collision-like event found by this pipeline.**

---

## Data Source

Danish AIS data © Danish Maritime Authority, published as open data:

[http://aisdata.ais.dk/](http://aisdata.ais.dk/)
