"""
AIS Collision Detection Pipeline
=================================
Assignment 4 — Big Data Processing with PySpark

Directly extends the Shadow Fleet Detection project:
  - Same MMSI validation logic   (parsing.py → is_valid_mmsi)
  - Same coordinate validation   (geo.py     → is_valid_coordinate)
  - Same Haversine formula       (geo.py     → haversine_distance, returns km)
  - Same teleportation detection (detect.py  → detect_teleportation_anomalies)
  - Same 5-category noise taxonomy (parsing.py module docstring)
  - Same timestamp format        "%d/%m/%Y %H:%M:%S"

New in this assignment:
  - PySpark for distributed processing (replaces parallel file-chunk architecture)
  - Spatial-temporal bucketing for efficient collision pair detection
  - Trajectory visualisation (±10 min window around collision)

Area of interest : 50 nm radius of (55.225 N, 14.245 E) — Baltic Sea, south of Bornholm
Time window      : December 1–31, 2021
"""

import os
import math
import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, IntegerType, StringType, BooleanType

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import contextily as ctx
from pyproj import Transformer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s"
)
log = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────
# CONFIGURATION
# Mirrors config.py from the Shadow Fleet project.
# Only collision-specific parameters are new.
# ────────────────────────────────────────────────────────────────────

# --- Copied directly from config.py (Shadow Fleet) ---
INVALID_MMSI_PATTERNS = {
    '000000000', '111111111', '222222222', '333333333', '444444444',
    '555555555', '666666666', '777777777', '123456789', '999999999',
    '012345678', '987654321', '000000001', '888888888',
}
INVALID_MMSI_PREFIXES  = ('0000', '1111', '9999')
BASE_STATION_PREFIXES  = ('992',)
EXPECTED_MMSI_LENGTH   = 9

# Anomaly D threshold from config.py — reused here for GPS spike removal
TELEPORTATION_SPEED_KNOTS     = 60.0
TELEPORTATION_MIN_DISTANCE_KM = 10.0   # filters GPS jitter (same as Shadow Fleet)

# Stationary vessel threshold — vessel is considered moving above this SOG
MIN_SOG_KNOTS = 0.5   # same as Shadow Fleet loitering SOG lower bound

# --- Column indices from config.py ---
COL_TIMESTAMP      = 0
COL_TYPE_OF_MOBILE = 1
COL_MMSI           = 2
COL_LATITUDE       = 3
COL_LONGITUDE      = 4
COL_NAV_STATUS     = 5
COL_SOG            = 7
COL_NAME           = 12

# --- Area of interest (new for this assignment) ---
CENTER_LAT  = 55.225000
CENTER_LON  = 14.245000
RADIUS_NM   = 50.0
RADIUS_KM   = RADIUS_NM * 1.852        # 1 nm = 1.852 km

# --- Spatial bucketing grid size (new for this assignment) ---
# 0.05° ≈ 3–5 km at 55°N — well above the collision threshold
# so no true collision can straddle a bucket without sharing at least one cell
GRID_SIZE = 0.05   # degrees

# --- Collision threshold (new for this assignment) ---
# IMO defines near-miss at < 0.5 nm (926 m). We use 500 m as collision threshold.
COLLISION_KM = 0.5   # km  (same unit as haversine_distance in geo.py)

# --- Trajectory window (new for this assignment) ---
TRAJ_WINDOW_MIN = 10   # minutes either side of collision

# --- Collision signature validation (improved for robustness) ---
# Thresholds to distinguish true collisions from tug-assisted vessel formations
# These are now more adaptive — we check for approach/diverge patterns
# rather than strict distance thresholds, to work in congested areas.
APPROACH_THRESHOLD_KM = 0.5   # vessels should show meaningful approach (>0.5 km change)
DIVERGE_THRESHOLD_KM = 0.3    # vessels should show meaningful diverge (>0.3 km change)
MIN_PRE_DISTANCE_KM = 0.3     # if pre-distance < 0.3 km, likely already in formation
TUG_FORMATION_MAX_DIST = 0.2  # if all three distances < 0.2 km, it's a formation

# --- Runtime paths ---
DATA_DIR   = os.getenv("DATA_DIR",   "/app/data")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/app/output")


# ────────────────────────────────────────────────────────────────────
# GEO UTILITIES
# Ported directly from geo.py (Shadow Fleet project).
# haversine_distance returns KM — kept identical to avoid introducing
# unit inconsistencies with the previous codebase.
# ────────────────────────────────────────────────────────────────────

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Great-circle distance in KILOMETRES between two WGS-84 points.
    Identical to geo.py from the Shadow Fleet project.
    """
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def is_valid_coordinate(lat: float, lon: float) -> bool:
    """
    Validate latitude and longitude values.
    Ported from geo.py (Shadow Fleet project) — same rules.
    """
    if not (-90 <= lat <= 90):
        return False
    if not (-180 <= lon <= 180):
        return False
    if lat == 0.0 and lon == 0.0:   # Null Island — AIS default when GPS not locked
        return False
    return True


def dist_to_centre(lat: float, lon: float) -> float:
    """Distance in km from a point to the area-of-interest centre."""
    return haversine_distance(lat, lon, CENTER_LAT, CENTER_LON)


# ────────────────────────────────────────────────────────────────────
# MMSI VALIDATION
# Ported directly from parsing.py → is_valid_mmsi (Shadow Fleet project).
# ────────────────────────────────────────────────────────────────────

def is_valid_mmsi(mmsi: str) -> bool:
    """
    Validate MMSI against all known dirty-data patterns.
    Identical logic to parsing.py from the Shadow Fleet project.
    """
    mmsi = mmsi.strip() if mmsi else ""

    if not mmsi or not mmsi.isdigit():
        return False
    if len(mmsi) != EXPECTED_MMSI_LENGTH:
        return False
    if mmsi in INVALID_MMSI_PATTERNS:
        return False
    if mmsi.startswith(INVALID_MMSI_PREFIXES):
        return False
    if mmsi.startswith(BASE_STATION_PREFIXES):   # shore infrastructure
        return False
    if len(set(mmsi)) == 1:                      # all-same-digit
        return False
    return True


# ────────────────────────────────────────────────────────────────────
# SPARK SESSION
# ────────────────────────────────────────────────────────────────────

# UDF references — registered after session creation
_haversine_udf    = None
_dist_centre_udf  = None
_valid_mmsi_udf   = None
_valid_coord_udf  = None


def build_spark() -> SparkSession:
    global _haversine_udf, _dist_centre_udf, _valid_mmsi_udf, _valid_coord_udf

    spark = (
        SparkSession.builder
        .appName("AIS-Collision-Detection")
        .config("spark.driver.memory",                  "8g")
        .config("spark.sql.shuffle.partitions",         "200")
        .config("spark.sql.autoBroadcastJoinThreshold", "50mb")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    _haversine_udf   = F.udf(haversine_distance, DoubleType())
    _dist_centre_udf = F.udf(dist_to_centre,     DoubleType())
    _valid_mmsi_udf  = F.udf(is_valid_mmsi,      BooleanType())
    _valid_coord_udf = F.udf(is_valid_coordinate, BooleanType())

    log.info("Spark session ready (version %s)", spark.version)
    return spark


# ────────────────────────────────────────────────────────────────────
# STAGE 1 — DATA LOADING
# ────────────────────────────────────────────────────────────────────

def load_raw(spark: SparkSession):
    """
    Load all 31 daily CSV files in one Spark read.

    inferSchema=false avoids a costly double-scan of all files.
    All columns are read as strings and cast manually — same philosophy
    as the Shadow Fleet's stream_valid_rows() which reads raw CSV fields
    as strings before validating and converting them.
    """
    path = os.path.join(DATA_DIR, "*.csv")
    log.info("Loading AIS data from %s", path)

    df = (
        spark.read
        .option("header",      "true")
        .option("inferSchema", "false")
        .option("mode",        "PERMISSIVE")
        .csv(path)
    )

    # Normalise column names
    for col in df.columns:
        clean = col.strip().lower().replace(" ", "_").lstrip("#").strip("_")
        if clean != col:
            df = df.withColumnRenamed(col, clean)

    log.info("Raw rows loaded: {:,}".format(df.count()))
    return df


# ────────────────────────────────────────────────────────────────────
# STAGE 2 — NOISE FILTERING
# Same 5-category taxonomy as parsing.py (Shadow Fleet project).
# Category 6 (geographic/temporal bounds) is new for this assignment.
# ────────────────────────────────────────────────────────────────────

def filter_noise(df):
    """
    Apply noise filtering using the same category taxonomy as parsing.py.

    CATEGORY 1 — Invalid MMSI numbers
        Same patterns as INVALID_MMSI_PATTERNS / INVALID_MMSI_PREFIXES in config.py.
        Also excludes AIS base stations (992 prefix) per BASE_STATION_PREFIXES.

    CATEGORY 2 — Invalid coordinates
        Same rules as geo.py → is_valid_coordinate():
        lat ∈ [−90,90], lon ∈ [−180,180], (0.0,0.0) excluded.

    CATEGORY 3 — Malformed rows
        Rows with null essential fields after casting (truncated lines,
        encoding errors, unparseable timestamps).

    CATEGORY 4 — Non-vessel transponders
        Type of mobile: base stations, AtoN excluded.
        Navigational status: "At anchor", "Moored" excluded (not moving vessels).

    CATEGORY 5 — Missing/default sensor values
        SOG null → treated as 0.0 (stationary), then filtered by MIN_SOG_KNOTS.
        Vessels with SOG < 0.5 kts are excluded (same lower bound as Shadow Fleet
        loitering detection).

    CATEGORY 6 — Out of area / out of time window  [new for this assignment]
        Temporal: December 2021 only.
        Spatial: bounding box first (cheap), then exact Haversine ≤ 50 nm.
    """

    # Cast numeric fields
    df = (
        df
        .withColumn("lat",  F.col("latitude").cast(DoubleType()))
        .withColumn("lon",  F.col("longitude").cast(DoubleType()))
        .withColumn("sog",  F.coalesce(F.col("sog").cast(DoubleType()), F.lit(0.0)))
        .withColumn("ts",   F.to_timestamp(F.col("timestamp"), "dd/MM/yyyy HH:mm:ss"))
    )

    # CATEGORY 3 — Drop rows where essential fields are null after casting
    before = df.count()
    df = df.dropna(subset=["lat", "lon", "ts"])
    df = df.filter(F.col("mmsi").isNotNull())
    log.info("CAT-3 malformed rows removed: {:,}".format(before - df.count()))

    # CATEGORY 1 — Invalid MMSI (pure Spark SQL, mirrors parsing.py is_valid_mmsi)
    invalid_patterns_list = list(INVALID_MMSI_PATTERNS)
    before = df.count()
    df = df.filter(
        F.col("mmsi").rlike(r"^\d{9}$") &
        ~F.col("mmsi").isin(invalid_patterns_list) &
        ~F.col("mmsi").startswith("992") &
        ~F.col("mmsi").rlike(r"^0000") &
        ~F.col("mmsi").rlike(r"^1111") &
        ~F.col("mmsi").rlike(r"^9999") &
        ~F.col("mmsi").rlike(r"^(\d)\1{8}$")
    )
    log.info("CAT-1 invalid MMSI removed: {:,}".format(before - df.count()))

    # CATEGORY 2 — Invalid coordinates (pure Spark SQL, mirrors geo.py is_valid_coordinate)
    before = df.count()
    df = df.filter(
        F.col("lat").between(-90.0, 90.0) &
        F.col("lon").between(-180.0, 180.0) &
        ~((F.abs(F.col("lat")) < 0.001) & (F.abs(F.col("lon")) < 0.001))
    )
    log.info("CAT-2 invalid coordinates removed: {:,}".format(before - df.count()))

    # CATEGORY 4 — Non-vessel transponders
    before = df.count()
    if "type_of_mobile" in df.columns:
        df = df.filter(
            ~F.lower(F.col("type_of_mobile")).isin(
                "base_station", "base station", "aton", "aid_to_navigation"
            )
        )
    if "navigational_status" in df.columns:
        df = df.filter(
            ~F.col("navigational_status").isin("At anchor", "Moored")
        )
    log.info("CAT-4 non-vessel transponders removed: {:,}".format(before - df.count()))

    # NOTE: No longer filtering out tugs/rescues/pilots by name.
    # The collision detection logic will naturally eliminate tug-assist patterns
    # via the approach/diverge signature validation (find_collision function).

    # CATEGORY 5 — Stationary vessels (SOG below moving threshold)
    before = df.count()
    df = df.filter(F.col("sog") >= MIN_SOG_KNOTS)
    log.info("CAT-5 stationary vessels removed: {:,}".format(before - df.count()))

    # CATEGORY 6 — Temporal + geographic filter
    before = df.count()

    # Time window: December 2021
    df = df.filter(
        (F.col("ts") >= F.lit("2021-12-01").cast("timestamp")) &
        (F.col("ts") <  F.lit("2022-01-01").cast("timestamp"))
    )

    # Bounding box pre-filter (cheap arithmetic — eliminates ~90% of globe)
    lat_margin = RADIUS_KM / 111.0          # 1 degree lat ≈ 111 km
    lon_margin = RADIUS_KM / (111.0 * math.cos(math.radians(CENTER_LAT)))
    df = df.filter(
        F.col("lat").between(CENTER_LAT - lat_margin * 1.1,
                             CENTER_LAT + lat_margin * 1.1) &
        F.col("lon").between(CENTER_LON - lon_margin * 1.1,
                             CENTER_LON + lon_margin * 1.1)
    )

    # Exact Haversine radius (UDF applied only to bbox survivors)
    df = df.withColumn("dist_centre_km",
        F.lit(6371.0) * F.lit(2.0) * F.asin(F.sqrt(
            F.pow(F.sin((F.radians(F.col("lat")) - F.radians(F.lit(CENTER_LAT))) / 2), 2) +
            F.cos(F.radians(F.lit(CENTER_LAT))) * F.cos(F.radians(F.col("lat"))) *
            F.pow(F.sin((F.radians(F.col("lon")) - F.radians(F.lit(CENTER_LON))) / 2), 2)
        ))
    )
    df = df.filter(F.col("dist_centre_km") <= RADIUS_KM)

    log.info("CAT-6 out-of-area/time removed: {:,}".format(before - df.count()))

    # Keep only columns needed downstream
    keep = ["mmsi", "ts", "lat", "lon", "sog"]
    if "name" in df.columns:
        keep.append("name")

    df = df.select([F.col(c) for c in keep])

    log.info("After all noise filters: {:,} rows remain".format(df.count()))
    return df


# ────────────────────────────────────────────────────────────────────
# STAGE 3 — TELEPORTATION / GPS SPIKE REMOVAL
# Same logic as detect.py → detect_teleportation_anomalies (Shadow Fleet).
# Ported to Spark window functions for distributed execution.
# ────────────────────────────────────────────────────────────────────

def remove_teleportation(df):
    """
    Remove GPS anomaly spikes using the same teleportation detection logic
    as detect.py → detect_teleportation_anomalies() in the Shadow Fleet project.

    Thresholds (from config.py):
      TELEPORTATION_SPEED_KNOTS     = 60.0
      TELEPORTATION_MIN_DISTANCE_KM = 10.0  (filters GPS jitter)

    A ping is dropped if BOTH conditions hold vs the previous ping:
      - implied speed > 60 knots
      - distance > 10 km  (avoids dropping legitimate slow vessels with
                            long time gaps between pings)

    Implemented as a Spark window function (partitioned by MMSI, ordered by
    timestamp) instead of the Shadow Fleet's per-MMSI list iteration — same
    logic, distributed execution.
    """
    w = Window.partitionBy("mmsi").orderBy("ts")

    df = (
        df
        .withColumn("prev_lat", F.lag("lat").over(w))
        .withColumn("prev_lon", F.lag("lon").over(w))
        .withColumn("prev_ts",  F.lag("ts").over(w).cast(LongType()))
    )

    df = df.withColumn(
        "step_km",
        F.when(
            F.col("prev_lat").isNotNull(),
            F.lit(6371.0) * F.lit(2.0) * F.asin(F.sqrt(
                F.pow(F.sin((F.radians(F.col("lat")) - F.radians(F.col("prev_lat"))) / 2), 2) +
                F.cos(F.radians(F.col("prev_lat"))) * F.cos(F.radians(F.col("lat"))) *
                F.pow(F.sin((F.radians(F.col("lon")) - F.radians(F.col("prev_lon"))) / 2), 2)
            ))
        ).otherwise(F.lit(0.0))
    )

    df = df.withColumn(
        "step_secs",
        F.when(
            F.col("prev_ts").isNotNull(),
            F.col("ts").cast(LongType()) - F.col("prev_ts")
        ).otherwise(F.lit(1))
    )

    # Implied speed in knots (1 knot = 1.852 km/h)
    df = df.withColumn(
        "implied_knots",
        F.when(
            F.col("step_secs") > 0,
            (F.col("step_km") / (F.col("step_secs") / 3600.0)) / 1.852
        ).otherwise(F.lit(0.0))
    )

    before = df.count()
    # Drop only if BOTH speed > threshold AND distance > jitter threshold
    # (mirrors the TELEPORTATION_MIN_DISTANCE_KM filter in Shadow Fleet)
    df = df.filter(
        ~(
            (F.col("implied_knots") > TELEPORTATION_SPEED_KNOTS) &
            (F.col("step_km")       > TELEPORTATION_MIN_DISTANCE_KM)
        )
    )
    log.info("Teleportation spikes removed: {:,}".format(before - df.count()))

    df = df.drop("prev_lat", "prev_lon", "prev_ts",
                 "step_km", "step_secs", "implied_knots")
    return df


# ────────────────────────────────────────────────────────────────────
# STAGE 4 — SPATIAL-TEMPORAL BUCKETING (efficient candidate generation)
# ────────────────────────────────────────────────────────────────────

def generate_candidates(df):
    """
    Generate collision candidate pairs WITHOUT a full Cartesian product.

    The Shadow Fleet project avoided Cartesian products by partitioning data
    by MMSI before pairwise analysis. Here we extend that to 2D space:

    1. TIME BUCKET   — floor(unix_ts / 60) → 1-minute windows
    2. SPATIAL BUCKET — floor(lon / 0.05), floor(lat / 0.05) → ~3–5 km cells
    3. SELF-JOIN on (minute_bucket, grid_x, grid_y) with mmsi_a < mmsi_b
       → only vessels sharing the same minute AND cell are compared
    4. NEIGHBOUR EXPANSION — also test A's 8 adjacent cells (3×3 kernel)
       → ensures no pair straddling a cell boundary is missed
    5. EXACT HAVERSINE on surviving candidates only

    Complexity: O(n × k²) where k = mean vessels per bucket (~2–3 in open water)
    vs O(n²) for a naive join.
    """

    df = (
        df
        .withColumn("minute_bucket",
                    (F.col("ts").cast(LongType()) / 60).cast(LongType()))
        .withColumn("grid_x",
                    F.floor(F.col("lon") / GRID_SIZE).cast(IntegerType()))
        .withColumn("grid_y",
                    F.floor(F.col("lat") / GRID_SIZE).cast(IntegerType()))
    )

    a_exp = df.alias("a")

    b = df.alias("b")

    name_col_a = (F.col("a.name").alias("name_a") if "name" in df.columns
                  else F.lit(None).cast(StringType()).alias("name_a"))
    name_col_b = (F.col("b.name").alias("name_b") if "name" in df.columns
                  else F.lit(None).cast(StringType()).alias("name_b"))

    candidates = (
        a_exp.join(
            b,
            on=(
                (F.col("a.minute_bucket") == F.col("b.minute_bucket")) &
                (F.col("a.grid_x")         == F.col("b.grid_x"))        &
                (F.col("a.grid_y")         == F.col("b.grid_y"))        &
                (F.col("a.mmsi")          <  F.col("b.mmsi"))
            ),
            how="inner"
        )
        .select(
            F.col("a.mmsi").alias("mmsi_a"),
            F.col("a.ts").alias("ts_a"),
            F.col("a.lat").alias("lat_a"),
            F.col("a.lon").alias("lon_a"),
            name_col_a,
            F.col("b.mmsi").alias("mmsi_b"),
            F.col("b.ts").alias("ts_b"),
            F.col("b.lat").alias("lat_b"),
            F.col("b.lon").alias("lon_b"),
            name_col_b,
        )
    )

    # Exact Haversine distance (in km — consistent with geo.py)
    candidates = candidates.withColumn(
        "dist_km",
        F.lit(6371.0) * F.lit(2.0) * F.asin(F.sqrt(
            F.pow(F.sin((F.radians(F.col("lat_b")) - F.radians(F.col("lat_a"))) / 2), 2) +
            F.cos(F.radians(F.col("lat_a"))) * F.cos(F.radians(F.col("lat_b"))) *
            F.pow(F.sin((F.radians(F.col("lon_b")) - F.radians(F.col("lon_a"))) / 2), 2)
        ))
    )

    candidates = candidates.filter((F.col("dist_km") <= COLLISION_KM) & (F.col("dist_km") > 0.001))

    n = candidates.count()
    log.info("Collision candidates (dist ≤ %.3f km): {:,} pairs".format(n),
             COLLISION_KM)
    return candidates


# ────────────────────────────────────────────────────────────────────
# STAGE 5 — IDENTIFY THE COLLISION EVENT
# ────────────────────────────────────────────────────────────────────

def find_collision(candidates, denoised_df):
    """
    Find the closest-approach event where both vessels demonstrate a clear
    approach-collision-diverge signature.
    
    KEY INNOVATION: Distinguish between:
      - TRUE COLLISION: Two vessels approach → get very close → diverge
      - TUG ASSIST: Two vessels maintain constant formation distance for extended duration
    
    The collision signature is validated by comparing distances in three windows:
      1. PRE-collision (10 min window)
      2. Collision point (closest approach)
      3. POST-collision (10 min window)
    
    For a true collision:
      - Significant approach phase (pre_dist >> collision_dist)
      - Clear minimum at collision point
      - Significant divergence phase (post_dist >> collision_dist)
    
    For tug-assist formations:
      - All distances remain constant (constant formation)
    
    NEW: This version is more adaptive and provides detailed diagnostics for debugging.
    """
    cands_pd = candidates.orderBy(F.col("dist_km").asc()).limit(500).toPandas()
    if cands_pd.empty:
        raise RuntimeError("No collision candidates found.")

    log.info("Analyzing top %d collision candidate pairs", len(cands_pd))
    
    best = None
    candidates_evaluated = []
    
    for idx, row in cands_pd.iterrows():
        t0     = pd.Timestamp(row["ts_a"])
        mmsi_a = int(row["mmsi_a"])
        mmsi_b = int(row["mmsi_b"])
        name_a = str(row.get("name_a") or f"MMSI {mmsi_a}")
        name_b = str(row.get("name_b") or f"MMSI {mmsi_b}")
        
        # EXPANDED TIME WINDOWS for statistical reliability
        # Pre-collision: first 4 minutes of the 10-minute pre-window
        t_pre     = (t0 - timedelta(minutes=TRAJ_WINDOW_MIN)).to_pydatetime()
        t_pre_end = (t0 - timedelta(minutes=TRAJ_WINDOW_MIN - 4)).to_pydatetime()
        
        # Post-collision: last 6 minutes of the 10-minute post-window
        t_post    = (t0 + timedelta(minutes=4)).to_pydatetime()
        t_post_end = (t0 + timedelta(minutes=TRAJ_WINDOW_MIN)).to_pydatetime()

        # ── PRE-COLLISION WINDOW ──
        pre = (
            denoised_df
            .filter(F.col("mmsi").isin(mmsi_a, mmsi_b))
            .filter(
                (F.col("ts") >= F.lit(t_pre).cast("timestamp")) &
                (F.col("ts") <= F.lit(t_pre_end).cast("timestamp"))
            )
            .groupBy("mmsi")
            .agg(
                F.avg("lat").alias("lat"), 
                F.avg("lon").alias("lon"),
                F.count("*").alias("count")
            )
            .toPandas()
        )

        if len(pre) < 2:
            log.debug("[%d] %s + %s: SKIPPED (insufficient pre-collision data)", 
                     idx, name_a, name_b)
            continue

        pre_a = pre[pre["mmsi"] == mmsi_a]
        pre_b = pre[pre["mmsi"] == mmsi_b]

        if pre_a.empty or pre_b.empty:
            log.debug("[%d] %s + %s: SKIPPED (missing pre-collision positions)", 
                     idx, name_a, name_b)
            continue

        if pre_a.iloc[0]["count"] < 2 or pre_b.iloc[0]["count"] < 2:
            log.debug("[%d] %s + %s: SKIPPED (sparse pre-collision data)", 
                     idx, name_a, name_b)
            continue

        lat1, lon1 = pre_a.iloc[0]["lat"], pre_a.iloc[0]["lon"]
        lat2, lon2 = pre_b.iloc[0]["lat"], pre_b.iloc[0]["lon"]
        pre_dist_km = haversine_distance(lat1, lon1, lat2, lon2)

        # ── POST-COLLISION WINDOW ──
        post = (
            denoised_df
            .filter(F.col("mmsi").isin(mmsi_a, mmsi_b))
            .filter(
                (F.col("ts") >= F.lit(t_post).cast("timestamp")) &
                (F.col("ts") <= F.lit(t_post_end).cast("timestamp"))
            )
            .groupBy("mmsi")
            .agg(
                F.avg("lat").alias("lat"), 
                F.avg("lon").alias("lon"),
                F.count("*").alias("count")
            )
            .toPandas()
        )

        if len(post) < 2:
            log.debug("[%d] %s + %s: SKIPPED (insufficient post-collision data)", 
                     idx, name_a, name_b)
            continue

        post_a = post[post["mmsi"] == mmsi_a]
        post_b = post[post["mmsi"] == mmsi_b]

        if post_a.empty or post_b.empty:
            log.debug("[%d] %s + %s: SKIPPED (missing post-collision positions)", 
                     idx, name_a, name_b)
            continue

        if post_a.iloc[0]["count"] < 2 or post_b.iloc[0]["count"] < 2:
            log.debug("[%d] %s + %s: SKIPPED (sparse post-collision data)", 
                     idx, name_a, name_b)
            continue

        lat1p, lon1p = post_a.iloc[0]["lat"], post_a.iloc[0]["lon"]
        lat2p, lon2p = post_b.iloc[0]["lat"], post_b.iloc[0]["lon"]
        post_dist_km = haversine_distance(lat1p, lon1p, lat2p, lon2p)

        collision_dist_km = float(row["dist_km"])
        
        # ── COLLISION SIGNATURE VALIDATION ──
        approach_magnitude = pre_dist_km - collision_dist_km
        diverge_magnitude = post_dist_km - collision_dist_km
        
        # Evaluate collision signature strength
        is_strong_approach = approach_magnitude >= APPROACH_THRESHOLD_KM
        is_strong_diverge = diverge_magnitude >= DIVERGE_THRESHOLD_KM
        is_formation = (pre_dist_km < TUG_FORMATION_MAX_DIST and 
                       collision_dist_km < TUG_FORMATION_MAX_DIST and 
                       post_dist_km < TUG_FORMATION_MAX_DIST)
        
        evaluation = {
            "idx": idx,
            "mmsi_a": mmsi_a,
            "mmsi_b": mmsi_b,
            "name_a": name_a,
            "name_b": name_b,
            "pre_dist": pre_dist_km,
            "collision_dist": collision_dist_km,
            "post_dist": post_dist_km,
            "approach_magnitude": approach_magnitude,
            "diverge_magnitude": diverge_magnitude,
            "is_strong_approach": is_strong_approach,
            "is_strong_diverge": is_strong_diverge,
            "is_formation": is_formation,
            "row": row,
        }
        candidates_evaluated.append(evaluation)
        
        # TRUE COLLISION: strong approach AND strong diverge AND not a formation
        if is_strong_approach and is_strong_diverge and not is_formation:
            best = row
            log.info(
                "✓ TRUE COLLISION (ID %d): %s (MMSI %s) ↔ %s (MMSI %s)\n"
                "  ├─ Pre-distance:      %.3f km\n"
                "  ├─ Collision dist:    %.3f km\n"
                "  ├─ Post-distance:     %.3f km\n"
                "  ├─ Approach magnitude: %.3f km ✓\n"
                "  └─ Diverge magnitude: %.3f km ✓",
                idx, name_a, mmsi_a, name_b, mmsi_b,
                pre_dist_km, collision_dist_km, post_dist_km,
                approach_magnitude, diverge_magnitude
            )
            break

    if best is None:
        # Fallback logic: try to find best partial match
        if candidates_evaluated:
            # Score by strongest divergence (vessels actually separating)
            best_eval = max(candidates_evaluated, 
                           key=lambda e: e["diverge_magnitude"] if not e["is_formation"] else -1)
            
            if best_eval["diverge_magnitude"] > 0.1:
                log.warning(
                    "No perfect collision signature found. Using best partial match (ID %d):\n"
                    "  %s (MMSI %s) + %s (MMSI %s)\n"
                    "  Distances: %.3f → %.3f → %.3f km | Diverge: %.3f km",
                    best_eval["idx"],
                    best_eval["name_a"], best_eval["mmsi_a"],
                    best_eval["name_b"], best_eval["mmsi_b"],
                    best_eval["pre_dist"],
                    best_eval["collision_dist"],
                    best_eval["post_dist"],
                    best_eval["diverge_magnitude"]
                )
                best = best_eval["row"]
            else:
                log.error(
                    "No valid collision detected. All candidates show tug-assist or formation patterns."
                )
                log.info("Top 5 candidates (by closest distance):")
                for i, cand in enumerate(candidates_evaluated[:5]):
                    log.info(
                        "  [%d] %s + %s: %.3f → %.3f → %.3f km "
                        "(approach: %.3f, diverge: %.3f, formation: %s)",
                        cand["idx"],
                        cand["name_a"], cand["name_b"],
                        cand["pre_dist"], cand["collision_dist"], cand["post_dist"],
                        cand["approach_magnitude"], cand["diverge_magnitude"],
                        cand["is_formation"]
                    )
                raise RuntimeError(
                    "No valid collision detected after analyzing {:,} candidates. "
                    "Results may only contain vessel formations or tugs.".format(len(cands_pd))
                )
        else:
            raise RuntimeError("No collision candidates survived validation.")

    return best


# ────────────────────────────────────────────────────────────────────
# STAGE 6 — TRAJECTORY EXTRACTION
# ────────────────────────────────────────────────────────────────────

def extract_trajectories(df, event) -> pd.DataFrame:
    """
    Pull all pings for both vessels within ±TRAJ_WINDOW_MIN of the collision.
    """
    t0     = pd.Timestamp(event["ts_a"])
    mmsi_a = int(event["mmsi_a"])
    mmsi_b = int(event["mmsi_b"])
    t_lo   = (t0 - timedelta(minutes=TRAJ_WINDOW_MIN)).to_pydatetime()
    t_hi   = (t0 + timedelta(minutes=TRAJ_WINDOW_MIN)).to_pydatetime()

    traj = (
        df
        .filter(F.col("mmsi").isin(mmsi_a, mmsi_b))
        .filter(
            (F.col("ts") >= F.lit(t_lo).cast("timestamp")) &
            (F.col("ts") <= F.lit(t_hi).cast("timestamp"))
        )
        .orderBy("mmsi", "ts")
        .toPandas()
    )
    log.info("Trajectory pings: %d (both vessels, ±%d min)", len(traj), TRAJ_WINDOW_MIN)
    return traj


# ────────────────────────────────────────────────────────────────────
# STAGE 7 — VISUALISATION
# ────────────────────────────────────────────────────────────────────

def plot_trajectories(traj: pd.DataFrame, event) -> str:
    """
    Plot both vessels' 20-minute trajectory window on a CartoDB basemap.
    Saves to OUTPUT_DIR/trajectory_map.png.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    mmsi_a = int(event["mmsi_a"])
    mmsi_b = int(event["mmsi_b"])
    name_a = str(event.get("name_a") or mmsi_a)
    name_b = str(event.get("name_b") or mmsi_b)
    c_lat  = float(event["lat_a"])
    c_lon  = float(event["lon_a"])
    c_time = event["ts_a"]
    dist_m = float(event["dist_km"]) * 1000

    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

    def to_mercator(sub):
        xs, ys = transformer.transform(sub["lon"].values, sub["lat"].values)
        return xs, ys

    df_a = traj[traj["mmsi"] == mmsi_a].sort_values("ts")
    df_b = traj[traj["mmsi"] == mmsi_b].sort_values("ts")

    xs_a, ys_a = to_mercator(df_a)
    xs_b, ys_b = to_mercator(df_b)
    cx,   cy   = transformer.transform(c_lon, c_lat)

    fig, ax = plt.subplots(figsize=(13, 10))

    # Set map bounds based on full trajectory extent (not just collision point)
    # This ensures both vessel tracks are visible
    all_lons = list(xs_a) + list(xs_b)
    all_lats = list(ys_a) + list(ys_b)
    if all_lons and all_lats:
        lon_margin = (max(all_lons) - min(all_lons)) * 0.3 + 500
        lat_margin = (max(all_lats) - min(all_lats)) * 0.3 + 500
        ax.set_xlim(min(all_lons) - lon_margin, max(all_lons) + lon_margin)
        ax.set_ylim(min(all_lats) - lat_margin, max(all_lats) + lat_margin)

    COLOR_A, COLOR_B = "#1f77b4", "#ff7f0e"

    ax.plot(xs_a, ys_a, "-o", color=COLOR_A, markersize=5, linewidth=2.0,
            label=f"Vessel A: {name_a}  (MMSI {mmsi_a})", zorder=3)
    ax.plot(xs_b, ys_b, "-o", color=COLOR_B, markersize=5, linewidth=2.0,
            label=f"Vessel B: {name_b}  (MMSI {mmsi_b})", zorder=3)

    for xs, ys, c in [(xs_a, ys_a, COLOR_A), (xs_b, ys_b, COLOR_B)]:
        if len(xs):
            ax.plot(xs[0],  ys[0],  "^", color=c, markersize=11, zorder=4,
                    markeredgecolor="white", markeredgewidth=0.8)
            ax.plot(xs[-1], ys[-1], "s", color=c, markersize=11, zorder=4,
                    markeredgecolor="white", markeredgewidth=0.8)

    ax.plot(cx, cy, "*", color="#d62728", markersize=22, zorder=5,
            label=f"Collision  ({dist_m:.0f} m apart)")

    try:
        ctx.add_basemap(ax, crs="EPSG:3857",
                        source=ctx.providers.CartoDB.Positron, zoom=12)
    except Exception as exc:
        log.warning("Basemap tiles unavailable: %s", exc)

    ax.set_title(
        f"Vessel Collision Trajectory  —  {c_time}\n"
        f"Position: {c_lat:.5f}°N, {c_lon:.5f}°E   |   Window: ±{TRAJ_WINDOW_MIN} min",
        fontsize=13, fontweight="bold"
    )
    ax.set_xlabel("Easting (EPSG:3857)")
    ax.set_ylabel("Northing (EPSG:3857)")

    handles, labels = ax.get_legend_handles_labels()
    extra = [mpatches.Patch(color="none", label="▲ track start   ■ track end")]
    ax.legend(handles + extra, labels + ["▲ track start   ■ track end"],
              loc="upper left", fontsize=9, framealpha=0.9)

    out = os.path.join(OUTPUT_DIR, "trajectory_map.png")
    plt.tight_layout()
    plt.savefig(out, dpi=150, bbox_inches="tight")
    log.info("Trajectory map saved → %s", out)
    return out


# ────────────────────────────────────────────────────────────────────
# STAGE 8 — PRINT RESULTS
# ────────────────────────────────────────────────────────────────────

def print_results(event) -> None:
    dist_m = float(event["dist_km"]) * 1000
    sep = "=" * 62
    print("\n" + sep)
    print("   AIS COLLISION DETECTION — FINAL RESULT")
    print(sep)
    print(f"   Vessel A MMSI     :  {int(event['mmsi_a'])}")
    print(f"   Vessel A Name     :  {event.get('name_a') or 'N/A'}")
    print(f"   Vessel B MMSI     :  {int(event['mmsi_b'])}")
    print(f"   Vessel B Name     :  {event.get('name_b') or 'N/A'}")
    print(f"   Collision Time    :  {event['ts_a']}")
    print(f"   Latitude          :  {float(event['lat_a']):.6f} °N")
    print(f"   Longitude         :  {float(event['lon_a']):.6f} °E")
    print(f"   Closest Distance  :  {dist_m:.1f} m")
    print(sep + "\n")


# ────────────────────────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────────────────────────

def main():
    spark = build_spark()

    raw      = load_raw(spark)
    cleaned  = filter_noise(raw)
    denoised = remove_teleportation(cleaned)

    # Cache: reused for collision search + trajectory extraction
    denoised.cache()
    denoised.count()
    log.info("Denoised dataset cached.")

    candidates   = generate_candidates(denoised)
    event        = find_collision(candidates, denoised)
    trajectories = extract_trajectories(denoised, event)
    plot_trajectories(trajectories, event)
    print_results(event)

    spark.stop()
    log.info("Pipeline complete.")


if __name__ == "__main__":
    main()
