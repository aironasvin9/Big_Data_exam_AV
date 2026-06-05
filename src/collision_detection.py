"""
AIS Collision-Like Trajectory Intersection Detection Pipeline
============================================================

Big Data / PySpark AIS collision-like trajectory intersection detection for
Danish AIS data.

Assignment constraints implemented:
  - Uses PySpark for distributed processing.
  - Restricts processing to December 1–31, 2021.
  - Restricts geographic area to 50 nautical miles around:
        Latitude  = 55.225000
        Longitude = 14.245000
  - Filters invalid MMSI values, invalid coordinates, stationary rows,
    non-vessel transponders, anchored/moored rows, GPS spikes/teleportation,
    tug/push/tow/pilot/rescue-like false positives, harbour/dock/formation
    false positives, physically impossible approach/divergence patterns,
    isolated one-ping GPS anomalies, and normal scheduled ferry/passenger
    near-passes.
  - Final output prints MMSI numbers, vessel names, timestamp, and coordinates.
  - Final visualization saves a ±10 minute trajectory plot for the selected
    collision-like event.

Important implementation details:
  1. MMSI values are kept as strings everywhere.
  2. Normal ship MMSIs are restricted to /^[2-7][0-9]{8}$/.
     This removes false positives such as 111xxxxxx special AIS/SAR ranges.
  3. Validation rejects physically impossible events, e.g.
        31 km -> 0.45 km -> 33 km in ±10 minutes.
  4. Approach/divergence scoring is capped so GPS jumps cannot dominate.
  5. Candidate generation uses time-shifted blocking by default so pairs near
     minute/time-block boundaries are less likely to be missed.
  6. Spatial shifted blocking is optional:
        USE_SHIFTED_SPATIAL_BLOCKING=1
  7. Near-event consistency is required:
        - the ±1 minute near-event average distance must be reasonably close,
        - the raw closest candidate must not be wildly inconsistent with the
          near-event trajectory,
        - the selected collision point must lie close to both vessels' local
          near-event trajectories.
  8. Post-impact anomaly is required by default:
        - rejects normal close-passes where both vessels continue sailing normally.

Notes:
  - Vessel names and ship types printed in the final result are taken directly
    from the AIS fields `name` and `ship_type`; they are not manually assigned.
  - This pipeline detects the strongest AIS-derived collision-like trajectory
    intersection. AIS alone cannot legally prove physical collision.

Recommended run:
    python src/collision_detection.py

If no candidate is found:
    USE_SHIFTED_SPATIAL_BLOCKING=1 CANDIDATE_PAIR_LIMIT=30000 python src/collision_detection.py

If validation is too strict:
    USE_SHIFTED_SPATIAL_BLOCKING=1 \
    CANDIDATE_PAIR_LIMIT=30000 \
    VALID_APPROACH_KM=0.03 \
    VALID_DIVERGE_KM=0.02 \
    MAX_VALIDATION_REL_SPEED_KNOTS=100 \
    MAX_NEAR_EVENT_DISTANCE_KM=1.00 \
    MAX_NEAR_MINUS_CANDIDATE_KM=1.00 \
    MAX_COLLISION_TO_NEAR_CENTROID_KM=1.50 \
    python src/collision_detection.py

If you want to allow passenger/HSC pairs:
    EXCLUDE_PASSENGER_HSC_PAIRS=0 python src/collision_detection.py

If you want to disable post-impact anomaly requirement:
    REQUIRE_POST_IMPACT_ANOMALY=0 python src/collision_detection.py

Optional sanity check:
    RUN_SANITY_CHECK=1 python src/collision_detection.py
"""

import os
import re
import math
import glob
import logging
from datetime import timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    IntegerType,
    StringType,
    BooleanType,
)

import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

import contextily as ctx
from pyproj import Transformer


# ────────────────────────────────────────────────────────────────────
# LOGGING
# ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ────────────────────────────────────────────────────────────────────

INVALID_MMSI_PATTERNS = {
    "000000000", "111111111", "222222222", "333333333", "444444444",
    "555555555", "666666666", "777777777", "123456789", "999999999",
    "012345678", "987654321", "000000001", "888888888",
}

INVALID_MMSI_PREFIXES = ("0000", "1111", "9999")
BASE_STATION_PREFIXES = ("992",)
EXPECTED_MMSI_LENGTH = 9

# Normal ship MMSIs begin with digits 2–7.
# This excludes special/non-vessel AIS ranges:
#   00xxxxxxx  = coast stations
#   111xxxxxx  = SAR aircraft / special AIS use
#   970xxxxxx  = AIS-SART
#   972xxxxxx  = MOB
#   974xxxxxx  = EPIRB-AIS
#   99xxxxxxx  = AtoN / aids to navigation
VALID_SHIP_MMSI_REGEX = r"^[2-7]\d{8}$"

# GPS anomaly thresholds.
TELEPORTATION_SPEED_KNOTS = 60.0
TELEPORTATION_MIN_DISTANCE_KM = 10.0

# More sensitive threshold for local GPS-spike removal.
GPS_SPIKE_MIN_DISTANCE_KM = float(os.getenv("GPS_SPIKE_MIN_DISTANCE_KM", "1.0"))

# Moving-vessel row threshold.
MIN_SOG_KNOTS = float(os.getenv("MIN_SOG_KNOTS", "0.5"))

# Area of interest.
CENTER_LAT = 55.225000
CENTER_LON = 14.245000
RADIUS_NM = 50.0
RADIUS_KM = RADIUS_NM * 1.852

# Spatial bucketing.
GRID_SIZE = float(os.getenv("GRID_SIZE", "0.05"))  # degrees

# Collision proximity threshold.
COLLISION_KM = float(os.getenv("COLLISION_KM", "0.5"))

# Trajectory window: exact assignment requirement, ±10 min.
TRAJ_WINDOW_MIN = 10

# Candidate generation.
PING_TIME_TOLERANCE_SEC = int(os.getenv("PING_TIME_TOLERANCE_SEC", "90"))

# Time blocking:
# Use blocks twice the tolerance with a half-block shift.
# This catches pairs that are close in time but lie on opposite sides of a
# minute/block boundary.
TIME_BLOCK_SEC = int(os.getenv("TIME_BLOCK_SEC", str(PING_TIME_TOLERANCE_SEC * 2)))
USE_SHIFTED_TIME_BLOCKING = os.getenv("USE_SHIFTED_TIME_BLOCKING", "1") == "1"

# Spatial shifted blocking is optional because it duplicates data four times.
# Default OFF for speed; turn ON if needed.
USE_SHIFTED_SPATIAL_BLOCKING = os.getenv("USE_SHIFTED_SPATIAL_BLOCKING", "0") == "1"

# Number of unique vessel-pair candidates to validate.
CANDIDATE_PAIR_LIMIT = int(os.getenv("CANDIDATE_PAIR_LIMIT", "5000"))

# Pair-level validation thresholds.
FORMATION_ALWAYS_CLOSE_KM = float(os.getenv("FORMATION_ALWAYS_CLOSE_KM", "0.15"))
MIN_WINDOW_MOVEMENT_KM = float(os.getenv("MIN_WINDOW_MOVEMENT_KM", "0.05"))
VALID_APPROACH_KM = float(os.getenv("VALID_APPROACH_KM", "0.05"))
VALID_DIVERGE_KM = float(os.getenv("VALID_DIVERGE_KM", "0.03"))
VERY_CLOSE_COLLISION_KM = float(os.getenv("VERY_CLOSE_COLLISION_KM", "0.05"))

# Physical plausibility.
# Reject events where the pair appears to close/separate at impossible relative
# speeds. 80 kn is already generous for ordinary vessels.
MAX_VALIDATION_REL_SPEED_KNOTS = float(os.getenv("MAX_VALIDATION_REL_SPEED_KNOTS", "80.0"))

# The pre phase is -10 to -3 min. Its center is approximately -6.5 min.
# The post phase is +3 to +10 min. Its center is approximately +6.5 min.
VALIDATION_PHASE_CENTER_MIN = (TRAJ_WINDOW_MIN + 3.0) / 2.0

# Minimum context pings required in validation windows.
MIN_TRACK_PINGS_PER_VESSEL = int(os.getenv("MIN_TRACK_PINGS_PER_VESSEL", "3"))
MIN_PRE_PINGS_PER_VESSEL = int(os.getenv("MIN_PRE_PINGS_PER_VESSEL", "1"))
MIN_NEAR_PINGS_PER_VESSEL = int(os.getenv("MIN_NEAR_PINGS_PER_VESSEL", "1"))

# Distance penalty in validation score. Larger value makes closer CPA/collision
# candidates strongly preferred over loose 300–500 m near misses.
DISTANCE_SCORE_PENALTY = float(os.getenv("DISTANCE_SCORE_PENALTY", "25.0"))

# Near-event consistency checks.
# These reject isolated one-ping GPS anomalies where the raw candidate distance
# is tiny, but the surrounding ±1 minute trajectory does not support a collision.
MAX_NEAR_EVENT_DISTANCE_KM = float(os.getenv("MAX_NEAR_EVENT_DISTANCE_KM", "0.75"))

# The near-window pair distance should not be much larger than the raw closest
# ping distance. Example false positive:
#   raw distance  = 0.0165 km
#   near distance = 0.9094 km
# difference ≈ 0.893 km, so this rejects it.
MAX_NEAR_MINUS_CANDIDATE_KM = float(os.getenv("MAX_NEAR_MINUS_CANDIDATE_KM", "0.75"))

# The selected collision point must be close to each vessel's near-event local
# trajectory centroid. If the plotted star is far away from the vessel track,
# this check rejects the event.
MAX_COLLISION_TO_NEAR_CENTROID_KM = float(os.getenv("MAX_COLLISION_TO_NEAR_CENTROID_KM", "1.25"))

# Default ON because scheduled passenger/HSC ferry close-passes dominate the
# Danish AIS data and are usually not collision events. You can turn it off with:
#   EXCLUDE_PASSENGER_HSC_PAIRS=0 python src/collision_detection.py
EXCLUDE_PASSENGER_HSC_PAIRS = os.getenv("EXCLUDE_PASSENGER_HSC_PAIRS", "1") == "1"

# Real collisions normally produce abnormal post-event behaviour:
#   - one vessel stops/slows,
#   - one vessel disappears,
#   - one vessel remains nearby,
#   - or the pair does not simply continue at normal service speed.
#
# This rejects normal ferry/traffic-lane close-passes where both vessels keep
# sailing normally after the closest approach.
REQUIRE_POST_IMPACT_ANOMALY = os.getenv("REQUIRE_POST_IMPACT_ANOMALY", "1") == "1"

# If both vessels have enough post-event pings and both average above this SOG,
# they are considered to have continued normally.
POST_CONTINUE_SOG_KNOTS = float(os.getenv("POST_CONTINUE_SOG_KNOTS", "5.0"))

# Minimum post-event pings required to say a vessel clearly continued normally.
POST_CONTINUE_MIN_PINGS = int(os.getenv("POST_CONTINUE_MIN_PINGS", "3"))

# Tug/push/tow/pilot/rescue/assistance regex.
# Important: we intentionally do NOT include "law enforcement", "KBV",
# "coast guard", etc. in this regex, because those are AIS-reported vessel
# types/names and we are allowing them as valid collision-like trajectory
# candidates in the final configuration.
TUG_REGEX = (
    r"(tug|tow|towage|towing|pusher|pushed|push|"
    r"slæb|slaeb|slæbe|bugser|bugsering|"
    r"pilot|lods|assistance|assist|rescue|sar)"
)

# Optional sanity check for known December 2021 Bornholm-area names.
RUN_SANITY_CHECK = os.getenv("RUN_SANITY_CHECK", "0") == "1"

# Paths.
DATA_DIR = os.getenv("DATA_DIR", "/app/data")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/app/output")


# ────────────────────────────────────────────────────────────────────
# GEO HELPERS
# ────────────────────────────────────────────────────────────────────

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Great-circle distance in kilometres between two WGS-84 points.
    """
    radius_km = 6371.0

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )

    return radius_km * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def haversine_col(lat1, lon1, lat2, lon2):
    """
    Spark SQL expression for Haversine distance in kilometres.

    Arguments can be Spark Columns or Spark literals.
    """
    return (
        F.lit(6371.0) * F.lit(2.0) * F.asin(F.sqrt(
            F.pow(F.sin((F.radians(lat2) - F.radians(lat1)) / F.lit(2.0)), 2)
            + F.cos(F.radians(lat1))
            * F.cos(F.radians(lat2))
            * F.pow(F.sin((F.radians(lon2) - F.radians(lon1)) / F.lit(2.0)), 2)
        ))
    )


def is_valid_coordinate(lat: float, lon: float) -> bool:
    """
    Validate latitude and longitude values.
    """
    if not (-90 <= lat <= 90):
        return False
    if not (-180 <= lon <= 180):
        return False
    if lat == 0.0 and lon == 0.0:
        return False
    return True


def dist_to_centre(lat: float, lon: float) -> float:
    """
    Distance in km from point to area-of-interest centre.
    """
    return haversine_distance(lat, lon, CENTER_LAT, CENTER_LON)


def clean_event_name(value):
    """
    Clean vessel names coming from Spark/Pandas.
    """
    if value is None:
        return "N/A"

    s = str(value).strip()

    if not s or s.lower() in {"nan", "none", "null"}:
        return "N/A"

    return s


# ────────────────────────────────────────────────────────────────────
# MMSI VALIDATION
# ────────────────────────────────────────────────────────────────────

def is_valid_mmsi(mmsi: str) -> bool:
    """
    Validate MMSI against common dirty AIS patterns and normal ship range.
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

    if mmsi.startswith(BASE_STATION_PREFIXES):
        return False

    if len(set(mmsi)) == 1:
        return False

    if not re.match(VALID_SHIP_MMSI_REGEX, mmsi):
        return False

    return True


# UDF placeholders.
_haversine_udf = None
_dist_centre_udf = None
_valid_mmsi_udf = None
_valid_coord_udf = None


# ────────────────────────────────────────────────────────────────────
# SPARK SESSION
# ────────────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    """
    Build Spark session.

    UTC is set explicitly to make timestamp handling reproducible inside Docker
    and outside Docker.
    """
    global _haversine_udf, _dist_centre_udf, _valid_mmsi_udf, _valid_coord_udf

    spark = (
        SparkSession.builder
        .appName("AIS-Collision-Detection")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.autoBroadcastJoinThreshold", "50mb")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    _haversine_udf = F.udf(haversine_distance, DoubleType())
    _dist_centre_udf = F.udf(dist_to_centre, DoubleType())
    _valid_mmsi_udf = F.udf(is_valid_mmsi, BooleanType())
    _valid_coord_udf = F.udf(is_valid_coordinate, BooleanType())

    log.info("Spark session ready (version %s)", spark.version)

    return spark


# ────────────────────────────────────────────────────────────────────
# STAGE 1 — DATA LOADING
# ────────────────────────────────────────────────────────────────────

def load_raw(spark: SparkSession):
    """
    Load all CSV files from DATA_DIR.
    """
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))

    log.info("CSV files found in %s: %d", DATA_DIR, len(files))

    if len(files) > 0:
        for f in files[:5]:
            log.info("  sample file: %s", f)

    path = os.path.join(DATA_DIR, "*.csv")
    log.info("Loading AIS data from %s", path)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(path)
    )

    # Normalize column names.
    for col in df.columns:
        clean = (
            col.strip()
            .lower()
            .replace(" ", "_")
            .replace(".", "_")
            .replace("-", "_")
            .lstrip("#")
            .strip("_")
        )

        if clean != col:
            df = df.withColumnRenamed(col, clean)

    log.info("Columns after normalization: %s", ", ".join(df.columns))
    log.info("Raw rows loaded: {:,}".format(df.count()))

    return df


# ────────────────────────────────────────────────────────────────────
# STAGE 2 — BASIC CLEANING AND AREA/TIME FILTERING
# ────────────────────────────────────────────────────────────────────

def filter_noise(df):
    """
    Basic AIS cleaning and assignment filters.

    Removes:
      - malformed rows,
      - invalid/special MMSI values,
      - invalid coordinates,
      - base stations / AtoN / non-vessel transponders,
      - anchored/moored rows,
      - low-SOG stationary rows,
      - rows outside December 2021,
      - rows outside the 50 nm AOI.

    Additional pair-level rejection is done later in find_collision().
    """

    required = ["timestamp", "mmsi", "latitude", "longitude"]
    missing = [c for c in required if c not in df.columns]

    if missing:
        raise RuntimeError(
            "Missing required columns after normalization: "
            + ", ".join(missing)
            + ". Actual columns: "
            + ", ".join(df.columns)
        )

    df = (
        df
        .withColumn("mmsi", F.trim(F.col("mmsi").cast(StringType())))
        .withColumn("lat", F.col("latitude").cast(DoubleType()))
        .withColumn("lon", F.col("longitude").cast(DoubleType()))
        .withColumn(
            "sog",
            F.coalesce(F.col("sog").cast(DoubleType()), F.lit(0.0))
            if "sog" in df.columns
            else F.lit(0.0)
        )
        .withColumn("ts", F.to_timestamp(F.col("timestamp"), "dd/MM/yyyy HH:mm:ss"))
    )

    # Category 3 — malformed rows.
    before = df.count()

    df = (
        df
        .dropna(subset=["lat", "lon", "ts"])
        .filter(F.col("mmsi").isNotNull())
    )

    log.info("CAT-3 malformed rows removed: {:,}".format(before - df.count()))

    # Category 1 — invalid/special MMSI.
    invalid_patterns_list = list(INVALID_MMSI_PATTERNS)

    before = df.count()

    df = df.filter(
        F.col("mmsi").rlike(VALID_SHIP_MMSI_REGEX)
        & ~F.col("mmsi").isin(invalid_patterns_list)
        & ~F.col("mmsi").startswith("992")
        & ~F.col("mmsi").rlike(r"^0000")
        & ~F.col("mmsi").rlike(r"^1111")
        & ~F.col("mmsi").rlike(r"^9999")
        & ~F.col("mmsi").rlike(r"^(\d)\1{8}$")
    )

    log.info("CAT-1 invalid/special MMSI removed: {:,}".format(before - df.count()))

    # Category 2 — invalid coordinates.
    before = df.count()

    df = df.filter(
        F.col("lat").between(-90.0, 90.0)
        & F.col("lon").between(-180.0, 180.0)
        & ~((F.abs(F.col("lat")) < 0.001) & (F.abs(F.col("lon")) < 0.001))
    )

    log.info("CAT-2 invalid coordinates removed: {:,}".format(before - df.count()))

    # Category 4 — non-vessel transponders and anchored/moored.
    before = df.count()

    if "type_of_mobile" in df.columns:
        mobile_lower = F.lower(F.trim(F.col("type_of_mobile")))

        df = df.filter(
            F.col("type_of_mobile").isNull()
            | ~mobile_lower.isin(
                "base_station",
                "base station",
                "aton",
                "aid_to_navigation",
                "aid to navigation",
            )
        )

    if "navigational_status" in df.columns:
        nav_lower = F.lower(F.trim(F.col("navigational_status")))

        df = df.filter(
            F.col("navigational_status").isNull()
            | ~nav_lower.isin("at anchor", "moored")
        )

    log.info("CAT-4 non-vessel/anchored/moored removed: {:,}".format(before - df.count()))

    # Category 5 — stationary rows by SOG.
    # Pair-level stationary/dock rejection is still done later.
    before = df.count()

    df = df.filter(F.col("sog") >= F.lit(MIN_SOG_KNOTS))

    log.info("CAT-5 low-SOG stationary rows removed: {:,}".format(before - df.count()))

    # Category 6 — temporal and geographic filter.
    before = df.count()

    df = df.filter(
        (F.col("ts") >= F.lit("2021-12-01").cast("timestamp"))
        & (F.col("ts") < F.lit("2022-01-01").cast("timestamp"))
    )

    # Cheap bounding-box pre-filter.
    lat_margin = RADIUS_KM / 111.0
    lon_margin = RADIUS_KM / (111.0 * math.cos(math.radians(CENTER_LAT)))

    df = df.filter(
        F.col("lat").between(
            CENTER_LAT - lat_margin * 1.1,
            CENTER_LAT + lat_margin * 1.1,
        )
        & F.col("lon").between(
            CENTER_LON - lon_margin * 1.1,
            CENTER_LON + lon_margin * 1.1,
        )
    )

    # Exact Haversine radius.
    df = df.withColumn(
        "dist_centre_km",
        haversine_col(
            F.col("lat"),
            F.col("lon"),
            F.lit(CENTER_LAT),
            F.lit(CENTER_LON),
        )
    )

    df = df.filter(F.col("dist_centre_km") <= F.lit(RADIUS_KM))

    log.info("CAT-6 out-of-area/time removed: {:,}".format(before - df.count()))

    keep = ["mmsi", "ts", "lat", "lon", "sog"]

    for optional_col in ["name", "ship_type"]:
        if optional_col in df.columns:
            keep.append(optional_col)

    df = df.select([F.col(c) for c in keep])

    log.info("After all noise filters: {:,} rows remain".format(df.count()))

    return df


# ────────────────────────────────────────────────────────────────────
# OPTIONAL SANITY CHECK
# ────────────────────────────────────────────────────────────────────

def sanity_check_known_incident(df):
    """
    Optional diagnostic only. Does not hard-code any answer.

    Usage:
        RUN_SANITY_CHECK=1 python src/collision_detection.py
    """
    if "name" not in df.columns:
        log.warning("No name column available for sanity check.")
        return

    log.info("Running optional sanity check for names matching scot/karin/hoj...")

    (
        df
        .filter(F.lower(F.col("name")).rlike("scot|karin|høj|hoej|hoj"))
        .groupBy("mmsi", "name")
        .agg(
            F.count("*").alias("n"),
            F.min("ts").alias("first_ts"),
            F.max("ts").alias("last_ts"),
            F.min("lat").alias("min_lat"),
            F.max("lat").alias("max_lat"),
            F.min("lon").alias("min_lon"),
            F.max("lon").alias("max_lon"),
        )
        .orderBy(F.desc("n"))
        .show(100, truncate=False)
    )


# ────────────────────────────────────────────────────────────────────
# STAGE 3 — GPS TELEPORTATION / SPIKE REMOVAL
# ────────────────────────────────────────────────────────────────────

def remove_teleportation(df):
    """
    Remove GPS teleportation/spike anomalies using a two-sided check.

    This avoids the common one-sided issue:

        valid point -> bad GPS spike -> valid point

    A one-sided algorithm may delete the spike and then incorrectly treat the
    following valid point as another impossible jump. This version identifies
    isolated middle spikes using previous/current/next geometry.
    """

    def knots_expr(dist_col, sec_col):
        return (
            F.when(
                sec_col > F.lit(0),
                (dist_col / (sec_col / F.lit(3600.0))) / F.lit(1.852)
            )
            .otherwise(F.lit(0.0))
        )

    w = Window.partitionBy("mmsi").orderBy("ts")

    df2 = (
        df
        .withColumn("prev_lat", F.lag("lat").over(w))
        .withColumn("prev_lon", F.lag("lon").over(w))
        .withColumn("prev_ts", F.lag("ts").over(w))
        .withColumn("next_lat", F.lead("lat").over(w))
        .withColumn("next_lon", F.lead("lon").over(w))
        .withColumn("next_ts", F.lead("ts").over(w))
        .withColumn("cur_sec", F.col("ts").cast(LongType()))
        .withColumn("prev_sec", F.col("prev_ts").cast(LongType()))
        .withColumn("next_sec", F.col("next_ts").cast(LongType()))
    )

    df2 = (
        df2
        .withColumn(
            "dist_prev_km",
            F.when(
                F.col("prev_lat").isNotNull(),
                haversine_col(
                    F.col("prev_lat"),
                    F.col("prev_lon"),
                    F.col("lat"),
                    F.col("lon"),
                )
            )
            .otherwise(F.lit(0.0))
        )
        .withColumn(
            "dist_next_km",
            F.when(
                F.col("next_lat").isNotNull(),
                haversine_col(
                    F.col("lat"),
                    F.col("lon"),
                    F.col("next_lat"),
                    F.col("next_lon"),
                )
            )
            .otherwise(F.lit(0.0))
        )
        .withColumn(
            "dist_prev_next_km",
            F.when(
                F.col("prev_lat").isNotNull() & F.col("next_lat").isNotNull(),
                haversine_col(
                    F.col("prev_lat"),
                    F.col("prev_lon"),
                    F.col("next_lat"),
                    F.col("next_lon"),
                )
            )
            .otherwise(F.lit(0.0))
        )
        .withColumn("sec_prev", F.col("cur_sec") - F.col("prev_sec"))
        .withColumn("sec_next", F.col("next_sec") - F.col("cur_sec"))
        .withColumn("sec_prev_next", F.col("next_sec") - F.col("prev_sec"))
    )

    df2 = (
        df2
        .withColumn("knots_prev", knots_expr(F.col("dist_prev_km"), F.col("sec_prev")))
        .withColumn("knots_next", knots_expr(F.col("dist_next_km"), F.col("sec_next")))
        .withColumn(
            "knots_prev_next",
            knots_expr(F.col("dist_prev_next_km"), F.col("sec_prev_next"))
        )
    )

    bad_from_prev = (
        F.col("prev_lat").isNotNull()
        & (F.col("knots_prev") > F.lit(TELEPORTATION_SPEED_KNOTS))
        & (F.col("dist_prev_km") > F.lit(GPS_SPIKE_MIN_DISTANCE_KM))
    )

    bad_to_next = (
        F.col("next_lat").isNotNull()
        & (F.col("knots_next") > F.lit(TELEPORTATION_SPEED_KNOTS))
        & (F.col("dist_next_km") > F.lit(GPS_SPIKE_MIN_DISTANCE_KM))
    )

    prev_to_next_plausible = (
        (F.col("knots_prev_next") <= F.lit(TELEPORTATION_SPEED_KNOTS))
        | (F.col("dist_prev_next_km") <= F.lit(GPS_SPIKE_MIN_DISTANCE_KM))
    )

    isolated_spike = (
        F.col("prev_lat").isNotNull()
        & F.col("next_lat").isNotNull()
        & bad_from_prev
        & bad_to_next
        & prev_to_next_plausible
    )

    terminal_spike = (
        (bad_from_prev & F.col("next_lat").isNull())
        | (bad_to_next & F.col("prev_lat").isNull())
    )

    before = df2.count()

    df2 = df2.filter(~(isolated_spike | terminal_spike))

    removed = before - df2.count()

    log.info("Teleportation/GPS spikes removed: {:,}".format(removed))

    df2 = df2.drop(
        "prev_lat", "prev_lon", "prev_ts",
        "next_lat", "next_lon", "next_ts",
        "cur_sec", "prev_sec", "next_sec",
        "dist_prev_km", "dist_next_km", "dist_prev_next_km",
        "sec_prev", "sec_next", "sec_prev_next",
        "knots_prev", "knots_next", "knots_prev_next",
    )

    return df2


# ────────────────────────────────────────────────────────────────────
# STAGE 4 — CANDIDATE GENERATION
# ────────────────────────────────────────────────────────────────────

def generate_candidates(df):
    """
    Generate close vessel-pair candidates without a full Cartesian product.

    Blocking strategy:
      - time bucket with optional half-bucket shift,
      - spatial grid cell with optional half-cell shifts,
      - self-join only within matching blocks,
      - mmsi_a < mmsi_b,
      - exact Haversine distance <= COLLISION_KM,
      - exact ping time difference <= PING_TIME_TOLERANCE_SEC.

    We do NOT remove zero/sub-metre distances. AIS coordinate rounding can make
    a real collision appear as zero distance.
    """

    base = df.select(
        F.col("mmsi").cast(StringType()).alias("mmsi"),
        F.col("ts"),
        F.col("lat"),
        F.col("lon"),
        F.col("sog"),
        (
            F.col("name").cast(StringType()).alias("name")
            if "name" in df.columns
            else F.lit(None).cast(StringType()).alias("name")
        ),
        (
            F.col("ship_type").cast(StringType()).alias("ship_type")
            if "ship_type" in df.columns
            else F.lit(None).cast(StringType()).alias("ship_type")
        ),
    )

    base = base.withColumn("t_sec", F.col("ts").cast(LongType()))

    if USE_SHIFTED_SPATIAL_BLOCKING:
        log.info("Using shifted spatial blocking: ON")

        spatial_shifts = F.array(
            F.struct(F.lit(0.0).alias("sx"), F.lit(0.0).alias("sy"), F.lit("s00").alias("sid")),
            F.struct(F.lit(0.5).alias("sx"), F.lit(0.0).alias("sy"), F.lit("s10").alias("sid")),
            F.struct(F.lit(0.0).alias("sx"), F.lit(0.5).alias("sy"), F.lit("s01").alias("sid")),
            F.struct(F.lit(0.5).alias("sx"), F.lit(0.5).alias("sy"), F.lit("s11").alias("sid")),
        )
    else:
        log.info("Using shifted spatial blocking: OFF")

        spatial_shifts = F.array(
            F.struct(F.lit(0.0).alias("sx"), F.lit(0.0).alias("sy"), F.lit("s00").alias("sid"))
        )

    if USE_SHIFTED_TIME_BLOCKING:
        log.info(
            "Using shifted time blocking: ON "
            "(TIME_BLOCK_SEC=%d, tolerance=%d sec)",
            TIME_BLOCK_SEC,
            PING_TIME_TOLERANCE_SEC,
        )

        time_shifts = F.array(
            F.struct(F.lit(0).alias("offset"), F.lit("t0").alias("tid")),
            F.struct(F.lit(int(TIME_BLOCK_SEC / 2)).alias("offset"), F.lit("t1").alias("tid")),
        )
    else:
        log.info(
            "Using shifted time blocking: OFF "
            "(TIME_BLOCK_SEC=%d, tolerance=%d sec)",
            TIME_BLOCK_SEC,
            PING_TIME_TOLERANCE_SEC,
        )

        time_shifts = F.array(
            F.struct(F.lit(0).alias("offset"), F.lit("t0").alias("tid"))
        )

    blocked = (
        base
        .withColumn("spatial_shift", F.explode(spatial_shifts))
        .withColumn("time_shift", F.explode(time_shifts))
        .withColumn(
            "time_bucket",
            F.floor(
                (F.col("t_sec") + F.col("time_shift.offset")) / F.lit(TIME_BLOCK_SEC)
            ).cast(LongType())
        )
        .withColumn(
            "grid_x",
            F.floor(
                (F.col("lon") / F.lit(GRID_SIZE)) + F.col("spatial_shift.sx")
            ).cast(IntegerType())
        )
        .withColumn(
            "grid_y",
            F.floor(
                (F.col("lat") / F.lit(GRID_SIZE)) + F.col("spatial_shift.sy")
            ).cast(IntegerType())
        )
        .withColumn("spatial_shift_id", F.col("spatial_shift.sid"))
        .withColumn("time_shift_id", F.col("time_shift.tid"))
        .drop("spatial_shift", "time_shift")
        .repartition("time_bucket", "grid_x", "grid_y")
    )

    a = blocked.alias("a")
    b = blocked.alias("b")

    joined = (
        a.join(
            b,
            on=(
                (F.col("a.time_bucket") == F.col("b.time_bucket"))
                & (F.col("a.grid_x") == F.col("b.grid_x"))
                & (F.col("a.grid_y") == F.col("b.grid_y"))
                & (F.col("a.spatial_shift_id") == F.col("b.spatial_shift_id"))
                & (F.col("a.time_shift_id") == F.col("b.time_shift_id"))
                & (F.col("a.mmsi") < F.col("b.mmsi"))
            ),
            how="inner",
        )
        .withColumn("time_diff_sec", F.abs(F.col("a.t_sec") - F.col("b.t_sec")))
        .filter(F.col("time_diff_sec") <= F.lit(PING_TIME_TOLERANCE_SEC))
        .select(
            F.col("a.mmsi").alias("mmsi_a"),
            F.col("a.ts").alias("ts_a"),
            F.col("a.t_sec").alias("t_sec_a"),
            F.col("a.lat").alias("lat_a"),
            F.col("a.lon").alias("lon_a"),
            F.col("a.sog").alias("sog_a"),
            F.col("a.name").alias("name_a"),
            F.col("a.ship_type").alias("ship_type_a"),

            F.col("b.mmsi").alias("mmsi_b"),
            F.col("b.ts").alias("ts_b"),
            F.col("b.t_sec").alias("t_sec_b"),
            F.col("b.lat").alias("lat_b"),
            F.col("b.lon").alias("lon_b"),
            F.col("b.sog").alias("sog_b"),
            F.col("b.name").alias("name_b"),
            F.col("b.ship_type").alias("ship_type_b"),

            F.col("time_diff_sec"),
        )
        .dropDuplicates(["mmsi_a", "mmsi_b", "ts_a", "ts_b"])
    )

    candidates = (
        joined
        .withColumn(
            "dist_km",
            haversine_col(
                F.col("lat_a"),
                F.col("lon_a"),
                F.col("lat_b"),
                F.col("lon_b"),
            )
        )
        .filter(F.col("dist_km") <= F.lit(COLLISION_KM))
        .withColumn(
            "collision_t_sec",
            F.round((F.col("t_sec_a") + F.col("t_sec_b")) / F.lit(2.0)).cast(LongType())
        )
        .withColumn(
            "collision_ts",
            F.to_timestamp(F.from_unixtime(F.col("collision_t_sec")))
        )
        .withColumn("collision_lat", (F.col("lat_a") + F.col("lat_b")) / F.lit(2.0))
        .withColumn("collision_lon", (F.col("lon_a") + F.col("lon_b")) / F.lit(2.0))
    )

    n = candidates.count()

    log.info(
        "Collision candidates (dist ≤ %.3f km, time diff ≤ %d sec): %s pairs",
        COLLISION_KM,
        PING_TIME_TOLERANCE_SEC,
        f"{n:,}",
    )

    return candidates


# ────────────────────────────────────────────────────────────────────
# STAGE 5 — COLLISION VALIDATION
# ────────────────────────────────────────────────────────────────────

def find_collision(candidates, denoised_df):
    """
    Select the most plausible collision-like trajectory intersection from
    close-pair candidates.

    Rejects:
      - non-ship/special MMSI candidates,
      - passenger/HSC/ferry scheduled close-passes by default,
      - tug/push/tow/pilot/rescue-like pairs,
      - dock/harbour/formation pairs that remain close,
      - stationary pairs,
      - physically impossible approach/divergence caused by GPS jumps,
      - isolated one-ping GPS anomalies where the raw candidate distance is tiny
        but the ±1 minute near-event trajectory is not actually close,
      - normal close-passes where both vessels continue sailing normally after.
    """

    if candidates is None:
        raise RuntimeError("Candidate DataFrame is None.")

    if candidates.rdd.isEmpty():
        raise RuntimeError("No collision candidates found.")

    cand = (
        candidates
        .withColumn("mmsi_a", F.col("mmsi_a").cast(StringType()))
        .withColumn("mmsi_b", F.col("mmsi_b").cast(StringType()))
        .withColumn("dist_km", F.col("dist_km").cast(DoubleType()))
    )

    # Safety check: enforce normal ship MMSI at candidate level too.
    cand = cand.filter(
        F.col("mmsi_a").rlike(VALID_SHIP_MMSI_REGEX)
        & F.col("mmsi_b").rlike(VALID_SHIP_MMSI_REGEX)
    )

    if cand.rdd.isEmpty():
        raise RuntimeError(
            "No candidates remain after strict ship-MMSI filtering. "
            "Check VALID_SHIP_MMSI_REGEX or input data."
        )

    for col_name in ["name_a", "name_b", "ship_type_a", "ship_type_b"]:
        if col_name not in cand.columns:
            cand = cand.withColumn(col_name, F.lit(None).cast(StringType()))

    if "time_diff_sec" not in cand.columns:
        cand = cand.withColumn("time_diff_sec", F.lit(0).cast(LongType()))

    if "collision_ts" not in cand.columns:
        cand = cand.withColumn("collision_ts", F.col("ts_a"))

    if "collision_lat" not in cand.columns:
        cand = cand.withColumn(
            "collision_lat",
            (F.col("lat_a") + F.col("lat_b")) / F.lit(2.0)
        )

    if "collision_lon" not in cand.columns:
        cand = cand.withColumn(
            "collision_lon",
            (F.col("lon_a") + F.col("lon_b")) / F.lit(2.0)
        )

    # Optional filter for scheduled high-speed/passenger ferry near-passes.
    # Default ON.
    if EXCLUDE_PASSENGER_HSC_PAIRS:
        ship_text = F.lower(F.concat_ws(
            " ",
            F.coalesce(F.col("ship_type_a"), F.lit("")),
            F.coalesce(F.col("ship_type_b"), F.lit("")),
            F.coalesce(F.col("name_a"), F.lit("")),
            F.coalesce(F.col("name_b"), F.lit("")),
        ))

        cand = cand.filter(
            ~(
                ship_text.rlike(r"\bhsc\b")
                | ship_text.rlike(r"passenger")
                | ship_text.rlike(r"ferry")
            )
        )

        log.warning("Passenger/HSC/ferry pair exclusion is ENABLED.")

    if cand.rdd.isEmpty():
        raise RuntimeError(
            "No candidates remain after passenger/HSC/ferry exclusion. "
            "If this is too strict, run with EXCLUDE_PASSENGER_HSC_PAIRS=0."
        )

    # One closest event per vessel pair.
    pair_w = Window.partitionBy("mmsi_a", "mmsi_b").orderBy(
        F.col("dist_km").asc(),
        F.col("time_diff_sec").asc(),
        F.col("collision_ts").asc(),
    )

    pair_best = (
        cand
        .withColumn("pair_rn", F.row_number().over(pair_w))
        .filter(F.col("pair_rn") == 1)
        .drop("pair_rn")
    )

    limited = pair_best.orderBy(
        F.col("dist_km").asc(),
        F.col("time_diff_sec").asc(),
    ).limit(CANDIDATE_PAIR_LIMIT)

    # Generated event id avoids global row_number Window warnings.
    events = (
        limited
        .withColumn("event_id", F.monotonically_increasing_id())
        .withColumn("event_sec", F.col("collision_ts").cast(LongType()))
    )

    tug_text = F.lower(F.concat_ws(
        " ",
        F.coalesce(F.col("name_a"), F.lit("")),
        F.coalesce(F.col("name_b"), F.lit("")),
        F.coalesce(F.col("ship_type_a"), F.lit("")),
        F.coalesce(F.col("ship_type_b"), F.lit("")),
    ))

    events = (
        events
        .withColumn("tug_text", tug_text)
        .withColumn("is_tug_like", F.col("tug_text").rlike(TUG_REGEX))
        .persist()
    )

    n_events = events.count()

    log.info("Validating top {:,} unique vessel-pair candidate events".format(n_events))

    if n_events == 0:
        raise RuntimeError("No unique vessel-pair events available for validation.")

    pings = (
        denoised_df
        .select(
            F.col("mmsi").cast(StringType()).alias("mmsi"),
            F.col("ts"),
            F.col("lat"),
            F.col("lon"),
            F.col("sog"),
        )
        .filter(F.col("mmsi").rlike(VALID_SHIP_MMSI_REGEX))
        .withColumn("ping_sec", F.col("ts").cast(LongType()))
    )

    event_a = events.select(
        "event_id",
        "event_sec",
        F.col("mmsi_a").alias("mmsi"),
        F.lit("a").alias("side"),
    )

    event_b = events.select(
        "event_id",
        "event_sec",
        F.col("mmsi_b").alias("mmsi"),
        F.lit("b").alias("side"),
    )

    event_long = event_a.unionByName(event_b)

    joined = (
        F.broadcast(event_long)
        .join(pings, on="mmsi", how="inner")
        .withColumn("dt_sec", F.col("ping_sec") - F.col("event_sec"))
        .filter(F.abs(F.col("dt_sec")) <= F.lit(TRAJ_WINDOW_MIN * 60))
    )

    # Phase windows:
    #   pre  = 10 to 3 minutes before event
    #   near = ±1 minute around event
    #   post = 3 to 10 minutes after event
    phase_joined = (
        joined
        .withColumn(
            "phase",
            F.when(
                (F.col("dt_sec") >= F.lit(-TRAJ_WINDOW_MIN * 60))
                & (F.col("dt_sec") <= F.lit(-180)),
                F.lit("pre"),
            )
            .when(
                F.abs(F.col("dt_sec")) <= F.lit(60),
                F.lit("near"),
            )
            .when(
                (F.col("dt_sec") >= F.lit(180))
                & (F.col("dt_sec") <= F.lit(TRAJ_WINDOW_MIN * 60)),
                F.lit("post"),
            )
        )
    )

    phase_agg = (
        phase_joined
        .filter(F.col("phase").isNotNull())
        .groupBy("event_id", "side", "phase")
        .agg(
            F.avg("lat").alias("avg_lat"),
            F.avg("lon").alias("avg_lon"),
            F.avg("sog").alias("avg_sog"),
            F.count("*").alias("n_pings"),
        )
    )

    def pick_phase(side, phase, prefix):
        return (
            phase_agg
            .filter((F.col("side") == side) & (F.col("phase") == phase))
            .select(
                F.col("event_id"),
                F.col("avg_lat").alias(f"{prefix}_lat"),
                F.col("avg_lon").alias(f"{prefix}_lon"),
                F.col("avg_sog").alias(f"{prefix}_sog"),
                F.col("n_pings").alias(f"{prefix}_n"),
            )
        )

    a_pre = pick_phase("a", "pre", "a_pre")
    b_pre = pick_phase("b", "pre", "b_pre")
    a_near = pick_phase("a", "near", "a_near")
    b_near = pick_phase("b", "near", "b_near")
    a_post = pick_phase("a", "post", "a_post")
    b_post = pick_phase("b", "post", "b_post")

    # Full-window movement span for stationary/dock-like pair rejection.
    movement_agg = (
        joined
        .groupBy("event_id", "side")
        .agg(
            F.min("lat").alias("min_lat"),
            F.max("lat").alias("max_lat"),
            F.min("lon").alias("min_lon"),
            F.max("lon").alias("max_lon"),
            F.avg("sog").alias("avg_sog"),
            F.count("*").alias("n_pings"),
        )
        .withColumn(
            "span_km",
            haversine_col(
                F.col("min_lat"),
                F.col("min_lon"),
                F.col("max_lat"),
                F.col("max_lon"),
            )
        )
    )

    def pick_movement(side, prefix):
        return (
            movement_agg
            .filter(F.col("side") == side)
            .select(
                F.col("event_id"),
                F.col("span_km").alias(f"{prefix}_span_km"),
                F.col("avg_sog").alias(f"{prefix}_avg_sog"),
                F.col("n_pings").alias(f"{prefix}_n"),
            )
        )

    a_mov = pick_movement("a", "a_win")
    b_mov = pick_movement("b", "b_win")

    scored = events

    for extra in [a_pre, b_pre, a_near, b_near, a_post, b_post, a_mov, b_mov]:
        scored = scored.join(extra, on="event_id", how="left")

    scored = (
        scored
        .withColumn(
            "pre_dist_km",
            haversine_col(
                F.col("a_pre_lat"),
                F.col("a_pre_lon"),
                F.col("b_pre_lat"),
                F.col("b_pre_lon"),
            )
        )
        .withColumn(
            "near_dist_km",
            haversine_col(
                F.col("a_near_lat"),
                F.col("a_near_lon"),
                F.col("b_near_lat"),
                F.col("b_near_lon"),
            )
        )
        .withColumn(
            "post_dist_km",
            haversine_col(
                F.col("a_post_lat"),
                F.col("a_post_lon"),
                F.col("b_post_lat"),
                F.col("b_post_lon"),
            )
        )
        .withColumn(
            "a_collision_to_near_km",
            haversine_col(
                F.col("collision_lat"),
                F.col("collision_lon"),
                F.col("a_near_lat"),
                F.col("a_near_lon"),
            )
        )
        .withColumn(
            "b_collision_to_near_km",
            haversine_col(
                F.col("collision_lat"),
                F.col("collision_lon"),
                F.col("b_near_lat"),
                F.col("b_near_lon"),
            )
        )
        .withColumn("approach_km", F.col("pre_dist_km") - F.col("dist_km"))
        .withColumn("diverge_km", F.col("post_dist_km") - F.col("dist_km"))
    )

    a_moving = (
        (F.coalesce(F.col("a_win_span_km"), F.lit(0.0)) >= F.lit(MIN_WINDOW_MOVEMENT_KM))
        | (F.coalesce(F.col("a_win_avg_sog"), F.lit(0.0)) >= F.lit(MIN_SOG_KNOTS))
    )

    b_moving = (
        (F.coalesce(F.col("b_win_span_km"), F.lit(0.0)) >= F.lit(MIN_WINDOW_MOVEMENT_KM))
        | (F.coalesce(F.col("b_win_avg_sog"), F.lit(0.0)) >= F.lit(MIN_SOG_KNOTS))
    )

    both_moving = a_moving & b_moving

    always_close = (
        (F.coalesce(F.col("pre_dist_km"), F.lit(999.0)) < F.lit(FORMATION_ALWAYS_CLOSE_KM))
        & (F.col("dist_km") < F.lit(FORMATION_ALWAYS_CLOSE_KM))
        & (F.coalesce(F.col("post_dist_km"), F.lit(999.0)) < F.lit(FORMATION_ALWAYS_CLOSE_KM))
    )

    stationary_pair = ~both_moving

    has_pre_evidence = F.col("pre_dist_km").isNotNull()
    has_near_evidence = F.col("near_dist_km").isNotNull()

    enough_track_context = (
        (F.coalesce(F.col("a_win_n"), F.lit(0)) >= F.lit(MIN_TRACK_PINGS_PER_VESSEL))
        & (F.coalesce(F.col("b_win_n"), F.lit(0)) >= F.lit(MIN_TRACK_PINGS_PER_VESSEL))
        & (F.coalesce(F.col("a_pre_n"), F.lit(0)) >= F.lit(MIN_PRE_PINGS_PER_VESSEL))
        & (F.coalesce(F.col("b_pre_n"), F.lit(0)) >= F.lit(MIN_PRE_PINGS_PER_VESSEL))
        & (F.coalesce(F.col("a_near_n"), F.lit(0)) >= F.lit(MIN_NEAR_PINGS_PER_VESSEL))
        & (F.coalesce(F.col("b_near_n"), F.lit(0)) >= F.lit(MIN_NEAR_PINGS_PER_VESSEL))
    )

    has_approach = (
        F.col("approach_km").isNotNull()
        & (F.col("approach_km") >= F.lit(VALID_APPROACH_KM))
    )

    has_divergence = (
        F.col("post_dist_km").isNull()
        | (
            F.col("diverge_km").isNotNull()
            & (F.col("diverge_km") >= F.lit(VALID_DIVERGE_KM))
        )
    )

    very_close = F.col("dist_km") <= F.lit(VERY_CLOSE_COLLISION_KM)

    # Physical plausibility:
    # Convert approach/divergence distance over approximately 6.5 minutes to knots.
    scored = (
        scored
        .withColumn(
            "approach_positive_km",
            F.greatest(F.coalesce(F.col("approach_km"), F.lit(0.0)), F.lit(0.0))
        )
        .withColumn(
            "diverge_positive_km",
            F.greatest(F.coalesce(F.col("diverge_km"), F.lit(0.0)), F.lit(0.0))
        )
        .withColumn(
            "approach_rate_knots",
            (
                F.col("approach_positive_km")
                / F.lit(VALIDATION_PHASE_CENTER_MIN / 60.0)
            )
            / F.lit(1.852)
        )
        .withColumn(
            "diverge_rate_knots",
            (
                F.col("diverge_positive_km")
                / F.lit(VALIDATION_PHASE_CENTER_MIN / 60.0)
            )
            / F.lit(1.852)
        )
    )

    plausible_approach_speed = (
        F.col("approach_rate_knots") <= F.lit(MAX_VALIDATION_REL_SPEED_KNOTS)
    )

    plausible_diverge_speed = (
        F.col("post_dist_km").isNull()
        | (F.col("diverge_rate_knots") <= F.lit(MAX_VALIDATION_REL_SPEED_KNOTS))
    )

    plausible_relative_motion = plausible_approach_speed & plausible_diverge_speed

    # Critical near-event consistency checks:
    # A single raw candidate ping is not enough. The near-event trajectory must
    # support the same event.
    near_pair_close = (
        F.col("near_dist_km").isNotNull()
        & (F.col("near_dist_km") <= F.lit(MAX_NEAR_EVENT_DISTANCE_KM))
    )

    near_matches_raw_candidate = (
        F.col("near_dist_km").isNotNull()
        & (
            (F.col("near_dist_km") - F.col("dist_km"))
            <= F.lit(MAX_NEAR_MINUS_CANDIDATE_KM)
        )
    )

    collision_point_close_to_a_track = (
        F.col("a_collision_to_near_km").isNotNull()
        & (F.col("a_collision_to_near_km") <= F.lit(MAX_COLLISION_TO_NEAR_CENTROID_KM))
    )

    collision_point_close_to_b_track = (
        F.col("b_collision_to_near_km").isNotNull()
        & (F.col("b_collision_to_near_km") <= F.lit(MAX_COLLISION_TO_NEAR_CENTROID_KM))
    )

    near_consistent = F.coalesce(
        near_pair_close
        & near_matches_raw_candidate
        & collision_point_close_to_a_track
        & collision_point_close_to_b_track,
        F.lit(False)
    )

    post_condition = (
        has_divergence
        | very_close
        | (
            (F.col("dist_km") <= F.lit(FORMATION_ALWAYS_CLOSE_KM))
            & (F.col("pre_dist_km") >= F.lit(FORMATION_ALWAYS_CLOSE_KM))
        )
    )

    # Reject ordinary near-passes where both vessels simply continue normally.
    both_continue_normally_after = (
        (F.coalesce(F.col("a_post_n"), F.lit(0)) >= F.lit(POST_CONTINUE_MIN_PINGS))
        & (F.coalesce(F.col("b_post_n"), F.lit(0)) >= F.lit(POST_CONTINUE_MIN_PINGS))
        & (F.coalesce(F.col("a_post_sog"), F.lit(0.0)) >= F.lit(POST_CONTINUE_SOG_KNOTS))
        & (F.coalesce(F.col("b_post_sog"), F.lit(0.0)) >= F.lit(POST_CONTINUE_SOG_KNOTS))
    )

    if REQUIRE_POST_IMPACT_ANOMALY:
        post_impact_anomaly = ~both_continue_normally_after
    else:
        post_impact_anomaly = F.lit(True)

    # Cap approach/divergence contribution so GPS jumps cannot dominate scoring.
    scored = (
        scored
        .withColumn(
            "approach_score_km",
            F.least(
                F.greatest(F.coalesce(F.col("approach_km"), F.lit(0.0)), F.lit(0.0)),
                F.lit(3.0)
            )
        )
        .withColumn(
            "diverge_score_km",
            F.least(
                F.greatest(F.coalesce(F.col("diverge_km"), F.lit(0.0)), F.lit(0.0)),
                F.lit(3.0)
            )
        )
        .withColumn("both_moving", both_moving)
        .withColumn("always_close", always_close)
        .withColumn("stationary_pair", stationary_pair)
        .withColumn("has_approach", has_approach)
        .withColumn("has_divergence", has_divergence)
        .withColumn("very_close", very_close)
        .withColumn("enough_track_context", enough_track_context)
        .withColumn("plausible_relative_motion", plausible_relative_motion)
        .withColumn("near_pair_close", near_pair_close)
        .withColumn("near_matches_raw_candidate", near_matches_raw_candidate)
        .withColumn("collision_point_close_to_a_track", collision_point_close_to_a_track)
        .withColumn("collision_point_close_to_b_track", collision_point_close_to_b_track)
        .withColumn("near_consistent", near_consistent)
        .withColumn("both_continue_normally_after", both_continue_normally_after)
        .withColumn("post_impact_anomaly", post_impact_anomaly)
        .withColumn(
            "validation_score",
            (
                F.col("approach_score_km") * F.lit(2.0)
                + F.col("diverge_score_km") * F.lit(1.2)
                + F.coalesce(F.col("a_win_span_km"), F.lit(0.0)) * F.lit(0.1)
                + F.coalesce(F.col("b_win_span_km"), F.lit(0.0)) * F.lit(0.1)

                # Prefer genuinely close events.
                - F.col("dist_km") * F.lit(DISTANCE_SCORE_PENALTY)

                # Penalize near-window inconsistency even before hard filtering.
                - F.coalesce(F.col("near_dist_km"), F.lit(10.0)) * F.lit(10.0)
                - F.coalesce(F.col("a_collision_to_near_km"), F.lit(10.0)) * F.lit(3.0)
                - F.coalesce(F.col("b_collision_to_near_km"), F.lit(10.0)) * F.lit(3.0)

                - F.when(F.col("is_tug_like"), F.lit(100.0)).otherwise(F.lit(0.0))
                - F.when(F.col("always_close"), F.lit(30.0)).otherwise(F.lit(0.0))
                - F.when(F.col("stationary_pair"), F.lit(30.0)).otherwise(F.lit(0.0))
                - F.when(~F.col("plausible_relative_motion"), F.lit(100.0)).otherwise(F.lit(0.0))
                - F.when(~F.col("enough_track_context"), F.lit(30.0)).otherwise(F.lit(0.0))
                - F.when(~F.col("near_consistent"), F.lit(100.0)).otherwise(F.lit(0.0))
                - F.when(~F.col("post_impact_anomaly"), F.lit(80.0)).otherwise(F.lit(0.0))
            )
        )
        .persist()
    )

    strict_valid = scored.filter(
        has_pre_evidence
        & has_near_evidence
        & F.col("enough_track_context")
        & F.col("plausible_relative_motion")
        & F.col("near_consistent")
        & F.col("post_impact_anomaly")
        & F.col("both_moving")
        & ~F.col("is_tug_like")
        & ~F.col("always_close")
        & ~F.col("stationary_pair")
        & (F.col("has_approach") | F.col("very_close"))
        & post_condition
    )

    strict_count = strict_valid.count()

    log.info(
        "Strict valid collision candidates after trajectory validation: {:,}".format(strict_count)
    )

    if strict_count > 0:
        best_pdf = (
            strict_valid
            .orderBy(
                F.col("validation_score").desc(),
                F.col("near_dist_km").asc(),
                F.col("dist_km").asc(),
                F.col("time_diff_sec").asc(),
            )
            .limit(1)
            .toPandas()
        )

        log.info("Selected strict-valid collision candidate.")

    else:
        log.warning(
            "No strict-valid candidate passed all filters. "
            "Trying relaxed but still near-consistent/post-anomaly fallback."
        )

        relaxed_valid = scored.filter(
            has_pre_evidence
            & has_near_evidence
            & F.col("enough_track_context")
            & F.col("plausible_relative_motion")
            & F.col("near_consistent")
            & F.col("post_impact_anomaly")
            & F.col("both_moving")
            & ~F.col("is_tug_like")
            & ~F.col("always_close")
            & ~F.col("stationary_pair")
            & (
                (F.col("approach_km") > F.lit(0.01))
                | (F.col("dist_km") <= F.lit(0.10))
                | (F.col("near_dist_km") <= F.lit(0.25))
            )
        )

        relaxed_count = relaxed_valid.count()

        log.info("Relaxed valid collision candidates: {:,}".format(relaxed_count))

        if relaxed_count > 0:
            best_pdf = (
                relaxed_valid
                .orderBy(
                    F.col("validation_score").desc(),
                    F.col("near_dist_km").asc(),
                    F.col("dist_km").asc(),
                    F.col("time_diff_sec").asc(),
                )
                .limit(1)
                .toPandas()
            )

            log.warning("Selected relaxed-valid collision candidate.")

        else:
            log.warning(
                "No relaxed candidate passed. Showing top diagnostic rows before final fallback."
            )

            scored.select(
                "mmsi_a", "name_a", "ship_type_a",
                "mmsi_b", "name_b", "ship_type_b",
                "collision_ts", "collision_lat", "collision_lon",
                "dist_km",
                "pre_dist_km",
                "near_dist_km",
                "post_dist_km",
                "a_collision_to_near_km",
                "b_collision_to_near_km",
                "approach_km",
                "diverge_km",
                "approach_rate_knots",
                "diverge_rate_knots",
                "both_moving",
                "is_tug_like",
                "always_close",
                "stationary_pair",
                "enough_track_context",
                "plausible_relative_motion",
                "near_consistent",
                "both_continue_normally_after",
                "post_impact_anomaly",
                "near_pair_close",
                "near_matches_raw_candidate",
                "collision_point_close_to_a_track",
                "collision_point_close_to_b_track",
                "validation_score",
            ).orderBy(F.col("validation_score").desc()).show(50, truncate=False)

            fallback = scored.filter(
                F.col("plausible_relative_motion")
                & F.col("enough_track_context")
                & F.col("near_consistent")
                & F.col("post_impact_anomaly")
                & ~F.col("is_tug_like")
                & ~F.col("stationary_pair")
                & ~F.col("always_close")
            )

            fallback_count = fallback.count()

            if fallback_count == 0:
                raise RuntimeError(
                    "No candidates passed validation and no plausible near-consistent/post-anomaly "
                    "fallback exists. Try running with USE_SHIFTED_SPATIAL_BLOCKING=1 and/or "
                    "increasing CANDIDATE_PAIR_LIMIT. You can also loosen "
                    "MAX_NEAR_EVENT_DISTANCE_KM or MAX_COLLISION_TO_NEAR_CENTROID_KM slightly."
                )

            best_pdf = (
                fallback
                .orderBy(
                    F.col("validation_score").desc(),
                    F.col("near_dist_km").asc(),
                    F.col("dist_km").asc(),
                    F.col("time_diff_sec").asc(),
                )
                .limit(1)
                .toPandas()
            )

            log.warning("Selected final fallback candidate. Review report/plot carefully.")

    if best_pdf.empty:
        raise RuntimeError("Collision selection produced an empty result.")

    best = best_pdf.iloc[0]

    near_m = (
        float(best.get("near_dist_km")) * 1000.0
        if pd.notna(best.get("near_dist_km"))
        else -1.0
    )

    log.info(
        "✓ COLLISION SELECTED: %s MMSI %s ↔ %s MMSI %s at %s, %.1f m apart "
        "(near-event %.1f m)",
        clean_event_name(best.get("name_a")),
        str(best.get("mmsi_a")),
        clean_event_name(best.get("name_b")),
        str(best.get("mmsi_b")),
        str(best.get("collision_ts")),
        float(best.get("dist_km")) * 1000.0,
        near_m,
    )

    return best


# ────────────────────────────────────────────────────────────────────
# STAGE 6 — TRAJECTORY EXTRACTION
# ────────────────────────────────────────────────────────────────────

def extract_trajectories(df, event) -> pd.DataFrame:
    """
    Pull all pings for both vessels within exactly ±10 minutes of collision.

    This version filters by Unix epoch seconds instead of timestamp literals.
    That avoids timezone/datetime conversion issues between Pandas, Python, and Spark.

    The validation stage already works with event_sec / ping_sec, so this uses
    the same method.
    """

    mmsi_a = str(event["mmsi_a"])
    mmsi_b = str(event["mmsi_b"])

    # Prefer Spark-generated event_sec because that is exactly what validation used.
    if "event_sec" in event and pd.notna(event["event_sec"]):
        t0_sec = int(event["event_sec"])
    elif "collision_t_sec" in event and pd.notna(event["collision_t_sec"]):
        t0_sec = int(event["collision_t_sec"])
    else:
        ts = pd.Timestamp(event["collision_ts"])

        # Treat naive timestamps as UTC, matching spark.sql.session.timeZone=UTC.
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")

        t0_sec = int(ts.timestamp())

    lo_sec = t0_sec - TRAJ_WINDOW_MIN * 60
    hi_sec = t0_sec + TRAJ_WINDOW_MIN * 60

    traj = (
        df
        .withColumn("mmsi_str", F.col("mmsi").cast(StringType()))
        .withColumn("ping_sec", F.col("ts").cast(LongType()))
        .filter(F.col("mmsi_str").isin([mmsi_a, mmsi_b]))
        .filter(
            (F.col("ping_sec") >= F.lit(lo_sec))
            & (F.col("ping_sec") <= F.lit(hi_sec))
        )
        .drop("mmsi_str", "ping_sec")
        .orderBy("mmsi", "ts")
        .toPandas()
    )

    if not traj.empty:
        traj["mmsi"] = traj["mmsi"].astype(str)

    log.info(
        "Trajectory pings: %d, both vessels, ±%d min, epoch window [%d, %d]",
        len(traj),
        TRAJ_WINDOW_MIN,
        lo_sec,
        hi_sec,
    )

    return traj


# ────────────────────────────────────────────────────────────────────
# STAGE 7 — VISUALIZATION
# ────────────────────────────────────────────────────────────────────

def plot_trajectories(traj: pd.DataFrame, event) -> str:
    """
    Plot both vessels' 20-minute trajectory window on a map.

    Output:
        OUTPUT_DIR/trajectory_map.png
    """

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    mmsi_a = str(event["mmsi_a"])
    mmsi_b = str(event["mmsi_b"])

    name_a = clean_event_name(event.get("name_a"))
    name_b = clean_event_name(event.get("name_b"))

    c_lat = float(event["collision_lat"])
    c_lon = float(event["collision_lon"])
    c_time = event["collision_ts"]
    dist_m = float(event["dist_km"]) * 1000.0

    if traj.empty:
        log.warning("Trajectory DataFrame is empty. Plot will contain only collision point.")
    else:
        traj["mmsi"] = traj["mmsi"].astype(str)

    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

    def to_mercator(sub):
        if sub.empty:
            return [], []

        xs, ys = transformer.transform(sub["lon"].values, sub["lat"].values)
        return xs, ys

    df_a = traj[traj["mmsi"] == mmsi_a].sort_values("ts") if not traj.empty else pd.DataFrame()
    df_b = traj[traj["mmsi"] == mmsi_b].sort_values("ts") if not traj.empty else pd.DataFrame()

    xs_a, ys_a = to_mercator(df_a)
    xs_b, ys_b = to_mercator(df_b)

    cx, cy = transformer.transform(c_lon, c_lat)

    fig, ax = plt.subplots(figsize=(13, 10))

    all_x = list(xs_a) + list(xs_b) + [cx]
    all_y = list(ys_a) + list(ys_b) + [cy]

    x_margin = (max(all_x) - min(all_x)) * 0.3 + 500
    y_margin = (max(all_y) - min(all_y)) * 0.3 + 500

    ax.set_xlim(min(all_x) - x_margin, max(all_x) + x_margin)
    ax.set_ylim(min(all_y) - y_margin, max(all_y) + y_margin)

    color_a = "#1f77b4"
    color_b = "#ff7f0e"

    if len(xs_a):
        ax.plot(
            xs_a,
            ys_a,
            "-o",
            color=color_a,
            markersize=5,
            linewidth=2.0,
            label=f"Vessel A: {name_a}  (MMSI {mmsi_a})",
            zorder=3,
        )

        ax.plot(
            xs_a[0],
            ys_a[0],
            "^",
            color=color_a,
            markersize=11,
            zorder=4,
            markeredgecolor="white",
            markeredgewidth=0.8,
        )

        ax.plot(
            xs_a[-1],
            ys_a[-1],
            "s",
            color=color_a,
            markersize=11,
            zorder=4,
            markeredgecolor="white",
            markeredgewidth=0.8,
        )

    if len(xs_b):
        ax.plot(
            xs_b,
            ys_b,
            "-o",
            color=color_b,
            markersize=5,
            linewidth=2.0,
            label=f"Vessel B: {name_b}  (MMSI {mmsi_b})",
            zorder=3,
        )

        ax.plot(
            xs_b[0],
            ys_b[0],
            "^",
            color=color_b,
            markersize=11,
            zorder=4,
            markeredgecolor="white",
            markeredgewidth=0.8,
        )

        ax.plot(
            xs_b[-1],
            ys_b[-1],
            "s",
            color=color_b,
            markersize=11,
            zorder=4,
            markeredgecolor="white",
            markeredgewidth=0.8,
        )

    ax.plot(
        cx,
        cy,
        "*",
        color="#d62728",
        markersize=24,
        zorder=5,
        label=f"Selected collision/closest-approach point ({dist_m:.1f} m apart)",
    )

    try:
        ctx.add_basemap(
            ax,
            crs="EPSG:3857",
            source=ctx.providers.CartoDB.Positron,
            zoom=12,
        )
    except Exception as exc:
        log.warning("Basemap tiles unavailable: %s", exc)

    ax.set_title(
        f"Vessel Collision Trajectory — {c_time}\n"
        f"Position: {c_lat:.6f}°N, {c_lon:.6f}°E | Window: ±{TRAJ_WINDOW_MIN} min",
        fontsize=13,
        fontweight="bold",
    )

    ax.set_xlabel("Easting EPSG:3857")
    ax.set_ylabel("Northing EPSG:3857")

    handles, labels = ax.get_legend_handles_labels()
    extra = [mpatches.Patch(color="none", label="▲ track start   ■ track end")]

    ax.legend(
        handles + extra,
        labels + ["▲ track start   ■ track end"],
        loc="upper left",
        fontsize=9,
        framealpha=0.9,
    )

    out = os.path.join(OUTPUT_DIR, "trajectory_map.png")

    plt.tight_layout()
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)

    log.info("Trajectory map saved → %s", out)

    return out


# ────────────────────────────────────────────────────────────────────
# STAGE 8 — PRINT RESULTS
# ────────────────────────────────────────────────────────────────────

def print_results(event) -> None:
    """
    Print final result in assignment-required format.
    """

    dist_m = float(event["dist_km"]) * 1000.0

    sep = "=" * 72

    print("\n" + sep)
    print("   AIS COLLISION DETECTION — FINAL RESULT")
    print(sep)

    print(f"   Vessel A MMSI              :  {str(event['mmsi_a'])}")
    print(f"   Vessel A Name              :  {clean_event_name(event.get('name_a'))}")
    print(f"   Vessel A Ship Type         :  {clean_event_name(event.get('ship_type_a'))}")

    print(f"   Vessel B MMSI              :  {str(event['mmsi_b'])}")
    print(f"   Vessel B Name              :  {clean_event_name(event.get('name_b'))}")
    print(f"   Vessel B Ship Type         :  {clean_event_name(event.get('ship_type_b'))}")

    print(f"   Collision Time             :  {event['collision_ts']}")
    print(f"   Latitude                   :  {float(event['collision_lat']):.6f} °N")
    print(f"   Longitude                  :  {float(event['collision_lon']):.6f} °E")
    print(f"   Closest Distance           :  {dist_m:.1f} m")

    if "pre_dist_km" in event and pd.notna(event["pre_dist_km"]):
        print(f"   Pre-event Distance         :  {float(event['pre_dist_km']) * 1000.0:.1f} m")

    if "near_dist_km" in event and pd.notna(event["near_dist_km"]):
        print(f"   Near-event Distance        :  {float(event['near_dist_km']) * 1000.0:.1f} m")

    if "post_dist_km" in event and pd.notna(event["post_dist_km"]):
        print(f"   Post-event Distance        :  {float(event['post_dist_km']) * 1000.0:.1f} m")

    if "a_collision_to_near_km" in event and pd.notna(event["a_collision_to_near_km"]):
        print(f"   A Star-to-Near-Track Dist  :  {float(event['a_collision_to_near_km']) * 1000.0:.1f} m")

    if "b_collision_to_near_km" in event and pd.notna(event["b_collision_to_near_km"]):
        print(f"   B Star-to-Near-Track Dist  :  {float(event['b_collision_to_near_km']) * 1000.0:.1f} m")

    if "approach_km" in event and pd.notna(event["approach_km"]):
        print(f"   Approach Magnitude         :  {float(event['approach_km']) * 1000.0:.1f} m")

    if "diverge_km" in event and pd.notna(event["diverge_km"]):
        print(f"   Divergence Magnitude       :  {float(event['diverge_km']) * 1000.0:.1f} m")

    if "approach_rate_knots" in event and pd.notna(event["approach_rate_knots"]):
        print(f"   Approach Rate Estimate     :  {float(event['approach_rate_knots']):.1f} kn")

    if "diverge_rate_knots" in event and pd.notna(event["diverge_rate_knots"]):
        print(f"   Divergence Rate Estimate   :  {float(event['diverge_rate_knots']):.1f} kn")

    if "near_consistent" in event:
        print(f"   Near-event Consistent      :  {bool(event['near_consistent'])}")

    if "both_continue_normally_after" in event:
        print(f"   Both Continue Normally     :  {bool(event['both_continue_normally_after'])}")

    if "post_impact_anomaly" in event:
        print(f"   Post-impact Anomaly        :  {bool(event['post_impact_anomaly'])}")

    if "is_tug_like" in event:
        print(f"   Tug/Tow/Pilot Like         :  {bool(event['is_tug_like'])}")

    if "validation_score" in event and pd.notna(event["validation_score"]):
        print(f"   Validation Score           :  {float(event['validation_score']):.3f}")

    print(sep + "\n")


# ────────────────────────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────────────────────────

def main():
    spark = build_spark()

    raw = load_raw(spark)

    cleaned = filter_noise(raw)

    if RUN_SANITY_CHECK:
        sanity_check_known_incident(cleaned)

    denoised = remove_teleportation(cleaned)

    # Cache because reused by candidate generation, validation, and plotting.
    denoised = denoised.persist()

    denoised_count = denoised.count()

    log.info("Denoised dataset cached: {:,} rows".format(denoised_count))

    candidates = generate_candidates(denoised)

    candidates = candidates.persist()

    candidate_count = candidates.count()

    log.info("Candidate dataset cached: {:,} rows".format(candidate_count))

    event = find_collision(candidates, denoised)

    trajectories = extract_trajectories(denoised, event)

    plot_trajectories(trajectories, event)

    print_results(event)

    spark.stop()

    log.info("Pipeline complete.")


if __name__ == "__main__":
    main()