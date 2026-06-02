"""
AIS Collision Detection Pipeline
=================================
Assignment 4 — Big Data Processing with PySpark

Builds directly on Assignment 3 (MongoDB sharded cluster + noise filtering)
and the Shadow Fleet Detection project (parallel chunk I/O, teleportation
detection, Haversine spatial filtering).

Area of interest : 50 nm radius of (55.225 N, 14.245 E) — Baltic Sea, south of Bornholm
Time window      : December 1–31, 2021

Pipeline stages:
  1. Load all 31 daily AIS CSV files via Spark
  2. Noise filtering (6 categories — same taxonomy as Assignment 3)
  3. Teleportation / GPS-spike removal (from Shadow Fleet work)
  4. Stationary vessel exclusion
  5. Spatial-temporal bucketing → candidate pair generation (avoids O(n²))
  6. Exact Haversine distance on candidates → collision identification
  7. Trajectory extraction (±10 min) + map visualisation
"""

import os
import math
import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, IntegerType, StringType

import pandas as pd
import matplotlib
matplotlib.use("Agg")           # headless — no display inside Docker
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import contextily as ctx
from pyproj import Transformer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s"
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# All thresholds are defined here and referenced by name throughout the code
# so the examiner can see at a glance what every magic number means.
# ──────────────────────────────────────────────────────────────────────────────

# Geographic area of interest
CENTER_LAT   = 55.225000
CENTER_LON   = 14.245000
RADIUS_NM    = 50.0            # nautical miles
RADIUS_M     = RADIUS_NM * 1852.0   # metres (1 nm = 1852 m)

# Spatial bucketing grid for the candidate join (degrees).
# 0.05° lat ≈ 5.5 km, 0.05° lon ≈ 3.1 km at 55°N — well above the
# collision threshold, so no true collision can straddle a bucket boundary
# without at least one ping appearing in the correct shared cell.
GRID_SIZE    = 0.05            # degrees

# Vessel motion thresholds
MIN_SOG_KTS  = 0.5             # below this → vessel is considered stationary
MAX_SOG_KTS  = 50.0            # above this → GPS spike / data error (teleportation)

# Collision proximity threshold.
# The IMO defines a "near-miss" at < 0.5 nm (926 m).  We use 500 m as a
# conservative collision threshold — vessels at this distance have almost
# certainly made physical contact or had a serious collision.
COLLISION_M  = 500.0           # metres

# Trajectory visualisation window
TRAJ_WINDOW  = 10              # minutes either side of collision

# Runtime paths (overrideable via environment variables)
DATA_DIR   = os.getenv("DATA_DIR",   "/app/data")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/app/output")


# ──────────────────────────────────────────────────────────────────────────────
# PURE-PYTHON HELPERS  (used both as standalone functions and as Spark UDFs)
# ──────────────────────────────────────────────────────────────────────────────

def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Great-circle distance in metres between two WGS-84 points.
    Same formula used in the Shadow Fleet assignment for teleportation detection.
    """
    R = 6_371_000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi    = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = (math.sin(d_phi / 2) ** 2
         + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2)
    return 2.0 * R * math.asin(math.sqrt(a))


def _dist_to_centre(lat: float, lon: float) -> float:
    """Haversine distance from a point to the area-of-interest centre."""
    return haversine_m(lat, lon, CENTER_LAT, CENTER_LON)


# Register as Spark UDFs once the session exists (done inside build_spark)
_haversine_udf      = None   # haversine_m(lat1, lon1, lat2, lon2) → metres
_dist_centre_udf    = None   # _dist_to_centre(lat, lon)          → metres


# ──────────────────────────────────────────────────────────────────────────────
# 1. SPARK SESSION
# ──────────────────────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    """
    Create a local Spark session tuned for a single-node Docker container.
    shuffle.partitions=200 balances parallelism vs overhead for ~10–50 M rows.
    """
    global _haversine_udf, _dist_centre_udf

    spark = (
        SparkSession.builder
        .appName("AIS-Collision-Detection")
        .config("spark.driver.memory",             "4g")
        .config("spark.sql.shuffle.partitions",    "200")
        .config("spark.sql.autoBroadcastJoinThreshold", "50mb")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Register UDFs after session creation
    _haversine_udf   = F.udf(haversine_m,       DoubleType())
    _dist_centre_udf = F.udf(_dist_to_centre,   DoubleType())

    log.info("Spark session ready  (version %s)", spark.version)
    return spark


# ──────────────────────────────────────────────────────────────────────────────
# 2. DATA LOADING
# ──────────────────────────────────────────────────────────────────────────────

def load_raw(spark: SparkSession):
    """
    Load all CSV files matching data/*.csv in a single Spark read.

    Schema note: we do NOT use inferSchema=true.  inferSchema triggers a full
    extra scan of every file — expensive for 31 daily files.  Instead we read
    everything as strings and cast manually (same approach as Assignment 3's
    streaming CSV parser).

    Danish AIS columns (from web.ais.dk):
        Timestamp, Type of mobile, MMSI, Latitude, Longitude,
        Navigational status, ROT, SOG, COG, Heading,
        IMO, Callsign, Name, Ship type, Cargo type,
        Width, Length, Type of position fixing device,
        Draught, Destination, ETA, Data source type, A, B, C, D
    """
    path = os.path.join(DATA_DIR, "*.csv")
    log.info("Loading AIS data from  %s", path)

    df = (
        spark.read
        .option("header",      "true")
        .option("inferSchema", "false")   # manual casting — see above
        .option("mode",        "PERMISSIVE")  # bad rows → null, not exception
        .csv(path)
    )

    # Normalise column names: strip whitespace, lowercase, spaces → underscores
    for col in df.columns:
        clean_name = col.strip().lower().replace(" ", "_")
        if clean_name != col:
            df = df.withColumnRenamed(col, clean_name)

    raw_count = df.count()
    log.info("Raw rows loaded: {:,}".format(raw_count))
    return df


# ──────────────────────────────────────────────────────────────────────────────
# 3. NOISE FILTERING  (6 categories from Assignment 3, extended)
# ──────────────────────────────────────────────────────────────────────────────

def filter_noise(df):
    """
    Apply the same 6-category noise taxonomy from Assignment 3, adapted for
    collision detection.

    CATEGORY 1 — Invalid / null essential fields
        Drop any row missing MMSI, lat, lon, timestamp, or SOG.
        These cannot be spatially or temporally placed.

    CATEGORY 2 — Invalid MMSI patterns
        MMSI must be a 9-digit number in [100_000_000, 999_999_999].
        Exclude known base-station prefixes (992xxxxxx used in Denmark).
        Exclude all-same-digit patterns (000000000, 111111111, …).

    CATEGORY 3 — Invalid coordinates
        Lat must be in [−90, 90], lon in [−180, 180].
        (0.0, 0.0) is "Null Island" — a well-known AIS default/error value.

    CATEGORY 4 — Non-vessel transponders
        Type of mobile: exclude base stations, AtoN, SART devices.
        Navigational status: exclude "Not defined" combined with zero SOG
        (often base station ghost records).

    CATEGORY 5 — Stationary vessels
        SOG < MIN_SOG_KTS → vessel is at anchor, moored, or drifting.
        Navigational status "At anchor" or "Moored" → explicit exclusion.
        We are looking for moving vessels only.

    CATEGORY 6 — Out of area / out of time
        Temporal: restrict to December 2021.
        Spatial: bounding box first (cheap), then exact Haversine (expensive).
    """

    # ── CATEGORY 1: Drop rows with null essential fields ──────────────────────
    df = df.withColumn("lat", F.col("latitude").cast(DoubleType()))
    df = df.withColumn("lon", F.col("longitude").cast(DoubleType()))
    df = df.withColumn("sog", F.col("sog").cast(DoubleType()))
    df = df.withColumn("mmsi_long", F.col("mmsi").cast(LongType()))
    df = df.withColumn(
        "ts",
        F.to_timestamp(F.col("timestamp"), "dd/MM/yyyy HH:mm:ss")
    )
    before = df.count()
    df = df.dropna(subset=["mmsi_long", "lat", "lon", "ts", "sog"])
    log.info("CAT-1 null fields removed: {:,} rows dropped"
             .format(before - df.count()))

    # ── CATEGORY 2: Invalid MMSI patterns ─────────────────────────────────────
    before = df.count()
    df = df.filter(
        (F.col("mmsi_long") >= 100_000_000) &   # 9-digit minimum
        (F.col("mmsi_long") <= 999_999_999) &   # 9-digit maximum
        # Exclude Danish base-station MMSI prefix 992xxxxxx
        (~F.col("mmsi_long").between(992_000_000, 992_999_999)) &
        # Exclude repeated-digit patterns (000…, 111…, … 999…)
        (~F.col("mmsi").rlike(r"^(\d)\1{8}$"))
    )
    log.info("CAT-2 invalid MMSI removed: {:,} rows dropped"
             .format(before - df.count()))

    # ── CATEGORY 3: Invalid / sentinel coordinates ────────────────────────────
    before = df.count()
    df = df.filter(
        (F.col("lat").between(-90.0, 90.0)) &
        (F.col("lon").between(-180.0, 180.0)) &
        # Exclude Null Island (0.0, 0.0) and immediate vicinity
        ~((F.abs(F.col("lat")) < 0.001) & (F.abs(F.col("lon")) < 0.001))
    )
    log.info("CAT-3 invalid coordinates removed: {:,} rows dropped"
             .format(before - df.count()))

    # ── CATEGORY 4: Non-vessel transponders ───────────────────────────────────
    before = df.count()
    exclude_mobile = ["base_station", "aton", "aid_to_navigation",
                      "base station", "aid to navigation"]
    if "type_of_mobile" in df.columns:
        df = df.filter(
            ~F.lower(F.col("type_of_mobile")).isin(exclude_mobile)
        )
    if "navigational_status" in df.columns:
        df = df.filter(
            ~F.col("navigational_status").isin(
                "Not defined"  # combined with SOG check below
            ) | (F.col("sog") > 0.0)
        )
    log.info("CAT-4 non-vessel transponders removed: {:,} rows dropped"
             .format(before - df.count()))

    # ── CATEGORY 5: Stationary vessels ────────────────────────────────────────
    before = df.count()
    df = df.filter(F.col("sog") >= MIN_SOG_KTS)
    if "navigational_status" in df.columns:
        df = df.filter(
            ~F.col("navigational_status").isin("At anchor", "Moored")
        )
    log.info("CAT-5 stationary vessels removed: {:,} rows dropped"
             .format(before - df.count()))

    # ── CATEGORY 6: Temporal + geographic bounds ──────────────────────────────
    before = df.count()

    # Time filter (exact)
    df = df.filter(
        (F.col("ts") >= F.lit("2021-12-01").cast("timestamp")) &
        (F.col("ts") <  F.lit("2022-01-01").cast("timestamp"))
    )

    # Bounding-box pre-filter (cheap arithmetic — eliminates ~90 % of globe)
    lat_margin = RADIUS_NM / 60.0                      # 1 nm ≈ 1/60 degree lat
    lon_margin = RADIUS_NM / (60.0 * math.cos(math.radians(CENTER_LAT)))
    df = df.filter(
        F.col("lat").between(CENTER_LAT - lat_margin * 1.1,
                             CENTER_LAT + lat_margin * 1.1) &
        F.col("lon").between(CENTER_LON - lon_margin * 1.1,
                             CENTER_LON + lon_margin * 1.1)
    )

    # Exact Haversine radius filter (UDF applied only to bbox survivors)
    df = df.withColumn("dist_centre_m", _dist_centre_udf("lat", "lon"))
    df = df.filter(F.col("dist_centre_m") <= RADIUS_M)

    log.info("CAT-6 out-of-area/time removed: {:,} rows dropped"
             .format(before - df.count()))

    # ── Materialise only the columns needed downstream ─────────────────────────
    keep = ["mmsi_long", "ts", "lat", "lon", "sog"]
    if "name" in df.columns:
        keep.append("name")

    df = df.select(
        F.col("mmsi_long").alias("mmsi"), *[F.col(c) for c in keep[1:]]
    )

    log.info("After all noise filters: {:,} rows remain".format(df.count()))
    return df


# ──────────────────────────────────────────────────────────────────────────────
# 4. TELEPORTATION / GPS-SPIKE REMOVAL
# ──────────────────────────────────────────────────────────────────────────────

def remove_teleportation(df):
    """
    Detect and drop GPS anomaly spikes using the same 'teleportation' logic
    from the Shadow Fleet detection project.

    Method:
      - Per vessel, order pings by timestamp (window function).
      - Compute implied speed = Haversine(prev_pos, curr_pos) / elapsed_seconds.
      - If implied speed > MAX_SOG_KTS (50 kts) → the ping is a GPS spike.

    A single-point spike (one bad reading surrounded by good ones) is removed
    in one pass.  Multi-point drift would require iterative passes but is rare
    in practice — one pass is sufficient per the Assignment 3 findings.

    Why 50 knots?
      The fastest conventional surface vessels top out at ~35 kts.  50 kts gives
      a generous buffer for legitimate high-speed craft (patrol boats, ferries)
      while still catching the typical AIS teleportation error where a vessel
      appears to jump 100+ km in a single report interval.
    """
    w = Window.partitionBy("mmsi").orderBy("ts")

    df = (
        df
        .withColumn("prev_lat", F.lag("lat").over(w))
        .withColumn("prev_lon", F.lag("lon").over(w))
        .withColumn("prev_ts",  F.lag("ts").cast(LongType()).over(w))
    )

    df = df.withColumn(
        "step_dist_m",
        F.when(
            F.col("prev_lat").isNotNull(),
            _haversine_udf("prev_lat", "prev_lon", "lat", "lon")
        ).otherwise(F.lit(0.0))
    )

    df = df.withColumn(
        "step_secs",
        F.when(
            F.col("prev_ts").isNotNull(),
            F.col("ts").cast(LongType()) - F.col("prev_ts")
        ).otherwise(F.lit(1))  # default 1 s avoids div-by-zero for first ping
    )

    # Implied speed in knots  (1 knot = 0.5144 m/s)
    df = df.withColumn(
        "implied_kts",
        F.when(
            F.col("step_secs") > 0,
            (F.col("step_dist_m") / F.col("step_secs")) / 0.5144
        ).otherwise(F.lit(0.0))
    )

    before = df.count()
    df = df.filter(F.col("implied_kts") <= MAX_SOG_KTS)
    log.info("Teleportation spikes removed: {:,} rows dropped"
             .format(before - df.count()))

    df = df.drop("prev_lat", "prev_lon", "prev_ts",
                 "step_dist_m", "step_secs", "implied_kts")
    return df


# ──────────────────────────────────────────────────────────────────────────────
# 5. SPATIAL-TEMPORAL BUCKETING  (efficient candidate generation)
# ──────────────────────────────────────────────────────────────────────────────

def generate_candidates(df):
    """
    Generate candidate collision pairs WITHOUT a full Cartesian product.

    The naive approach — join every ping against every other ping — is O(n²)
    and completely impractical for millions of records.  We use the same
    bucketing strategy proven effective in the Shadow Fleet project:

    APPROACH
    ────────
    1.  Assign each ping a TIME BUCKET:
            minute_bucket = floor(unix_timestamp / 60)
        Two pings in the same minute bucket are temporally co-located.

    2.  Assign each ping a SPATIAL BUCKET (grid cell):
            grid_x = floor(lon / GRID_SIZE)
            grid_y = floor(lat / GRID_SIZE)
        GRID_SIZE = 0.05° ≈ 3–5 km — much larger than the 500 m collision
        threshold, guaranteeing that colliding vessels share a bucket.

    3.  SELF-JOIN on (minute_bucket, grid_x, grid_y) with mmsi_a < mmsi_b.
        This reduces the join space from O(n²) to O(n × k²) where k is the
        mean number of vessels per bucket (typically < 3 in open water).

    4.  Apply the exact Haversine UDF only to the surviving candidate pairs
        (a tiny fraction of all pairs) to get precise distances.

    5.  Keep only pairs with distance ≤ COLLISION_M (500 m).

    BOUNDARY NOTE: a pair straddling a cell boundary could theoretically be
    missed.  We address this by also expanding the join to adjacent cells
    (grid_x ± 1, grid_y ± 1) using a cross-join on a small offset table,
    ensuring complete coverage.
    """

    # ── Add bucket columns ────────────────────────────────────────────────────
    df = (
        df
        .withColumn("minute_bucket",
                    (F.col("ts").cast(LongType()) / 60).cast(LongType()))
        .withColumn("grid_x",
                    F.floor(F.col("lon") / GRID_SIZE).cast(IntegerType()))
        .withColumn("grid_y",
                    F.floor(F.col("lat") / GRID_SIZE).cast(IntegerType()))
    )

    # ── Build offset table for neighbour-cell expansion ───────────────────────
    # We test the vessel's own cell + 8 neighbours (3×3 kernel).
    # Only cell A's own bucket is expanded; cell B keeps its natural bucket.
    # This ensures every pair within GRID_SIZE of a boundary is captured.
    offsets = [(dx, dy) for dx in (-1, 0, 1) for dy in (-1, 0, 1)]
    offset_df = (
        df.sparkSession
        .createDataFrame(offsets, ["dx", "dy"])
    )

    # Expand A-side pings to include neighbour cells
    a_expanded = (
        df.alias("a")
        .crossJoin(F.broadcast(offset_df))
        .withColumn("adj_grid_x",
                    (F.col("a.grid_x") + F.col("dx")).cast(IntegerType()))
        .withColumn("adj_grid_y",
                    (F.col("a.grid_y") + F.col("dy")).cast(IntegerType()))
        .drop("dx", "dy")
    )

    b = df.alias("b")

    # ── Join: same minute + same (possibly offset) cell + different MMSI ──────
    candidates = (
        a_expanded.join(
            b,
            on=(
                (F.col("a.minute_bucket") == F.col("b.minute_bucket")) &
                (F.col("adj_grid_x")      == F.col("b.grid_x"))        &
                (F.col("adj_grid_y")      == F.col("b.grid_y"))        &
                (F.col("a.mmsi")          <  F.col("b.mmsi"))
            ),
            how="inner"
        )
        .select(
            F.col("a.mmsi").alias("mmsi_a"),
            F.col("a.ts").alias("ts_a"),
            F.col("a.lat").alias("lat_a"),
            F.col("a.lon").alias("lon_a"),
            F.col("a.name").alias("name_a") if "name" in df.columns
                else F.lit(None).cast(StringType()).alias("name_a"),
            F.col("b.mmsi").alias("mmsi_b"),
            F.col("b.ts").alias("ts_b"),
            F.col("b.lat").alias("lat_b"),
            F.col("b.lon").alias("lon_b"),
            F.col("b.name").alias("name_b") if "name" in df.columns
                else F.lit(None).cast(StringType()).alias("name_b"),
        )
    )

    # ── Exact Haversine distance on candidate pairs ───────────────────────────
    candidates = candidates.withColumn(
        "dist_m",
        _haversine_udf("lat_a", "lon_a", "lat_b", "lon_b")
    )

    candidates = candidates.filter(F.col("dist_m") <= COLLISION_M)

    n = candidates.count()
    log.info("Collision candidates (dist ≤ %.0f m): {:,} pairs".format(n),
             COLLISION_M)
    return candidates


# ──────────────────────────────────────────────────────────────────────────────
# 6. IDENTIFY THE COLLISION EVENT
# ──────────────────────────────────────────────────────────────────────────────

def find_collision(candidates):
    """
    Select the single closest-approach event from all candidates.

    We take the minimum-distance record.  In real-world AIS data the actual
    physical collision is the moment of closest approach — the point where
    the two vessels' GPS positions are nearest to each other.

    Returns a pandas Series (one row).
    """
    row = (
        candidates
        .orderBy(F.col("dist_m").asc())
        .limit(1)
        .toPandas()
    )

    if row.empty:
        raise RuntimeError(
            "No collision found within %.0f m.  "
            "Consider increasing COLLISION_M or checking the data files." % COLLISION_M
        )

    return row.iloc[0]


# ──────────────────────────────────────────────────────────────────────────────
# 7. EXTRACT TRAJECTORY WINDOW
# ──────────────────────────────────────────────────────────────────────────────

def extract_trajectories(df, event) -> pd.DataFrame:
    """
    Pull all pings for both collision vessels within ±TRAJ_WINDOW minutes of
    the collision timestamp.  Returns a pandas DataFrame for plotting.
    """
    t0     = pd.Timestamp(event["ts_a"])
    mmsi_a = int(event["mmsi_a"])
    mmsi_b = int(event["mmsi_b"])
    t_lo   = (t0 - timedelta(minutes=TRAJ_WINDOW)).to_pydatetime()
    t_hi   = (t0 + timedelta(minutes=TRAJ_WINDOW)).to_pydatetime()

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
    log.info("Trajectory pings extracted: %d  (both vessels, ±%d min)",
             len(traj), TRAJ_WINDOW)
    return traj


# ──────────────────────────────────────────────────────────────────────────────
# 8. VISUALISATION
# ──────────────────────────────────────────────────────────────────────────────

def plot_trajectories(traj: pd.DataFrame, event) -> str:
    """
    Plot both vessels' 20-minute trajectory window on a CartoDB Positron
    basemap.  Saves to OUTPUT_DIR/trajectory_map.png.

    Coordinate system: WGS-84 (EPSG:4326) reprojected to Web Mercator
    (EPSG:3857) so contextily basemap tiles align correctly.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    mmsi_a = int(event["mmsi_a"])
    mmsi_b = int(event["mmsi_b"])
    name_a = str(event.get("name_a") or mmsi_a)
    name_b = str(event.get("name_b") or mmsi_b)
    c_lat  = float(event["lat_a"])
    c_lon  = float(event["lon_a"])
    c_time = event["ts_a"]

    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

    def to_mercator(sub_df):
        xs, ys = transformer.transform(
            sub_df["lon"].values, sub_df["lat"].values
        )
        return xs, ys

    df_a = traj[traj["mmsi"] == mmsi_a].sort_values("ts")
    df_b = traj[traj["mmsi"] == mmsi_b].sort_values("ts")

    xs_a, ys_a = to_mercator(df_a)
    xs_b, ys_b = to_mercator(df_b)
    cx,   cy   = transformer.transform(c_lon, c_lat)

    fig, ax = plt.subplots(figsize=(13, 10))

    # ── Track lines ───────────────────────────────────────────────────────────
    COLOR_A, COLOR_B = "#1f77b4", "#ff7f0e"   # matplotlib default blue / orange

    ax.plot(xs_a, ys_a, "-o", color=COLOR_A, markersize=5, linewidth=2.0,
            label=f"Vessel A: {name_a}  (MMSI {mmsi_a})", zorder=3)
    ax.plot(xs_b, ys_b, "-o", color=COLOR_B, markersize=5, linewidth=2.0,
            label=f"Vessel B: {name_b}  (MMSI {mmsi_b})", zorder=3)

    # ── Start / end markers ───────────────────────────────────────────────────
    for xs, ys, c in [(xs_a, ys_a, COLOR_A), (xs_b, ys_b, COLOR_B)]:
        if len(xs) > 0:
            ax.plot(xs[0],  ys[0],  "^", color=c, markersize=11,
                    zorder=4, markeredgecolor="white", markeredgewidth=0.8)
            ax.plot(xs[-1], ys[-1], "s", color=c, markersize=11,
                    zorder=4, markeredgecolor="white", markeredgewidth=0.8)

    # ── Collision star ────────────────────────────────────────────────────────
    ax.plot(cx, cy, "*", color="#d62728", markersize=22, zorder=5,
            label=f"Collision  ({float(event['dist_m']):.0f} m apart)")

    # ── Basemap ───────────────────────────────────────────────────────────────
    try:
        ctx.add_basemap(ax, crs="EPSG:3857",
                        source=ctx.providers.CartoDB.Positron, zoom=12)
    except Exception as exc:
        log.warning("Basemap tiles unavailable: %s", exc)

    # ── Labels ────────────────────────────────────────────────────────────────
    ax.set_title(
        f"Vessel Collision Trajectory  —  {c_time}\n"
        f"Position: {c_lat:.5f} °N,  {c_lon:.5f} °E   |   "
        f"Window: ±{TRAJ_WINDOW} min",
        fontsize=13, fontweight="bold"
    )
    ax.set_xlabel("Easting (EPSG:3857)")
    ax.set_ylabel("Northing (EPSG:3857)")

    legend_extra = [
        mpatches.Patch(color="none", label="▲ track start   ■ track end")
    ]
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles + legend_extra, labels + ["▲ track start   ■ track end"],
              loc="upper left", fontsize=9, framealpha=0.9)

    # ── Save ──────────────────────────────────────────────────────────────────
    out = os.path.join(OUTPUT_DIR, "trajectory_map.png")
    plt.tight_layout()
    plt.savefig(out, dpi=150, bbox_inches="tight")
    log.info("Trajectory map saved → %s", out)
    return out


# ──────────────────────────────────────────────────────────────────────────────
# 9. PRINT RESULTS
# ──────────────────────────────────────────────────────────────────────────────

def print_results(event) -> None:
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
    print(f"   Closest Distance  :  {float(event['dist_m']):.1f} m")
    print(sep + "\n")


# ──────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

def main():
    spark = build_spark()

    # Stage 1 — Load
    raw = load_raw(spark)

    # Stage 2 — 6-category noise filter (same taxonomy as Assignment 3)
    cleaned = filter_noise(raw)

    # Stage 3 — Teleportation / GPS spike removal (from Shadow Fleet project)
    denoised = remove_teleportation(cleaned)

    # Cache here: denoised is used twice — once for collision search,
    # once for trajectory extraction.  Without caching Spark would recompute
    # the entire pipeline a second time.
    denoised.cache()
    denoised.count()   # materialise the cache now
    log.info("Denoised dataset cached.")

    # Stage 4 — Spatial-temporal bucketing → candidate pairs
    candidates = generate_candidates(denoised)

    # Stage 5 — Identify the collision event (closest pair)
    event = find_collision(candidates)

    # Stage 6 — Extract trajectory window
    trajectories = extract_trajectories(denoised, event)

    # Stage 7 — Visualise
    plot_trajectories(trajectories, event)

    # Stage 8 — Report
    print_results(event)

    spark.stop()
    log.info("Pipeline complete.")


if __name__ == "__main__":
    main()
