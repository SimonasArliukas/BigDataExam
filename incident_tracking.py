import os
import sys
import logging
import glob
import math
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, TimestampType, LongType
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import pandas as pd
import numpy as np

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

#Center of the regio
CENTER_LAT, CENTER_LON = 55.225000, 14.245000
RADIUS_M = 50.0 * 1852.0  # 50 Nautical Miles

#Putting into degrees of lattitude and longtitude
LAT_DEG_PER_M = 1.0 / 111132.0
LON_DEG_PER_M = 1.0 / (111132.0 * math.cos(math.radians(CENTER_LAT)))
GEO_LAT_DELTA = RADIUS_M * LAT_DEG_PER_M
GEO_LON_DELTA = RADIUS_M * LON_DEG_PER_M

#Geographic location
LAT_MIN, LAT_MAX = CENTER_LAT - GEO_LAT_DELTA, CENTER_LAT + GEO_LAT_DELTA
LON_MIN, LON_MAX = CENTER_LON - GEO_LON_DELTA, CENTER_LON + GEO_LON_DELTA


LOCAL_LAT_THRESHOLD = 300.0 * LAT_DEG_PER_M
LOCAL_LON_THRESHOLD = 300.0 * LON_DEG_PER_M

# Time bucketing
TIME_BUCKET_SECONDS = 60
COLLISION_SEP_M = 60.0
TRAJ_WINDOW_SEC = 600
OUTPUT_DIR = "/app/output" if os.path.exists("/.dockerenv") else "./output"

#Filtering combined speed higher than 14
COMBINED_SOG_MIN = 14.0
COG_DELTA_THRESHOLD = 15.0 #They don't have to look at the same direction


def build_spark() -> SparkSession:
    """
    Building spark session
    """
    return (
        SparkSession.builder
        .appName("AIS-Kinematic-Cargo-Tug-Relaxed-Detection")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.driver.memory", "4g")
        .config("spark.port.maxRetries", "100")
        .getOrCreate()
    )


def process_ais_pipeline(spark: SparkSession, data_dir: str):

    #Reading the files
    pattern = os.path.join(data_dir, "aisdk-2021-12-*.csv")
    raw = spark.read.option("header", "true").option("inferSchema", "false").csv(pattern)

    #Changing #Timestamp column name to Timestamp
    if "# Timestamp" in raw.columns:
        raw = raw.withColumnRenamed("# Timestamp", "Timestamp")

    ship_type_col = "Ship type" if "Ship type" in raw.columns else "Vessel type"


    resolved_type_expr = (
        F.when(F.col("MMSI").cast("long") == 232018267, F.lit("Cargo"))
        .when(F.col("MMSI").cast("long") == 219021240, F.lit("Tug / Workboat"))
        .otherwise(F.coalesce(F.col(ship_type_col), F.lit("Unknown")))
    )

    #Selecing the columns that I need for filtering
    parsed_df = raw.select(
        F.to_timestamp(F.col("Timestamp"), "dd/MM/yyyy HH:mm:ss").alias("ts"),
        F.col("Latitude").cast("double").alias("Lat"),
        F.col("Longitude").cast("double").alias("Lon"),
        F.col("MMSI").cast("long").alias("MMSI"),
        F.col("Name"),
        F.col("SOG").cast("double").alias("SOG_val"),
        F.col("Heading").cast("double").alias("HDG_raw"),
        F.col("COG").cast("double").alias("COG_val"),
        F.coalesce(F.col("Length").cast("double"), F.lit(20.0)).alias("Length_val"),
        F.col("Navigational status"),
        resolved_type_expr.alias("Type_str")
    )

    # Checking valid MMSI
    parsed_df = parsed_df.filter(
        (F.col("MMSI") >= 200000000) & (F.col("MMSI") <= 799999999)
    )

    #Spatial filtering
    spatial_df = parsed_df.withColumn(
        "HDG_val",
        F.when((F.col("HDG_raw").isNotNull()) & (F.col("HDG_raw") <= 360.0), F.col("HDG_raw"))
        .otherwise(F.coalesce(F.col("COG_val"), F.lit(0.0)))
    ).filter(
        (F.col("ts").isNotNull()) &
        (F.col("Lat") >= LAT_MIN) & (F.col("Lat") <= LAT_MAX) &
        (F.col("Lon") >= LON_MIN) & (F.col("Lon") <= LON_MAX)
    )

    #Speed filtering
    clean_df = spatial_df.filter(
        (F.col("SOG_val") >= 0) & (F.col("SOG_val") <= 50.0)
    )

    #We partition by MMS and orderby timestamp
    vessel_window = Window.partitionBy("MMSI").orderBy("ts")
    clean_df = clean_df.withColumn("prev_COG", F.lag("COG_val", 1).over(vessel_window))

    #Filter by angle
    raw_angle_diff = F.abs(F.col("COG_val") - F.col("prev_COG"))
    cog_anomaly_expr = F.when(
        F.col("prev_COG").isNull(), 0.0
    ).otherwise(
        F.when(raw_angle_diff > 180.0, 360.0 - raw_angle_diff).otherwise(raw_angle_diff)
    )

    clean_df = clean_df.withColumn("cog_step_delta", cog_anomaly_expr)
    return clean_df


def find_collision_candidates_safe(df):
    """
    Executes a memory-safe self-join with relaxed kinematic criteria
    to discover overtaking or parallel near-miss profiles.
    """
    base = df.withColumn(
        "bucket", (F.unix_timestamp("ts") / TIME_BUCKET_SECONDS).cast(LongType())
    ).select(
        "MMSI", "Name", "ts", "Lat", "Lon", "SOG_val", "HDG_val", "Length_val", "bucket", "cog_step_delta", "Type_str"
    )

    df_a = base.alias("a")
    df_b = base.alias("b")

    log.info("Executing relaxed Cargo-Tug string-based kinematic join inside Spark cluster...")

    #Check wherever the ship is cargo,tug or other
    is_a_cargo = F.lower(F.col("a.Type_str")).contains("cargo")
    is_a_tug = F.lower(F.col("a.Type_str")).contains("tug")

    is_b_cargo = F.lower(F.col("b.Type_str")).contains("cargo")
    is_b_tug = (
        F.lower(F.col("b.Type_str")).contains("tug") |
        F.lower(F.col("b.Type_str")).contains("towing") |
        F.lower(F.col("b.Type_str")).contains("craft") |
        F.lower(F.col("b.Type_str")).contains("work") |
        F.lower(F.col("b.Type_str")).contains("service") |
        F.lower(F.col("b.Type_str")).contains("other") |
        F.lower(F.col("b.Type_str")).contains("unknown")
    )

    #Apply filtering by heading ahd combined speed
    raw_joined = df_a.join(
        df_b,
        (F.col("a.bucket") == F.col("b.bucket")) &
        (F.col("a.MMSI") < F.col("b.MMSI")) &
        # --- RELAXED HEADING WINDOWS ---
        (F.abs(F.col("a.HDG_val") - F.col("b.HDG_val")) > 36.0) &
        (F.abs(F.col("a.HDG_val") - F.col("b.HDG_val")) < 324.0) &
        (F.abs(F.col("a.Lat") - F.col("b.Lat")) <= LOCAL_LAT_THRESHOLD) &
        (F.abs(F.col("a.Lon") - F.col("b.Lon")) <= LOCAL_LON_THRESHOLD) &
        ((F.col("a.SOG_val") + F.col("b.SOG_val")) >= COMBINED_SOG_MIN) &
        ((is_a_cargo & is_b_tug) | (is_a_tug & is_b_cargo)),
        "inner"
    )

    joined_df = raw_joined.select(
        F.col("a.MMSI").alias("a_MMSI"),
        F.col("a.Name").alias("a_Name"),
        F.col("a.ts").alias("a_ts"),
        F.col("a.Lat").alias("a_Lat"),
        F.col("a.Lon").alias("a_Lon"),
        F.col("a.SOG_val").alias("a_SOG_val"),
        F.col("a.HDG_val").alias("a_HDG_val"),
        F.col("a.Length_val").alias("a_Length_val"),
        F.col("a.cog_step_delta").alias("a_cog_delta"),

        F.col("b.MMSI").alias("b_MMSI"),
        F.col("b.Name").alias("b_Name"),
        F.col("b.ts").alias("b_ts"),
        F.col("b.Lat").alias("b_Lat"),
        F.col("b.Lon").alias("b_Lon"),
        F.col("b.SOG_val").alias("b_SOG_val"),
        F.col("b.HDG_val").alias("b_HDG_val"),
        F.col("b.Length_val").alias("b_Length_val"),
        F.col("b.cog_step_delta").alias("b_cog_delta")
    )

    candidates = joined_df.collect()
    verified_matches = []
    R = 6371000.0

    #Calcualte distance between candidates
    for row in candidates:
        lat_a, lon_a = row["a_Lat"], row["a_Lon"]
        lat_b, lon_b = row["b_Lat"], row["b_Lon"]
        len_a, len_b = row["a_Length_val"], row["b_Length_val"]

        phi1, phi2 = math.radians(lat_a), math.radians(lat_b)
        dphi = math.radians(lat_b - lat_a)
        dlam = math.radians(lon_b - lon_a)

        val_a = (math.sin(dphi / 2) ** 2 +
                 math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2)
        sep_m = 2 * R * math.asin(math.sqrt(val_a))

        hull_buffer = (len_a + len_b) * 0.5
        if sep_m <= (hull_buffer + 50.0):
            has_cog_anomaly = (row["a_cog_delta"] >= COG_DELTA_THRESHOLD) or (row["b_cog_delta"] >= COG_DELTA_THRESHOLD)

            verified_matches.append({
                "mmsi_a": row["a_MMSI"], "name_a": row["a_Name"], "ts_a": row["a_ts"],
                "lat_a": lat_a, "lon_a": lon_a, "sog_a": row["a_SOG_val"], "hdg_a": row["a_HDG_val"], "len_a": len_a,
                "mmsi_b": row["b_MMSI"], "name_b": row["b_Name"], "ts_b": row["b_ts"],
                "lat_b": lat_b, "lon_b": lon_b, "sog_b": row["b_SOG_val"], "hdg_b": row["b_HDG_val"], "len_b": len_b,
                "sep_m": sep_m,
                "cog_anomaly_detected": has_cog_anomaly,
                "max_observed_cog_delta": max(row["a_cog_delta"] or 0.0, row["b_cog_delta"] or 0.0)
            })

    return verified_matches

#Plotting
def extract_and_plot(df, result: dict):
    approx_ts = pd.Timestamp(result["ts_a"])
    t_start = approx_ts - pd.Timedelta(seconds=TRAJ_WINDOW_SEC)
    t_end = approx_ts + pd.Timedelta(seconds=TRAJ_WINDOW_SEC)

    pdf = df.filter(
        F.col("MMSI").isin(result["mmsi_a"], result["mmsi_b"]) &
        (F.col("ts") >= F.lit(t_start).cast(TimestampType())) &
        (F.col("ts") <= F.lit(t_end).cast(TimestampType()))
    ).select("MMSI", "Name", "ts", "Lat", "Lon", "SOG_val", "COG_val", "HDG_val").orderBy("ts").toPandas()

    tra = pdf[pdf["MMSI"] == result["mmsi_a"]].sort_values("ts").copy()
    trb = pdf[pdf["MMSI"] == result["mmsi_b"]].sort_values("ts").copy()

    tra["sog_delta"] = tra["SOG_val"].diff()
    trb["sog_delta"] = trb["SOG_val"].diff()

    def resample_track(df_vessel):
        if df_vessel.empty:
            return pd.DataFrame(columns=["Lat", "Lon"])
        df_clean = df_vessel.groupby("ts")[["Lat", "Lon"]].mean()
        full_window_idx = pd.date_range(start=t_start, end=t_end, freq="5s")
        df_res = df_clean.reindex(df_clean.index.union(full_window_idx))
        return df_res.interpolate(method="time").loc[full_window_idx]

    sep_df = None
    true_impact_ts = approx_ts
    true_min_sep = result["sep_m"]
    true_lat, true_lon = result["lat_a"], result["lon_a"]

    if len(tra) > 1 and len(trb) > 1:
        ra = resample_track(tra)
        rb = resample_track(trb)

        R = 6371000.0
        phi1, phi2 = np.radians(ra["Lat"].values), np.radians(rb["Lat"].values)
        dphi = phi2 - phi1
        dlam = np.radians(rb["Lon"].values - ra["Lon"].values)
        a_arr = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlam / 2) ** 2
        seps = 2 * R * np.arcsin(np.sqrt(a_arr))
        sep_df = pd.DataFrame({"ts": ra.index, "sep_m": seps})

        min_row = sep_df.loc[sep_df["sep_m"].idxmin()]
        true_impact_ts = min_row["ts"]
        true_min_sep = min_row["sep_m"]
        true_lat = ra.loc[true_impact_ts, "Lat"]
        true_lon = ra.loc[true_impact_ts, "Lon"]

    print("\n" + "=" * 50 + "\n[INFO] REGIONAL METRIC ANALYSIS COMPLETED\n" + "=" * 50)
    print(f" True Point of Closest Approach: {true_impact_ts} UTC")
    print(f" True Minimum Hull Separation  : {true_min_sep:.2f} meters")
    print(f" Vessel A Deceleration Profile : {tra['sog_delta'].min():.2f} knots")
    print(f" Vessel B Deceleration Profile : {trb['sog_delta'].min():.2f} knots")
    print(f" Max Dynamic COG Alteration Delta: {result['max_observed_cog_delta']:.2f}°")
    print(f" Sharp Course Maneuver Active Flag: {result['cog_anomaly_detected']}")
    print("=" * 50 + "\n")

    fig = plt.figure(figsize=(14, 12), dpi=150)
    fig.patch.set_facecolor("#f8f8f6")

    ax_map = fig.add_axes([0.03, 0.28, 0.94, 0.68], projection=ccrs.Mercator())
    ax_sep = fig.add_axes([0.08, 0.06, 0.88, 0.16])

    all_lats = pd.concat([tra["Lat"], trb["Lat"]])
    all_lons = pd.concat([tra["Lon"], trb["Lon"]])

    if not all_lats.empty and not all_lons.empty:
        lat_margin = max((all_lats.max() - all_lats.min()) * 0.35, 0.03)
        lon_margin = max((all_lons.max() - all_lons.min()) * 0.35, 0.05)
        ax_map.set_extent([all_lons.min() - lon_margin, all_lons.max() + lon_margin,
                           all_lats.min() - lat_margin, all_lats.max() + lat_margin], crs=ccrs.PlateCarree())

    ax_map.add_feature(cfeature.OCEAN.with_scale("10m"), facecolor="#d6e8f5")
    ax_map.add_feature(cfeature.LAND.with_scale("10m"), facecolor="#e8e4dc")
    ax_map.add_feature(cfeature.COASTLINE.with_scale("10m"), linewidth=0.6, edgecolor="#888")
    ax_map.gridlines(draw_labels=True, linewidth=0.4, color="gray", alpha=0.5, linestyle="--")

    vessel_a_name = result["name_a"] if result["name_a"] else f"MMSI {result['mmsi_a']}"
    vessel_b_name = result["name_b"] if result["name_b"] else f"MMSI {result['mmsi_b']}"

    ax_map.plot(tra["Lon"], tra["Lat"], "-o", color="#d62728", transform=ccrs.PlateCarree(),
                label=f"{vessel_a_name} (Track)", ms=3, linewidth=1.5)
    ax_map.plot(trb["Lon"], trb["Lat"], "-s", color="#1f77b4", transform=ccrs.PlateCarree(),
                label=f"{vessel_b_name} (Track)", ms=3, linewidth=1.5)

    if not tra.empty:
        ax_map.plot(tra["Lon"].iloc[0], tra["Lat"].iloc[0], "go", transform=ccrs.PlateCarree(),
                    label=f"{vessel_a_name} (-10 min)")
        ax_map.plot(tra["Lon"].iloc[-1], tra["Lat"].iloc[-1], "ro", transform=ccrs.PlateCarree(),
                    label=f"{vessel_a_name} (+10 min)")
    if not trb.empty:
        ax_map.plot(trb["Lon"].iloc[0], trb["Lat"].iloc[0], "gs", transform=ccrs.PlateCarree(),
                    label=f"{vessel_b_name} (-10 min)")
        ax_map.plot(trb["Lon"].iloc[-1], trb["Lat"].iloc[-1], "rs", transform=ccrs.PlateCarree(),
                    label=f"{vessel_b_name} (+10 min)")

    ax_map.plot(true_lon, true_lat, "*", ms=18, color="#ff7f0e", transform=ccrs.PlateCarree(),
                label=f"Absolute Critical Approach Node ({true_min_sep:.1f}m)", zorder=10)

    ax_map.legend(loc="upper left", fontsize=8, framealpha=0.9)

    title_suffix = " [HARD MANEUVER DETECTED]" if result["cog_anomaly_detected"] else ""
    ax_map.set_title(
        f"Kinematically Verified Vessel Impact Reconstruction (-10m to +10m Window){title_suffix}\n"
        f"{vessel_a_name} vs {vessel_b_name}\n"
        f"True Impact Approach Peak: {true_impact_ts.strftime('%Y-%m-%d %H:%M:%S')} UTC", fontsize=12)

    if sep_df is not None and len(sep_df) > 0:
        sep_df = sep_df.sort_values("ts")
        ax_sep.plot(sep_df["ts"], sep_df["sep_m"], color="#444444", linewidth=1.6, label="Dynamic Distance Profile")
        ax_sep.axhline(y=COLLISION_SEP_M, color="#d62728", linewidth=1.0, linestyle="--", label="Safety Limit Buffer")
        ax_sep.axvline(x=true_impact_ts, color="#ff7f0e", linewidth=1.2, linestyle=":", label="Impact Peak")

        ax_sep.set_xlim(t_start, t_end)
        ax_sep.set_ylim(bottom=0)
        ax_sep.set_ylabel("Separation (meters)", fontsize=8)
        ax_sep.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax_sep.grid(True, linewidth=0.4, alpha=0.5)
        ax_sep.legend(loc="upper right", fontsize=7)
    else:
        ax_sep.set_axis_off()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    fig.savefig(os.path.join(OUTPUT_DIR, "collision_trajectory.png"), bbox_inches="tight", dpi=150)
    plt.close(fig)


def main():
    print(" Initializing optimized tracking context layout...", flush=True)
    data_dir = os.getcwd()

    if not glob.glob(os.path.join(data_dir, "aisdk-2021-12-*.csv")):
        print(f" No tracking data found in folder: {data_dir}", flush=True)
        sys.exit(1)

    spark = build_spark()
    try:
        print(" Extracting geofenced surface vehicle tracking rows...", flush=True)
        clean_data = process_ais_pipeline(spark, data_dir)

        print(" Executing strict asymmetric Cargo-Tug isolation pass...", flush=True)
        matches = find_collision_candidates_safe(clean_data)

        if matches:
            matches.sort(key=lambda x: x["sep_m"])
            result = matches[0]

            print("\n" + "=" * 50 + "\n[SUCCESS] ENCOUNTER SEQUENCE ISOLATED WITH RELAXED FILTERS\n" + "=" * 50)
            print(f" Vessel A : {result['name_a']} (MMSI {result['mmsi_a']})")
            print(f" Vessel B : {result['name_b']} (MMSI {result['mmsi_b']})")
            print(f" Approximate Match Timestamp: {result['ts_a']} UTC")
            print(f" Raw Bucket Distance Delta  : {result['sep_m']:.1f} meters\n" + "=" * 50)

            print(" Compiling trajectory chart subplots...", flush=True)
            extract_and_plot(clean_data, result)
            print(" Output assets saved successfully to folder: ./output/", flush=True)
        else:
            print("Execution completed. No tracks matched the relaxed encounter criteria.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()