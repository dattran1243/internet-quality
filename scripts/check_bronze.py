from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("check-bronze")
    .getOrCreate()
)

path = (
    "s3a://telecom-data/"
    "bronze/ripe_atlas/"
    "ping_by_country/"
    "country=DE/"
    "measurement_id=*/"
    "year=2026/"
    "month=09/"
    "day=20/"
    "hour=00/"
    "*.json"
)

print("CHECK PATH:")
print(path)

df = spark.read.json(path)

print("COUNT:")
print(df.count())

df.select(
    "metadata.country_code",
    "metadata.measurement_id",
    "metadata.requested_start_utc",
    "metadata.requested_stop_utc"
).show(
    truncate=False
)

spark.stop()