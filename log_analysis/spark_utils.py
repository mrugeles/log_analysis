from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "LogParser") -> SparkSession:
    """Create and return a SparkSession with the given app name."""
    return SparkSession.builder.appName(app_name).getOrCreate()
