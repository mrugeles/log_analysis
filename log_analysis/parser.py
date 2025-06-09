import re
from pyspark.sql.functions import regexp_extract

# Regular expression pattern for CLF
PATTERN = r'^(\S+) (\S+) (\S+) \[([^\]]+)\] "([^"]*)" (\d{3}) (\S+)'

def parse_logs(spark, log_file):
    """Return a DataFrame with parsed log fields."""
    lines_df = spark.read.text(log_file)
    return lines_df.select(
        regexp_extract('value', PATTERN, 1).alias('client_ip'),
        regexp_extract('value', PATTERN, 2).alias('client_identity'),
        regexp_extract('value', PATTERN, 3).alias('client_username'),
        regexp_extract('value', PATTERN, 4).alias('datetime'),
        regexp_extract('value', PATTERN, 5).alias('request'),
        regexp_extract('value', PATTERN, 6).alias('status_code'),
        regexp_extract('value', PATTERN, 7).alias('size'),
    )
