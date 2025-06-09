import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

# Input file path can be passed as first command line argument
log_file = sys.argv[1] if len(sys.argv) > 1 else 'access_log_sample.txt'

spark = SparkSession.builder.appName('LogParser').getOrCreate()

# Regular expression pattern for CLF
pattern = r'^(\S+) (\S+) (\S+) \[([^\]]+)\] "([^"]*)" (\d{3}) (\S+)' 

# Read the log file as text
lines_df = spark.read.text(log_file)

# Extract fields using regexp_extract
logs_df = lines_df.select(
    regexp_extract('value', pattern, 1).alias('client_ip'),
    regexp_extract('value', pattern, 2).alias('client_identity'),
    regexp_extract('value', pattern, 3).alias('client_username'),
    regexp_extract('value', pattern, 4).alias('datetime'),
    regexp_extract('value', pattern, 5).alias('request'),
    regexp_extract('value', pattern, 6).alias('status_code'),
    regexp_extract('value', pattern, 7).alias('size')
)

logs_df.show(truncate=False)

spark.stop()
