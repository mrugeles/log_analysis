import sys
from log_analysis.spark_utils import create_spark_session
from log_analysis import parser


def main(argv=None):
    argv = argv or sys.argv[1:]
    log_file = argv[0] if argv else 'data/access_log_sample.txt'
    spark = create_spark_session('LogParser')
    logs_df = parser.parse_logs(spark, log_file)
    logs_df.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
