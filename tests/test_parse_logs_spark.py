import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from spark_utils import create_spark_session
from parse_logs_spark import parse_logs


def test_parse_logs_basic():
    spark = create_spark_session('TestSession')
    df = parse_logs(spark, 'access_log_sample.txt')
    first = df.first()
    spark.stop()
    assert first['client_ip'] == '10.223.157.186'
    assert first['status_code'] == '403'
