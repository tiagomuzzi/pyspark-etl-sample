"""schemas module."""
from pyspark.sql.types import (
    StructType, StructField, TimestampType, LongType, StringType)


generic_event_schema = StructType([
    StructField('event', StringType(), False),
    StructField('timestamp', TimestampType(), False),
    StructField('initiator_id', LongType(), False),
    StructField('channel', StringType(), True),
    StructField('device_type', StringType(), True)
])
