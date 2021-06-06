"""pipeline module."""
from typing import Dict, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

from schemas import generic_event_schema


def extract(
    spark: SparkSession,
    file_paths: Dict,
) -> Tuple[DataFrame, DataFrame]:
    """Reads `generic_events` JSON file, creates `app_loading_events` and
    `user_registration_events` datasets, loades them in Parquet format,
    reads them back and return.

    :param spark: Spark session object.
    :param file_paths: File Paths Mapping
    :return: Spark DataFrames.
    """
    generic_events = spark.read.json(
        path=file_paths['generic_events'],
        schema=generic_event_schema)

    app_loading_events = generic_events\
        .filter("event = 'app_loaded'")\
        .select(
            F.col('timestamp').cast('timestamp').alias('time'),
            F.col('initiator_id'),
            F.col('device_type'),
        )\
        .write.mode('overwrite')\
        .parquet(file_paths['app_loading_events'])

    user_registration_events = generic_events\
        .filter("event = 'registered'")\
        .select(
            F.col('timestamp').cast('timestamp').alias('time'),
            F.col('initiator_id'),
            F.col('channel'),
        )\
        .write.mode('overwrite')\
        .parquet(file_paths['user_registration_events'])

    app_loading_events = spark.read.parquet(file_paths['app_loading_events'])
    user_registration_events = spark.read.parquet(file_paths['user_registration_events'])

    return (app_loading_events, user_registration_events)


def transform(
    app_loading_events: DataFrame,
    user_registration_events: DataFrame,
) -> DataFrame:
    """Transform the data for final loading.
    :param app_loading_events: Incremental DataFrame.
    :param user_registration_events: Final DataFrame.
    :return: Transformed DataFrame.
    """

    joinable_registered = user_registration_events\
        .select('initiator_id', 'time')\
        .withColumnRenamed('initiator_id', 'id')\
        .withColumnRenamed('time', 'registered_at')

    joinable_app_loaded = app_loading_events\
        .select('initiator_id', 'time')\
        .withColumnRenamed('initiator_id', 'id')\
        .withColumnRenamed('time', 'app_loaded_at')

    joined = joinable_registered\
        .join(joinable_app_loaded, on='id', how='inner')\
        .drop(joinable_app_loaded.id)

    def sun_year_week(column_name: str):
        return F.weekofyear(F.next_day(column_name, 'SUN'))

    registered_week = sun_year_week('registered_at')
    app_loaded_week = sun_year_week('app_loaded_at')

    transformed = joined\
        .withColumn('is_active', registered_week < app_loaded_week)

    transformed = transformed\
        .groupBy('is_active')\
        .agg(F.countDistinct('id').alias('unique_ids'))

    total_unique_ids = F.sum('unique_ids').over(Window.partitionBy())

    transformed = transformed\
        .withColumn('percent', F.col('unique_ids') / total_unique_ids)

    return transformed


def load(user_activity: DataFrame, file_paths: Dict) -> True:
    """Write data in final destination
    :param user_activity: DataFrame to save.
    :param file_paths: job configuration
    :return: True
    """
    user_activity\
        .write.mode('overwrite')\
        .csv(path=file_paths['output_path'], mode='overwrite')

    return True


def run(spark: SparkSession, file_paths: Dict) -> bool:
    """Entry point to the pipeline.

    :param spark: SparkSession object
    :param file_paths: job configurations and command lines
    :return: True
    """
    app_loading_events, user_registration_events = extract(
        spark=spark,
        file_paths=file_paths)

    user_activity = transform(
        app_loading_events=app_loading_events,
        user_registration_events=user_registration_events)

    load(user_activity=user_activity, file_paths=file_paths)

    return True
