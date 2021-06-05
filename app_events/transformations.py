"""app_events.transformations module."""
import pyspark.sql.functions as F
from pyspark.sql.session import DataFrame, SparkSession
from pyspark.sql.window import Window


def prepare_events(source_df: DataFrame) -> DataFrame:
    """Prepares source_df to be manipulated.

    :param source_df: Source DataFrame
    :return: Prepared DataFrame
    """
    casted = source_df\
        .withColumn('timestamp', F.col('timestamp').cast('timestamp'))

    latests = casted\
        .withColumn('rn1',
            F.row_number().over(
                Window\
                    .partitionBy('initiator_id', 'event')\
                    .orderBy(F.col('timestamp').desc())
            )
        )\
        .filter('rn1 = 1').drop('rn1')

    return latests\
        .withColumn("week_end", F.next_day("timestamp", "SUN"))\
        .withColumn("year_week", F.weekofyear('week_end')).drop('week_end')


def get_event_partition(spark: SparkSession, df_path: str, event: str) -> DataFrame:
    """Extracts DataFrame's event partition.

    :param df_path: Path for the DataFrame
    :param event: event partition
    :return: Partitioned DataFrame
    """
    return spark.read\
        .parquet(f'{df_path}/event={event}')\
        .select('year_week', 'initiator_id')


def join_tables(registered: DataFrame, app_loaded: DataFrame) -> DataFrame:
    """Inner Joins tables based on id.

    :param registered: `registered` events
    :param app_loaded: `app_loaded` events
    :return: Joined DataFrame
    """
    joinable_registered = registered\
        .select('initiator_id', 'year_week')\
        .withColumnRenamed('initiator_id', 'id')\
        .withColumnRenamed('year_week', 'registered_at')

    joinable_app_loaded = app_loaded\
        .select('initiator_id', 'year_week')\
        .withColumnRenamed('initiator_id', 'id')\
        .withColumnRenamed('year_week', 'app_loaded_at')

    return joinable_registered\
        .join(joinable_app_loaded, on='id', how='inner')\
        .drop(joinable_app_loaded.id)


def add_activity(joined_df: DataFrame) -> DataFrame:
    """Adds `is_active` column to Joined DataFrame.

    :param joined_df: Joined DataFrame
    :return: Joined DataFrame enriched with activity
    """
    return joined_df\
        .withColumn('is_active', F.col('app_loaded_at') > F.col('registered_at'))\
        .drop('app_loaded_at').drop('registered_at').cache()


def uniques_activity(activity_df: DataFrame) -> DataFrame:
    """Creates DataFrame based on unique ids per `is_active`.

    :param activity_df: Activity DataFrame
    :return: Unique Ids Activity DataFrame
    """
    return activity_df\
        .groupBy('is_active')\
        .agg(F.countDistinct('id').alias('unique_ids'))


def add_percent(uniques_df: DataFrame):
    """Adds `percent` column to Joined DataFrame.

    :param uniques_df: Unique Ids Activity DataFrame
    :return: Unique Ids Activity DataFrame with added `percent` column
    """
    total_unique_ids = F.sum('unique_ids').over(Window.partitionBy())

    return uniques_df\
        .withColumn('percent', F.col('unique_ids') / total_unique_ids)
