"""main module."""
from typing import Dict
import json

from pyspark.sql import SparkSession
import pipeline


def create_spark_session(job_name: str) -> SparkSession:
    """Gets or Creates SparkSession.

    :param job_name: Job name
    :return: SparkSession
    """
    return (SparkSession
        .builder
        .appName(job_name)
        .getOrCreate())


def load_config_file(file_name: str) -> Dict:
    """Reads the configs/config.json file and parse as a dictionary.

    :param file_name: name of the config file
    :return: Config Mapping
    """
    with open(file_name) as file_stream:
        conf: Dict = json.load(file_stream)
    return conf

if __name__ == '__main__':
    spark = create_spark_session('userActivity')
    file_paths = load_config_file('configs/file_paths.json')

    pipeline.run(spark=spark, file_paths=file_paths)
    spark.stop()
