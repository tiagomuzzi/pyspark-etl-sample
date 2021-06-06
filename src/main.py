from typing import Dict, Tuple, Any
import json

from pyspark.sql import SparkSession
import pipeline


def create_spark_session(job_name: str) -> SparkSession:
    """Create spark session to run the job.

    :param job_name: Job name
    :return: Spark Session
    """
    spark = (SparkSession
        .builder
        .appName(job_name)
        .enableHiveSupport()
        .getOrCreate())

    return spark


def load_config_file(file_name: str) -> Dict:
    """Reads the configs/config.json file and parse as a dictionary.

    :param file_name: name of the config file
    :return: Config Mapping
    """
    try:
        with open(f'{file_name}') as f:
            conf: Dict = json.load(f)
        return conf

    except FileNotFoundError:
        raise FileNotFoundError(f'{file_name} Not found')


if __name__ == '__main__':
    spark = create_spark_session('userActivity')
    file_paths = load_config_file('configs/file_paths.json')

    pipeline.run(spark=spark, file_paths=file_paths)
    spark.stop()