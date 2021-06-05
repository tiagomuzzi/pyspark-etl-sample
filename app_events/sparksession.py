"""app_events.sparksession module."""
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .master("local[*]")
         .appName("app_events")
         .getOrCreate())
