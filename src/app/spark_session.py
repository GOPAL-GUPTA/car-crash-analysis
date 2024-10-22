from pyspark.sql import SparkSession

def create_spark_session(app_name="CarCrashAnalysis",master="local[*]"):
    """
    Creates and returns a SparkSession.
    Args:
        app_name (str): Name of the Spark application.
        master (str): Default is local with all available cores.
    Returns:
        SparkSession: Configured Spark session.
    """
    spark = SparkSession.builder.\
        appName(app_name).\
        master(master).\
        getOrCreate()

    return spark
    