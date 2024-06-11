from pyspark.sql import SparkSession


class DataValidator:
    def __init__(self, spark, hdfs_path, mongo_uri, mongo_db, mongo_collection):
        self.spark = spark
        self.hdfs_path = hdfs_path
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection

    def validate_hdfs_data(self):
        # Load data from HDFS
        hdfs_df = self.spark.read.parquet(self.hdfs_path)

        # Perform data quality checks
        # Example: Check for missing values
        missing_values = hdfs_df.select([count(when(col(c).isNull(), c)).alias(c) for c in hdfs_df.columns])
        # You can add more data quality checks based on your requirements

        return missing_values

    def validate_mongodb_data(self):
        # Load data from MongoDB
        mongo_df = self.spark.read.format("mongo").load()

        # Perform data quality checks
        # Example: Check for missing values
        missing_values = mongo_df.select([count(when(col(c).isNull(), c)).alias(c) for c in mongo_df.columns])
        # You can add more data quality checks based on your requirements

        return missing_values

    def compare_data(self):
        # Load data from HDFS
        hdfs_df = self.spark.read.parquet(self.hdfs_path)

        # Load data from MongoDB
        mongo_df = self.spark.read.format("mongo").load()

        # Perform comparison
        comparison_result = hdfs_df.subtract(mongo_df)

        return comparison_result
