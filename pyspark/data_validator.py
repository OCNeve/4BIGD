from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import StringType, IntegerType, ArrayType

class DataValidator:
    def __init__(self, spark: SparkSession, hdfs_imdb_path: str, mongo_uri: str, mongo_collection: str):
        self.spark = spark
        self.mongo_uri = mongo_uri
        self.hdfs_imdb_path = hdfs_imdb_path
        self.mongo_collection = mongo_collection

    def validate_data(self):
        print('loading dataframes form hdfs and mongo')
        hdfs_df = self.load_hdfs_data()
        mongo_df = self.load_mongo_data()
        print('checking data types')
        self.check_data_types(hdfs_df)
        print('checking data integrity')
        self.check_data_integrity(hdfs_df, mongo_df)
        print('comparing data')
        self.compare_data(hdfs_df, mongo_df)
        print('checking for missing values')
        self.check_missing_values(hdfs_df, mongo_df)

    def load_hdfs_data(self):
        return self.spark.read.format("parquet").load(self.hdfs_imdb_path)

    def load_mongo_data(self):
        return self.spark.read.format("mongo").option("uri", self.mongo_uri).option("collection", self.mongo_collection).load()


    def check_data_types(self, df):
        expected_hdfs_data_types = {
            "nconst": StringType(),
            "primaryName": StringType(),
            "birthYear": IntegerType(),
            "deathYear": IntegerType(),
            "primaryProfession": ArrayType(StringType()),
            "knownForTitles": ArrayType(StringType())
        }
        for col_name, expected_type in expected_hdfs_data_types.items():
            actual_type = df.schema[col_name].dataType
            if actual_type != expected_type:
                print(f"Data type mismatch for column '{col_name}' in DataFrame. Expected: {expected_type}, Actual: {actual_type}")

    def check_data_integrity(self, hdfs_df, mongo_df):
        unique_columns = ["nconst"] 
        for col_name in unique_columns:
            unique_count = hdfs_df.select(col_name).distinct().count()
            total_count = hdfs_df.count()
            if unique_count != total_count:
                print(f"Data integrity issue: {col_name} does not contain unique values in HDFS DataFrame.")

        null_columns = ["birthYear", "deathYear"]
        for col_name in null_columns:
            null_count = hdfs_df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                print(f"Data integrity issue: {col_name} contains {null_count} null values in HDFS DataFrame.")

        mongo_unique_columns = ["nconst"]
        for col_name in mongo_unique_columns:
            unique_count = mongo_df.select(col_name).distinct().count()
            total_count = mongo_df.count()
            if unique_count != total_count:
                print(f"Data integrity issue: {col_name} does not contain unique values in MongoDB DataFrame.")

        mongo_null_columns = ["birthYear", "deathYear"]
        for col_name in mongo_null_columns:
            null_count = mongo_df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                print(f"Data integrity issue: {col_name} contains {null_count} null values in MongoDB DataFrame.")
   
    def compare_data(self, hdfs_df, mongo_df):
        hdfs_count = hdfs_df.count()
        mongo_count = mongo_df.count()

        if hdfs_count == mongo_count:
            print("Data counts match between HDFS and MongoDB DataFrames.")
        else:
            print("Data counts do not match between HDFS and MongoDB DataFrames.")

    def check_missing_values(self, hdfs_df, mongo_df):
        hdfs_missing_values = hdfs_df.select([col(c).isNull().alias(c) for c in hdfs_df.columns])
        hdfs_missing_count = hdfs_missing_values.agg(*[count(col(c)).alias(c) for c in hdfs_missing_values.columns])

        mongo_missing_values = mongo_df.select([col(c).isNull().alias(c) for c in mongo_df.columns])
        mongo_missing_count = mongo_missing_values.agg(*[count(col(c)).alias(c) for c in mongo_missing_values.columns])

        print("Missing values in HDFS DataFrame:")
        hdfs_missing_count.show()

        print("Missing values in MongoDB DataFrame:")
        mongo_missing_count.show()
