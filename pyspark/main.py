from dataset_downloader import IMDBDatasetDownloader
from hdfs_manager import HDFSManager
from pipeline import IMDbDataPipeline
from data_validator import DataValidator  # Import the DataValidator class

# Get values from environment variables
hdfs_base_dir = "/imdb"  # Base directory in HDFS to store IMDb data

# Paths and URIs
imdb_tsv_local_path = "name.basics.tsv"
hdfs_imdb_path = f"{hdfs_base_dir}/name.basics"
mongo_uri = f"mongodb://mongodb:27017/mydb"

# Dataset download details
imdb_dataset_url = "https://datasets.imdbws.com/name.basics.tsv.gz"
imdb_gz_file_name = "name.basics.tsv.gz"

if __name__ == "__main__":
    # Step 1: Download and decompress the IMDb dataset
    downloader = IMDBDatasetDownloader(imdb_dataset_url, imdb_gz_file_name, imdb_tsv_local_path)
    downloader.download_and_decompress()

    # Step 2: Create HDFS directories
    hdfs_url = "http://hadoop-master:50070"
    hdfs_user = "root"

    manager = HDFSManager(hdfs_url, hdfs_user)
    manager.create_directory("/user/root/imdb")
    manager.list_directory("/user/root/imdb")

    # Step 3: Run the data pipeline
    pipeline = IMDbDataPipeline(imdb_tsv_local_path, hdfs_imdb_path, mongo_uri, "mydb", "name_basics")
    pipeline.run()

    # Step 4: Data Validation
    spark = pipeline.spark  # Get the SparkSession object from the pipeline

    validator = DataValidator(spark, hdfs_imdb_path, mongo_uri, "mydb", "name_basics")

    # Validate data in HDFS
    hdfs_validation_result = validator.validate_hdfs_data()
    hdfs_validation_result.show()

    # Validate data in MongoDB
    mongo_validation_result = validator.validate_mongodb_data()
    mongo_validation_result.show()

    # Compare data between HDFS and MongoDB
    comparison_result = validator.compare_data()
    comparison_result.show()
