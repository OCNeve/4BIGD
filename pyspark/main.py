from dataset_downloader import IMDBDatasetDownloader
from hdfs_manager import HDFSManager
from pipeline import IMDbDataPipeline


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

