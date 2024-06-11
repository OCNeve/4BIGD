from dataset_downloader import IMDBDatasetDownloader
from hdfs_manager import HDFSManager
from pipeline import IMDbDataPipeline
from data_validator import DataValidator 


hdfs_base_dir = "/imdb" 

imdb_tsv_local_path = "name.basics.tsv"
hdfs_imdb_path = f"{hdfs_base_dir}/name.basics"
mongo_uri = f"mongodb://mongodb:27017/mydb"

imdb_dataset_url = "https://datasets.imdbws.com/name.basics.tsv.gz"
imdb_gz_file_name = "name.basics.tsv.gz"

if __name__ == "__main__":

    downloader = IMDBDatasetDownloader(imdb_dataset_url, imdb_gz_file_name, imdb_tsv_local_path)
    downloader.download_and_decompress()

    hdfs_url = "http://hadoop-master:50070"
    hdfs_user = "root"

    manager = HDFSManager(hdfs_url, hdfs_user)
    manager.create_directory("/user/root/imdb")
    manager.list_directory("/user/root/imdb")

    pipeline = IMDbDataPipeline(imdb_tsv_local_path, hdfs_imdb_path, mongo_uri, "mydb", "name_basics")
    pipeline.run()

    validator = DataValidator(pipeline.spark, hdfs_imdb_path, mongo_uri, pipeline.mongo_collection)  # Pass Spark session and MongoDB URI

    validator.validate_data()

    spark.stop()
