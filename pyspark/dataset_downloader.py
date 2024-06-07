import requests
import gzip
import shutil

class IMDBDatasetDownloader:
    def __init__(self, url, gz_file_name, tsv_file_name):
        self.url = url
        self.gz_file_name = gz_file_name
        self.tsv_file_name = tsv_file_name

    def download_file(self):
        response = requests.get(self.url, stream=True)
        with open(self.gz_file_name, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded {self.gz_file_name}")

    def decompress_file(self):
        with gzip.open(self.gz_file_name, 'rb') as f_in:
            with open(self.tsv_file_name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"Decompressed {self.gz_file_name} to {self.tsv_file_name}")

    def download_and_decompress(self):
        self.download_file()
        self.decompress_file()

