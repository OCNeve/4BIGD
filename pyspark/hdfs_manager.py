from hdfs import InsecureClient

class HDFSManager:
    def __init__(self, hdfs_url, user):
        self.client = InsecureClient(hdfs_url, user=user)

    def create_directory(self, directory_path):
        if not self.client.status(directory_path, strict=False):
            self.client.makedirs(directory_path)
            print(f"Directory {directory_path} created.")
        else:
            print(f"Directory {directory_path} already exists.")

    def upload_file(self, local_path, hdfs_path):
        self.client.upload(hdfs_path, local_path, overwrite=True)
        print(f"Uploaded {local_path} to {hdfs_path}.")

    def list_directory(self, directory_path):
        files = self.client.list(directory_path)
        print(f"Files in {directory_path}: {files}")
        return files

