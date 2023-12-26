import unittest

from azure.storage.filedatalake import DataLakeDirectoryClient, FileSystemClient
from dotenv import load_dotenv
from pyarrow.filesystem import LocalFileSystem

from cloud.local.local import LocalFileSystemStorage


class LocalFilesystemTestBase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        load_dotenv()

        # Create the writer object
        cls._local_filesystem_storage = LocalFileSystemStorage()

    def _delete_dir(self, path: str):
        """

        :param path:
        :return:
        """

        filesystem = LocalFileSystem()
        filesystem.delete(path=path, recursive=True)
