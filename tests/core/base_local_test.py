import unittest

from dotenv import load_dotenv
from fsspec.implementations.local import LocalFileSystem
# from pyarrow.fs import LocalFileSystem

from cloud.local.local import LocalFileSystemStorage


class LocalFilesystemTestBase(unittest.TestCase):

    _local_filesystem_storage = None
    _filesystem: LocalFileSystem = LocalFileSystem()

    @classmethod
    def setUpClass(cls):
        load_dotenv()

        # Create the writer object
        cls._local_filesystem_storage = LocalFileSystemStorage()
