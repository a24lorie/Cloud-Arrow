import unittest

from dotenv import load_dotenv
from fsspec.implementations.local import LocalFileSystem
# from pyarrow.fs import LocalFileSystem

from cloud.local.local import LocalFileSystemStorage
from tests.core.base_test import TestBase


class LocalFilesystemTestBase(TestBase):

    _local_filesystem_storage = None
    _filesystem: LocalFileSystem = LocalFileSystem()
    _base_path = None
    _test_table = None

    @classmethod
    def setUpClass(cls):
        load_dotenv()

        # Create the writer object
        cls._local_filesystem_storage = LocalFileSystemStorage()

        cls._base_path = "./write/diabetes"
        cls._test_table = cls.make_mock_diabetes_arrow_table()

    @classmethod
    def tearDownClass(cls):
        # Remove write directory recursively
        cls._filesystem.delete(f"{cls._base_path}", recursive=True)