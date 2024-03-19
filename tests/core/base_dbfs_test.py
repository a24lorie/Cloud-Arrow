import os
import unittest

from dotenv import load_dotenv
from fsspec.implementations.dbfs import DatabricksFileSystem

from cloud.dbfs import DBFSStorage
from tests.core.base_test import TestBase


class DBFSTestBase(TestBase):
    _instance = None
    _token = None
    _dbfs_object_storage = None
    _filesystem: DatabricksFileSystem = None

    @classmethod
    def setUpClass(cls):
        load_dotenv()

        cls._instance = f"adb-{os.getenv('DTBK_INSTANCE')}.azuredatabricks.net"
        cls._token = os.getenv("DTBK_TOKEN")

        cls._filesystem = DatabricksFileSystem(
            instance=cls._instance,
            token=cls._token
        )

        cls._dbfs_object_storage = DBFSStorage(
            instance=cls._instance,
            token=cls._token
        )

    @classmethod
    def _delete_dbfs_dir(cls, path: str):
        """

        :param path:
        :return:
        """

        cls._filesystem.rm(path=path, recursive=True)
