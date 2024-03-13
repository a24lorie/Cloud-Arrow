import os
import unittest

from adlfs import AzureBlobFileSystem
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeDirectoryClient, FileSystemClient
from dotenv import load_dotenv

from cloud.adls import ADLSStorage
from tests.core.base_test import TestBase


class ADLSTestBase(TestBase):

    _container_name = None
    _storage_account_name = None
    _client_secret = None
    _client_id = None
    _tenant_id = None
    _credentials = None
    _adls_object_storage = None
    _filesystem = None
    _base_path = None
    _test_table = None
    _fixed_table = None


    @classmethod
    def setUpClass(cls):
        load_dotenv()

        cls._tenant_id = os.getenv("TENANT_ID")
        cls._client_id = os.getenv("CLIENT_ID")
        cls._client_secret = os.getenv("CLIENT_SECRET")
        cls._storage_account_name = os.getenv("STORAGE_ACCOUNT")
        cls._container_name = os.getenv("CONTAINER_NAME")

        cls._credentials = ClientSecretCredential(
            tenant_id=cls._tenant_id,
            client_id=cls._client_id,
            client_secret=cls._client_secret
        )

        # Create the writer object
        cls._adls_object_storage = ADLSStorage(
            tenant_id=cls._tenant_id,
            client_id=cls._client_id,
            client_secret=cls._client_secret,
            account_name=cls._storage_account_name,
            container=cls._container_name
        )

        cls._filesystem = AzureBlobFileSystem(
            account_name=cls._storage_account_name,
            tenant_id=cls._tenant_id,
            client_id=cls._client_id,
            client_secret=cls._client_secret
        )

        cls._base_path = "write"
        cls._test_table = cls.make_mock_diabetes_arrow_table(random=True)
        cls._fixed_table = cls.make_mock_diabetes_arrow_table(random=False)

    @classmethod
    def __get_azure_account_url(cls):
        """

        :return:
        """

        """
            How do you delete a file from an Azure Data Lake using the Python SDK?
                https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-directory-file-acl-python?tabs=azure-ad
                https://stackoverflow.com/questions/63475269/how-do-you-delete-a-file-from-an-azure-data-lake-using-the-python-sdk
        """

        return f"https://{cls._storage_account_name}.dfs.core.windows.net"

    @classmethod
    def _get_filesystem_client(cls):
        """

        :return:
        """

        return FileSystemClient(
           account_url=cls.__get_azure_account_url(),
           file_system_name=cls._container_name,
           credential=cls._credentials
        )

    @classmethod
    def _get_DataLakeDirectoryClient(cls, path: str):
        """
        :param path:
        :return:
        """
        return DataLakeDirectoryClient(
            account_url=cls.__get_azure_account_url(),
            file_system_name=cls._container_name,
            directory_name=path,
            credential=cls._credentials
        )

    @classmethod
    def _delete_adls_dir(cls, path: str):
        """

        :param path:
        :return:
        """

        cls._get_DataLakeDirectoryClient(path=path).delete_directory()
