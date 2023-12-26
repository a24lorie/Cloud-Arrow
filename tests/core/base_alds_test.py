import os
import unittest

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeDirectoryClient, FileSystemClient
from dotenv import load_dotenv

from cloud.adls import ADLSStorage


class ADLSTestBase(unittest.TestCase):

    _container_name = None
    _storage_account_name = None
    _client_secret = None
    _client_id = None
    _tenant_id = None
    _credentials = None
    _adls_object_storage = None

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

    def __get_azure_account_url(self, storage_account: str):
        """

        :param storage_account:
        :return:
        """

        """
            How do you delete a file from an Azure Data Lake using the Python SDK?
                https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-directory-file-acl-python?tabs=azure-ad
                https://stackoverflow.com/questions/63475269/how-do-you-delete-a-file-from-an-azure-data-lake-using-the-python-sdk
        """

        return f"https://{storage_account}.dfs.core.windows.net"

    def _get_filesystem_client(self, storage_account: str, container: str):
        """

        :param storage_account:
        :param container:
        :param path:
        :return:
        """

        return FileSystemClient(
           account_url=self.__get_azure_account_url(storage_account),
           file_system_name=container,
           credential=self._credentials
        )

    def _get_DataLakeDirectoryClient(self, storage_account: str, container: str, path: str):
        """
        :param storage_account:
        :param container:
        :param path:
        :return:
        """
        return DataLakeDirectoryClient(
            account_url=self.__get_azure_account_url(storage_account),
            file_system_name=container,
            directory_name=path,
            credential=self._credentials
        )

    def _delete_adls_dir(self, storage_account: str, container: str, path: str):
        """

        :param storage_account:
        :param container:
        :param path:
        :return:
        """

        directory_client = self._get_DataLakeDirectoryClient(
           storage_account=storage_account,
           container=container,
           path=path
        )
        directory_client.delete_directory()
