import os
import unittest

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeDirectoryClient, DataLakeFileClient, FileSystemClient
from dotenv import load_dotenv

__all__ = ['ADLSTestBase']

from cloud.adls import ADLSObjectStorage


class ADLSTestBase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(ADLSTestBase, self).__init__(*args, **kwargs)

        load_dotenv()

        self._tenant_id = os.getenv("TENANT_ID")
        self._client_id = os.getenv("CLIENT_ID")
        self._client_secret = os.getenv("CLIENT_SECRET")
        self._storage_account_name = os.getenv("STORAGE_ACCOUNT")
        self._container_name = os.getenv("CONTAINER_NAME")

        self._credentials = ClientSecretCredential(
            tenant_id=self._tenant_id,
            client_id=self._client_id,
            client_secret=self._client_secret
        )

        # Create the writer object
        self._adls_object_storage = ADLSObjectStorage(
            tenant_id=self._tenant_id,
            client_id=self._client_id,
            client_secret=self._client_secret,
            account_name=self._storage_account_name,
            container=self._container_name
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
