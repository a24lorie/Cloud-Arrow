import logging
from abc import ABCMeta
from typing import Any

from adlfs import AzureBlobFileSystem

from ..core import AbstractStorage


class ADLSStorage(AbstractStorage, metaclass=ABCMeta):

    def __init__(self,
                 tenant_id: str,
                 client_id: str,
                 client_secret: str,
                 account_name: str,
                 container: str):

        """
        :param tenant_id:
        :param client_id:
        :param client_secret:
        :param account_name:
        :param container:
        """

        super().__init__()
        self._logger = logging.getLogger('cloud_arrow.adlsReader')

        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._account_name = account_name
        self._container = container

    def _get_filesystem(self) -> Any:
        return AzureBlobFileSystem(
            account_name=self._account_name,
            tenant_id=self._tenant_id,
            client_id=self._client_id,
            client_secret=self._client_secret
        )

    def _get_filesystem_base_path(self, path):
        return f"{self._container}/{AbstractStorage._normalize_path(path)}"

    def _get_deltalake_storage_options(self):
        """
           For delta-io documentation see: https://delta-io.github.io/delta-rs/python/usage.html#querying-delta-tables
           For Azure Options see:https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants
           For Available AuthProvider see:
            https://github.com/delta-io/delta-rs/blob/7090a1260fab0efc6804764559688f7766439b4f/crates/deltalake-core/src/data_catalog/unity/credential.rs#L79

           example:
               storage_options = {"azure_storage_account_name": f"{self._account_name}", "azure_storage_access_key": "..."}
       """

        return {
            "azure_storage_account_name": f"{self._account_name}",
            "tenant_id": f"{self._tenant_id}",
            "client_id": f"{self._client_id}",
            "client_secret": f"{self._client_secret}"
        }

    def _get_deltalake_url(self, path) -> str:
        return f"abfss://{self._container}@{self._account_name}.dfs.core.windows.net/{AbstractStorage._normalize_path(path)}"