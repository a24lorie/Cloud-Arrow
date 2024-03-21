import logging
from abc import ABCMeta
from typing import Any

from adlfs import AzureBlobFileSystem
from fsspec.implementations.dbfs import DatabricksFileSystem

from ..core import AbstractStorage


class DBFSStorage(AbstractStorage, metaclass=ABCMeta):

    def __init__(self,
                 instance: str,
                 token: str):

        """

        Parameters
        ----------
        :param instance: str
            The instance URL of the databricks cluster.
            For example for an Azure databricks cluster, this
            has the form adb-<some-number>.<two digits>.azuredatabricks.net.
        :param  token: str
            Your personal token. Find out more
            here: https://docs.databricks.com/dev-tools/api/latest/authentication.html

        """

        super().__init__()
        self._logger = logging.getLogger(__name__)

        self._instance = instance
        self._token = token


    def _get_filesystem(self) -> Any:
        return DatabricksFileSystem(
            instance=self._instance,
            token=self._token
        )

    def _get_filesystem_base_path(self, path):
        return f"{AbstractStorage._normalize_path(path)}"

    def _get_deltalake_storage_options(self):
        """
           For delta-io documentation see: https://delta-io.github.io/delta-rs/python/usage.html#querying-delta-tables
           For Azure Options see:https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants
           For Available AuthProvider see:
            https://github.com/delta-io/delta-rs/blob/7090a1260fab0efc6804764559688f7766439b4f/crates/deltalake-core/src/data_catalog/unity/credential.rs#L79

           example:
               storage_options = {"azure_storage_account_name": f"{self._account_name}", "azure_storage_access_key": "..."}
       """
        pass

    def _get_deltalake_url(self, path) -> str:
        return self._get_filesystem_base_path(path=path)