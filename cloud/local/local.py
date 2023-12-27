import logging
from abc import ABCMeta
from typing import Any

from pyarrow.fs import LocalFileSystem

from ..core import AbstractStorage


class LocalFileSystemStorage(AbstractStorage, metaclass=ABCMeta):

    def __init__(self):

        """

        """
        super().__init__()
        self._logger = logging.getLogger('cloud_arrow.localFileSystemReader')

    def _get_filesystem(self) -> Any:
        return LocalFileSystem()

    def _get_filesystem_base_path(self, path):
        return AbstractStorage._normalize_path(path)

    def _get_deltalake_storage_options(self):
        return {}

    def _get_deltalake_url(self, path) -> str:
        return self._get_filesystem_base_path(path=path)