import logging
from abc import ABCMeta
from typing import Any

from gcsfs import GCSFileSystem

from ..core import AbstractObjectStorage


class GCSFSObjectStorage(AbstractObjectStorage, metaclass=ABCMeta):

    def __init__(self,
                 project: str,
                 access: str,
                 token: str,
                 bucket: str,
                 default_location: str):

        """
        :param project:
        :param access:
        :param token:
        :param bucket:
        :param default_location:
        """

        super().__init__()
        self._logger = logging.getLogger('cloud_arrow.gcsfsObjectStorage')

        self._project = project
        self._access = access
        self._token = token
        self._bucket = bucket
        self._default_location = default_location

    def _get_filesystem(self) -> Any:
        return GCSFileSystem(
            project=self._project,
            access=self._access,
            token=self._token,
            default_location=self._default_location
        )

    def _get_cloud_path(self, path):
        return f"{self._bucket}/{AbstractObjectStorage._normalize_path(path)}"

    def _get_deltalake_storage_options(self):
        """
                    For delta-io documentation see: https://delta-io.github.io/delta-rs/python/usage.html#querying-delta-tables
                    For GCP Options see: https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html
        """
        return {
            "google_service_account_path": f"{self._token}",
            "bucket": f"{self._bucket}"
        }

    def _get_delta_lake_url(self, path) -> str:
        return f"gs://{self._bucket}/{AbstractObjectStorage._normalize_path(path)}"
