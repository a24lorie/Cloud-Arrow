import os
import unittest

import pandas as pd
import pyarrow.parquet as pq
from adlfs import AzureBlobFileSystem
from azure.identity import ClientSecretCredential
from dotenv import load_dotenv

from cloud.adls import ADLSObjectStorage
from cloud.core import ParquetWriteOptions, DeltaLakeWriteOptions
from tests.cloud.core import ADLSTestBase

if __name__ == '__main__':
    unittest.main()


class TestADLSRead(ADLSTestBase):

    def __init__(self, *args, **kwargs):
        super(TestADLSRead, self).__init__(*args, **kwargs)
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

    def test_adls_read_parquet_nopart_no_compression(self):
        base_path = "test_read_parquet_nopart_no_compression"
        test_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=test_df,
                                        file_format="parquet",
                                        path=base_path,
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="None",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="parquet",
                path=base_path,
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_read_parquet_nopart_snappy(self):
        base_path = "test_read_parquet_nopart_snappy"
        test_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=test_df,
                                        file_format="parquet",
                                        path=base_path,
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="snappy",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="parquet",
                path=base_path,
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_read_parquet_nopart_gzip(self):
        base_path = "test_read_parquet_nopart_gzip"
        test_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=test_df,
                                        file_format="parquet",
                                        path=base_path,
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="gzip",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="parquet",
                path=base_path,
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_read_parquet_nopart_brotli(self):
        base_path = "test_read_parquet_nopart_brotli"
        test_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=test_df,
                                        file_format="parquet",
                                        path=base_path,
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="brotli",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="parquet",
                path=base_path,
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_read_parquet_nopart_zstd(self):
        base_path = "test_read_parquet_nopart_zstd"
        test_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=test_df,
                                        file_format="parquet",
                                        path=base_path,
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="zstd",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="parquet",
                path=base_path,
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_read_parquet_nopart_lz4(self):
        base_path = "test_read_parquet_nopart_lz4"
        test_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=test_df,
                                        file_format="parquet",
                                        path=base_path,
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="lz4",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="parquet",
                path=base_path,
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_read_deltalake_nopart_no_compression(self):
        base_path = "test_read_deltalake_nopart_no_compression"
        test_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=test_df,
                                        file_format="deltalake",
                                        path=base_path,
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="None",
                                            existing_data_behavior="overwrite")
                                        )

        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="parquet",
                path=base_path,
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_read_deltalake_nopart_snappy(self):
        base_path = "test_read_deltalake_nopart_snappy"
        test_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=test_df,
                                        file_format="deltalake",
                                        path=base_path,
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="snappy",
                                            existing_data_behavior="overwrite")
                                        )

        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="parquet",
                path=base_path,
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_read_deltalake_nopart_gzip(self):
        base_path = "test_read_deltalake_nopart_gzip"
        test_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=test_df,
                                        file_format="deltalake",
                                        path=base_path,
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="gzip",
                                            existing_data_behavior="overwrite")
                                        )

        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="parquet",
                path=base_path,
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_read_deltalake_nopart_brotli(self):
        base_path = "test_read_deltalake_nopart_brotli"
        test_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=test_df,
                                        file_format="deltalake",
                                        path=base_path,
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="brotli",
                                            existing_data_behavior="overwrite")
                                        )

        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="parquet",
                path=base_path,
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_read_deltalake_nopart_zstd(self):
        base_path = "test_read_deltalake_nopart_zstd"
        test_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=test_df,
                                        file_format="deltalake",
                                        path=base_path,
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="zstd",
                                            existing_data_behavior="overwrite")
                                        )

        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="parquet",
                path=base_path,
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_read_deltalake_nopart_lz4(self):
        base_path = "test_read_deltalake_nopart_lz4"
        test_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=test_df,
                                        file_format="deltalake",
                                        path=base_path,
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="lz4",
                                            existing_data_behavior="overwrite")
                                        )

        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="parquet",
                path=base_path,
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )
