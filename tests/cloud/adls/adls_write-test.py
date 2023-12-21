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


class TestADLSWrite(ADLSTestBase):

    def __init__(self, *args, **kwargs):
        super(TestADLSWrite, self).__init__(*args, **kwargs)
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

    def test_adls_write_parquet_nopart_no_compression(self):
        base_path = "test_write_parquet_nopart_no_compression"
        input_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=input_df,
                                        file_format="parquet",
                                        path=base_path,
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="None",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        try:
            filesystem_client = self._get_filesystem_client(
                storage_account=self._storage_account_name,
                container=self._container_name,
            )
            paths = filesystem_client.get_paths(path=base_path)

            adls_filesystem = AzureBlobFileSystem(
                account_name=self._storage_account_name,
                tenant_id=self._tenant_id,
                client_id=self._client_id,
                client_secret=self._client_secret
            )

            for path in paths:
                # print(path)
                # read the file metadata
                metadata = pq.read_metadata(
                    where=f"{self._container_name}/{path['name']}",
                    filesystem=adls_filesystem
                )
                compression_type = metadata.row_group(0).column(0).compression
                print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                assert (compression_type == 'UNCOMPRESSED')

        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_write_parquet_nopart_snappy(self):
        base_path = "test_write_parquet_nopart_snappy"
        input_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=input_df,
                                        file_format="parquet",
                                        path=base_path,
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="snappy",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        try:
            filesystem_client = self._get_filesystem_client(
                storage_account=self._storage_account_name,
                container=self._container_name,
            )
            paths = filesystem_client.get_paths(path=base_path)

            adls_filesystem = AzureBlobFileSystem(
                account_name=self._storage_account_name,
                tenant_id=self._tenant_id,
                client_id=self._client_id,
                client_secret=self._client_secret
            )

            for path in paths:
                # print(path)
                # read the file metadata
                metadata = pq.read_metadata(
                    where=f"{self._container_name}/{path['name']}",
                    filesystem=adls_filesystem
                )
                compression_type = metadata.row_group(0).column(0).compression
                print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                assert (compression_type == 'SNAPPY')
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_write_parquet_nopart_gzip(self):
        base_path = "test_write_parquet_nopart_gzip"
        input_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=input_df,
                                        file_format="parquet",
                                        path=base_path,
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="gzip",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        try:
            filesystem_client = self._get_filesystem_client(
                storage_account=self._storage_account_name,
                container=self._container_name,
            )
            paths = filesystem_client.get_paths(path=base_path)

            adls_filesystem = AzureBlobFileSystem(
                account_name=self._storage_account_name,
                tenant_id=self._tenant_id,
                client_id=self._client_id,
                client_secret=self._client_secret
            )

            for path in paths:
                # print(path)
                # read the file metadata
                metadata = pq.read_metadata(
                    where=f"{self._container_name}/{path['name']}",
                    filesystem=adls_filesystem
                )
                compression_type = metadata.row_group(0).column(0).compression
                print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                assert (compression_type == 'GZIP')
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_write_parquet_nopart_brotli(self):
        base_path = "test_write_parquet_nopart_brotli"
        input_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=input_df,
                                        file_format="parquet",
                                        path=base_path,
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="brotli",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        try:
            filesystem_client = self._get_filesystem_client(
                storage_account=self._storage_account_name,
                container=self._container_name,
            )
            paths = filesystem_client.get_paths(path=base_path)

            adls_filesystem = AzureBlobFileSystem(
                account_name=self._storage_account_name,
                tenant_id=self._tenant_id,
                client_id=self._client_id,
                client_secret=self._client_secret
            )

            for path in paths:
                # print(path)
                # read the file metadata
                metadata = pq.read_metadata(
                    where=f"{self._container_name}/{path['name']}",
                    filesystem=adls_filesystem
                )
                compression_type = metadata.row_group(0).column(0).compression
                print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                assert (compression_type == 'BROTLI')
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_write_parquet_nopart_zstd(self):
        base_path = "test_write_parquet_nopart_zstd"
        input_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=input_df,
                                        file_format="parquet",
                                        path=base_path,
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="zstd",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        try:
            filesystem_client = self._get_filesystem_client(
                storage_account=self._storage_account_name,
                container=self._container_name,
            )
            paths = filesystem_client.get_paths(path=base_path)

            adls_filesystem = AzureBlobFileSystem(
                account_name=self._storage_account_name,
                tenant_id=self._tenant_id,
                client_id=self._client_id,
                client_secret=self._client_secret
            )

            for path in paths:
                # print(path)
                # read the file metadata
                metadata = pq.read_metadata(
                    where=f"{self._container_name}/{path['name']}",
                    filesystem=adls_filesystem
                )
                compression_type = metadata.row_group(0).column(0).compression
                print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                assert (compression_type == 'ZSTD')
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_write_parquet_nopart_lz4(self):
        base_path = "test_write_parquet_nopart_lz4"
        input_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=input_df,
                                        file_format="parquet",
                                        path=base_path,
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="lz4",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        try:
            filesystem_client = self._get_filesystem_client(
                storage_account=self._storage_account_name,
                container=self._container_name,
            )
            paths = filesystem_client.get_paths(path=base_path)

            adls_filesystem = AzureBlobFileSystem(
                account_name=self._storage_account_name,
                tenant_id=self._tenant_id,
                client_id=self._client_id,
                client_secret=self._client_secret
            )

            for path in paths:
                # print(path)
                # read the file metadata
                metadata = pq.read_metadata(
                    where=f"{self._container_name}/{path['name']}",
                    filesystem=adls_filesystem
                )
                compression_type = metadata.row_group(0).column(0).compression
                print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                assert (compression_type == 'LZ4')
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_write_deltalake_nopart_no_compression(self):
        base_path = "test_write_deltalake_nopart_no_compression"
        input_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=input_df,
                                        file_format="deltalake",
                                        path=base_path,
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="None",
                                            existing_data_behavior="overwrite")
                                        )

        try:
            filesystem_client = self._get_filesystem_client(
                storage_account=self._storage_account_name,
                container=self._container_name,
            )
            paths = filesystem_client.get_paths(path=base_path, recursive=False)

            adls_filesystem = AzureBlobFileSystem(
                account_name=self._storage_account_name,
                tenant_id=self._tenant_id,
                client_id=self._client_id,
                client_secret=self._client_secret
            )

            for path in paths:
                # print(path)
                # read the file metadata
                if not path.is_directory:
                    metadata = pq.read_metadata(
                        where=f"{self._container_name}/{path['name']}",
                        filesystem=adls_filesystem
                    )

                    compression_type = metadata.row_group(0).column(0).compression
                    print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                    assert (compression_type == 'UNCOMPRESSED')

        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_write_deltalake_nopart_snappy(self):
        base_path = "test_write_deltalake_nopart_snappy"
        input_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=input_df,
                                        file_format="deltalake",
                                        path=base_path,
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="snappy",
                                            existing_data_behavior="overwrite")
                                        )

        try:
            filesystem_client = self._get_filesystem_client(
                storage_account=self._storage_account_name,
                container=self._container_name,
            )
            paths = filesystem_client.get_paths(path=base_path, recursive=False)

            adls_filesystem = AzureBlobFileSystem(
                account_name=self._storage_account_name,
                tenant_id=self._tenant_id,
                client_id=self._client_id,
                client_secret=self._client_secret
            )

            for path in paths:
                # print(path)
                # read the file metadata
                if not path.is_directory:
                    metadata = pq.read_metadata(
                        where=f"{self._container_name}/{path['name']}",
                        filesystem=adls_filesystem
                    )

                    compression_type = metadata.row_group(0).column(0).compression
                    print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                    assert (compression_type == 'SNAPPY')
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_write_deltalake_nopart_gzip(self):
        base_path = "test_write_deltalake_nopart_gzip"
        input_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=input_df,
                                        file_format="deltalake",
                                        path=base_path,
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="gzip",
                                            existing_data_behavior="overwrite")
                                        )

        try:
            filesystem_client = self._get_filesystem_client(
                storage_account=self._storage_account_name,
                container=self._container_name,
            )
            paths = filesystem_client.get_paths(path=base_path, recursive=False)

            adls_filesystem = AzureBlobFileSystem(
                account_name=self._storage_account_name,
                tenant_id=self._tenant_id,
                client_id=self._client_id,
                client_secret=self._client_secret
            )

            for path in paths:
                # print(path)
                # read the file metadata
                if not path.is_directory:
                    metadata = pq.read_metadata(
                        where=f"{self._container_name}/{path['name']}",
                        filesystem=adls_filesystem
                    )

                    compression_type = metadata.row_group(0).column(0).compression
                    print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                    assert (compression_type == 'GZIP')
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_write_deltalake_nopart_brotli(self):
        base_path = "test_write_deltalake_nopart_brotli"
        input_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=input_df,
                                        file_format="deltalake",
                                        path=base_path,
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="brotli",
                                            existing_data_behavior="overwrite")
                                        )

        try:
            filesystem_client = self._get_filesystem_client(
                storage_account=self._storage_account_name,
                container=self._container_name,
            )
            paths = filesystem_client.get_paths(path=base_path, recursive=False)

            adls_filesystem = AzureBlobFileSystem(
                account_name=self._storage_account_name,
                tenant_id=self._tenant_id,
                client_id=self._client_id,
                client_secret=self._client_secret
            )

            for path in paths:
                # print(path)
                # read the file metadata
                if not path.is_directory:
                    metadata = pq.read_metadata(
                        where=f"{self._container_name}/{path['name']}",
                        filesystem=adls_filesystem
                    )

                    compression_type = metadata.row_group(0).column(0).compression
                    print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                    assert (compression_type == 'BROTLI')
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_write_deltalake_nopart_zstd(self):
        base_path = "test_write_deltalake_nopart_zstd"
        input_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=input_df,
                                        file_format="deltalake",
                                        path=base_path,
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="zstd",
                                            existing_data_behavior="overwrite")
                                        )

        try:
            filesystem_client = self._get_filesystem_client(
                storage_account=self._storage_account_name,
                container=self._container_name,
            )
            paths = filesystem_client.get_paths(path=base_path, recursive=False)

            adls_filesystem = AzureBlobFileSystem(
                account_name=self._storage_account_name,
                tenant_id=self._tenant_id,
                client_id=self._client_id,
                client_secret=self._client_secret
            )

            for path in paths:
                # print(path)
                # read the file metadata
                if not path.is_directory:
                    metadata = pq.read_metadata(
                        where=f"{self._container_name}/{path['name']}",
                        filesystem=adls_filesystem
                    )

                    compression_type = metadata.row_group(0).column(0).compression
                    print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                    assert (compression_type == 'ZSTD')
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )

    def test_adls_write_deltalake_nopart_lz4(self):
        base_path = "test_write_deltalake_nopart_lz4"
        input_df = pd.read_csv('../../data/hmeq.csv')

        # write the table to ADLS
        self._adls_object_storage.write(table=input_df,
                                        file_format="deltalake",
                                        path=base_path,
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="lz4",
                                            existing_data_behavior="overwrite")
                                        )

        try:
            filesystem_client = self._get_filesystem_client(
                storage_account=self._storage_account_name,
                container=self._container_name,
            )
            paths = filesystem_client.get_paths(path=base_path, recursive=False)

            adls_filesystem = AzureBlobFileSystem(
                account_name=self._storage_account_name,
                tenant_id=self._tenant_id,
                client_id=self._client_id,
                client_secret=self._client_secret
            )

            for path in paths:
                # print(path)
                # read the file metadata
                if not path.is_directory:
                    metadata = pq.read_metadata(
                        where=f"{self._container_name}/{path['name']}",
                        filesystem=adls_filesystem
                    )

                    compression_type = metadata.row_group(0).column(0).compression
                    print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                    assert (compression_type == 'LZ4')
        finally:
            # delete the file
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=base_path
            )
