import unittest

import pandas as pd
import pyarrow.parquet as pq
from adlfs import AzureBlobFileSystem

from cloud.core import ParquetWriteOptions, DeltaLakeWriteOptions
from tests.core import ADLSTestBase

if __name__ == '__main__':
    unittest.main()


class TestADLSWrite(ADLSTestBase):
    
    _base_path = None

    @classmethod
    def setUpClass(cls):
        ADLSTestBase.setUpClass()

        cls._base_path = "write"
        cls._test_df = pd.read_csv("../data/diabetes/csv/nopart/diabetes.csv")

    @classmethod
    def tearDownClass(cls):
        # Remove write directory recursively
        cls._delete_adls_dir(f"{cls._base_path}")

    def test_adls_write_parquet_nopart_no_compression(self):
        # write the table to ADLS
        self._adls_object_storage.write(table=self._test_df,
                                        file_format="parquet",
                                        path=f"{self._base_path}/parquet/test_nocompression",
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="None",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        try:
            filesystem_client = self._get_filesystem_client()
            paths = filesystem_client.get_paths(path=f"{self._base_path}/parquet/test_nocompression")

            for path in paths:
                # print(path)
                # read the file metadata
                metadata = pq.read_metadata(
                    where=f"{self._container_name}/{path['name']}",
                    filesystem=self._filesystem
                )
                compression_type = metadata.row_group(0).column(0).compression
                print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                assert (compression_type == 'UNCOMPRESSED')

        finally:
            pass

    def test_adls_write_parquet_nopart_snappy(self):
        # write the table to ADLS
        self._adls_object_storage.write(table=self._test_df,
                                        file_format="parquet",
                                        path=f"{self._base_path}/parquet/test_snappy",
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="snappy",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        try:
            filesystem_client = self._get_filesystem_client()
            paths = filesystem_client.get_paths(path=f"{self._base_path}/parquet/test_snappy")

            for path in paths:
                # print(path)
                # read the file metadata
                metadata = pq.read_metadata(
                    where=f"{self._container_name}/{path['name']}",
                    filesystem=self._filesystem
                )
                compression_type = metadata.row_group(0).column(0).compression
                print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                assert (compression_type == 'SNAPPY')
        finally:
            pass

    def test_adls_write_parquet_nopart_gzip(self):
        # write the table to ADLS
        self._adls_object_storage.write(table=self._test_df,
                                        file_format="parquet",
                                        path=f"{self._base_path}/parquet/test_gzip",
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="gzip",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        try:
            filesystem_client = self._get_filesystem_client()
            paths = filesystem_client.get_paths(path=f"{self._base_path}/parquet/test_gzip")

            for path in paths:
                # print(path)
                # read the file metadata
                metadata = pq.read_metadata(
                    where=f"{self._container_name}/{path['name']}",
                    filesystem=self._filesystem
                )
                compression_type = metadata.row_group(0).column(0).compression
                print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                assert (compression_type == 'GZIP')
        finally:
            pass

    def test_adls_write_parquet_nopart_brotli(self):
        # write the table to ADLS
        self._adls_object_storage.write(table=self._test_df,
                                        file_format="parquet",
                                        path=f"{self._base_path}/parquet/test_brotli",
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="brotli",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        try:
            filesystem_client = self._get_filesystem_client()
            paths = filesystem_client.get_paths(path=f"{self._base_path}/parquet/test_brotli")

            for path in paths:
                # print(path)
                # read the file metadata
                metadata = pq.read_metadata(
                    where=f"{self._container_name}/{path['name']}",
                    filesystem=self._filesystem
                )
                compression_type = metadata.row_group(0).column(0).compression
                print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                assert (compression_type == 'BROTLI')
        finally:
            pass

    def test_adls_write_parquet_nopart_zstd(self):
        # write the table to ADLS
        self._adls_object_storage.write(table=self._test_df,
                                        file_format="parquet",
                                        path=f"{self._base_path}/parquet/test_zstd",
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="zstd",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        try:
            filesystem_client = self._get_filesystem_client()
            paths = filesystem_client.get_paths(path=f"{self._base_path}/parquet/test_zstd")

            for path in paths:
                # print(path)
                # read the file metadata
                metadata = pq.read_metadata(
                    where=f"{self._container_name}/{path['name']}",
                    filesystem=self._filesystem
                )
                compression_type = metadata.row_group(0).column(0).compression
                print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                assert (compression_type == 'ZSTD')
        finally:
            pass

    def test_adls_write_parquet_nopart_lz4(self):
        # write the table to ADLS
        self._adls_object_storage.write(table=self._test_df,
                                        file_format="parquet",
                                        path=f"{self._base_path}/parquet/test_lz4",
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="lz4",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        try:
            filesystem_client = self._get_filesystem_client()
            paths = filesystem_client.get_paths(path=f"{self._base_path}/parquet/test_lz4")

            for path in paths:
                # print(path)
                # read the file metadata
                metadata = pq.read_metadata(
                    where=f"{self._container_name}/{path['name']}",
                    filesystem=self._filesystem
                )
                compression_type = metadata.row_group(0).column(0).compression
                print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                assert (compression_type == 'LZ4')
        finally:
            pass

    def test_adls_write_deltalake_nopart_no_compression(self):
        # write the table to ADLS
        self._adls_object_storage.write(table=self._test_df,
                                        file_format="deltalake",
                                        path=f"{self._base_path}/deltalake/test_nocompression",
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="None",
                                            existing_data_behavior="overwrite")
                                        )

        try:
            filesystem_client = self._get_filesystem_client()
            paths = filesystem_client.get_paths(path=f"{self._base_path}/deltalake/test_nocompression", recursive=False)

            for path in paths:
                # print(path)
                # read the file metadata
                if not path.is_directory:
                    metadata = pq.read_metadata(
                        where=f"{self._container_name}/{path['name']}",
                        filesystem=self._filesystem
                    )

                    compression_type = metadata.row_group(0).column(0).compression
                    print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                    assert (compression_type == 'UNCOMPRESSED')

        finally:
            pass

    def test_adls_write_deltalake_nopart_snappy(self):
        # write the table to ADLS
        self._adls_object_storage.write(table=self._test_df,
                                        file_format="deltalake",
                                        path=f"{self._base_path}/deltalake/test_snappy",
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="snappy",
                                            existing_data_behavior="overwrite")
                                        )

        try:
            filesystem_client = self._get_filesystem_client()
            paths = filesystem_client.get_paths(path=f"{self._base_path}/deltalake/test_snappy", recursive=False)

            for path in paths:
                # print(path)
                # read the file metadata
                if not path.is_directory:
                    metadata = pq.read_metadata(
                        where=f"{self._container_name}/{path['name']}",
                        filesystem=self._filesystem
                    )

                    compression_type = metadata.row_group(0).column(0).compression
                    print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                    assert (compression_type == 'SNAPPY')
        finally:
            pass

    def test_adls_write_deltalake_nopart_gzip(self):
        # write the table to ADLS
        self._adls_object_storage.write(table=self._test_df,
                                        file_format="deltalake",
                                        path=f"{self._base_path}/deltalake/test_gzip",
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="gzip",
                                            existing_data_behavior="overwrite")
                                        )

        try:
            filesystem_client = self._get_filesystem_client()
            paths = filesystem_client.get_paths(path=f"{self._base_path}/deltalake/test_gzip", recursive=False)

            for path in paths:
                # print(path)
                # read the file metadata
                if not path.is_directory:
                    metadata = pq.read_metadata(
                        where=f"{self._container_name}/{path['name']}",
                        filesystem=self._filesystem
                    )

                    compression_type = metadata.row_group(0).column(0).compression
                    print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                    assert (compression_type == 'GZIP')
        finally:
            pass

    def test_adls_write_deltalake_nopart_brotli(self):
        # write the table to ADLS
        self._adls_object_storage.write(table=self._test_df,
                                        file_format="deltalake",
                                        path=f"{self._base_path}/deltalake/test_brotli",
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="brotli",
                                            existing_data_behavior="overwrite")
                                        )

        try:
            filesystem_client = self._get_filesystem_client()
            paths = filesystem_client.get_paths(path=f"{self._base_path}/deltalake/test_brotli", recursive=False)

            for path in paths:
                # print(path)
                # read the file metadata
                if not path.is_directory:
                    metadata = pq.read_metadata(
                        where=f"{self._container_name}/{path['name']}",
                        filesystem=self._filesystem
                    )

                    compression_type = metadata.row_group(0).column(0).compression
                    print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                    assert (compression_type == 'BROTLI')
        finally:
            pass

    def test_adls_write_deltalake_nopart_zstd(self):
        # write the table to ADLS
        self._adls_object_storage.write(table=self._test_df,
                                        file_format="deltalake",
                                        path=f"{self._base_path}/deltalake/test_zstd",
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="zstd",
                                            existing_data_behavior="overwrite")
                                        )

        try:
            filesystem_client = self._get_filesystem_client()
            paths = filesystem_client.get_paths(path=f"{self._base_path}/deltalake/test_zstd", recursive=False)

            for path in paths:
                # print(path)
                # read the file metadata
                if not path.is_directory:
                    metadata = pq.read_metadata(
                        where=f"{self._container_name}/{path['name']}",
                        filesystem=self._filesystem
                    )

                    compression_type = metadata.row_group(0).column(0).compression
                    print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                    assert (compression_type == 'ZSTD')
        finally:
            pass

    def test_adls_write_deltalake_nopart_lz4(self):
        # write the table to ADLS
        self._adls_object_storage.write(table=self._test_df,
                                        file_format="deltalake",
                                        path=f"{self._base_path}/deltalake/test_lz4",
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="lz4",
                                            existing_data_behavior="overwrite")
                                        )

        try:
            filesystem_client = self._get_filesystem_client()
            paths = filesystem_client.get_paths(path=f"{self._base_path}/deltalake/test_lz4", recursive=False)

            for path in paths:
                # print(path)
                # read the file metadata
                if not path.is_directory:
                    metadata = pq.read_metadata(
                        where=f"{self._container_name}/{path['name']}",
                        filesystem=self._filesystem
                    )

                    compression_type = metadata.row_group(0).column(0).compression
                    print(f"Filename: {self._container_name}/{path['name']}, compression_type: {compression_type}")
                    assert (compression_type == 'LZ4')
        finally:
            pass
