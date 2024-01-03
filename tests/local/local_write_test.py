import unittest

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from cloud.core import ParquetWriteOptions, DeltaLakeWriteOptions
from tests.core import LocalFilesystemTestBase

if __name__ == '__main__':
    unittest.main()


class TestLocalFilesystemWrite(LocalFilesystemTestBase):

    _base_path = None
    _test_df = None

    @classmethod
    def setUpClass(cls):
        LocalFilesystemTestBase.setUpClass()

        cls._base_path = "data/diabetes"
        cls._test_df = pd.read_csv(f"{cls._base_path}/csv/nopart/diabetes.csv")

    @classmethod
    def tearDownClass(cls):
        # Remove write directory recursively
        cls._filesystem.delete(f"{cls._base_path}/write", recursive=True)

    def test_adls_write_parquet_nopart_no_compression(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(table=self._test_df,
                                             file_format="parquet",
                                             path=f"{self._base_path}/write/parquet/test_nocompression",
                                             write_options=ParquetWriteOptions(
                                                 partitions=[],
                                                 compression_codec="None",
                                                 existing_data_behavior="overwrite_or_ignore")
                                             )

        try:
            paths = self._filesystem.ls(path=f"{self._base_path}/write/parquet/test_nocompression")

            for path in paths:
                # print(path)
                # read the file metadata
                metadata = pq.read_metadata(
                    where=f"{path}",
                    filesystem=self._filesystem
                )
                compression_type = metadata.row_group(0).column(0).compression
                print(f"Filename: {path}, compression_type: {compression_type}")
                assert (compression_type == 'UNCOMPRESSED')

        finally:
            pass

    def test_adls_write_parquet_nopart_snappy(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(table=self._test_df,
                                             file_format="parquet",
                                             path=f"{self._base_path}/write/parquet/test_snappy",
                                             write_options=ParquetWriteOptions(
                                                 partitions=[],
                                                 compression_codec="snappy",
                                                 existing_data_behavior="overwrite_or_ignore")
                                             )

        try:
            paths = self._filesystem.ls(path=f"{self._base_path}/write/parquet/test_snappy")

            for path in paths:
                # print(path)
                # read the file metadata
                metadata = pq.read_metadata(
                    where=f"{path}",
                    filesystem=self._filesystem
                )
                compression_type = metadata.row_group(0).column(0).compression
                print(f"Filename: {path}, compression_type: {compression_type}")
                assert (compression_type == 'SNAPPY')
        finally:
            pass

    def test_adls_write_parquet_nopart_gzip(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(table=self._test_df,
                                             file_format="parquet",
                                             path=f"{self._base_path}/write/parquet/test_gzip",
                                             write_options=ParquetWriteOptions(
                                                 partitions=[],
                                                 compression_codec="gzip",
                                                 existing_data_behavior="overwrite_or_ignore")
                                             )

        try:
            paths = self._filesystem.ls(path=f"{self._base_path}/write/parquet/test_gzip")

            for path in paths:
                # print(path)
                # read the file metadata
                metadata = pq.read_metadata(
                    where=f"{path}",
                    filesystem=self._filesystem
                )
                compression_type = metadata.row_group(0).column(0).compression
                print(f"Filename: {path}, compression_type: {compression_type}")
                assert (compression_type == 'GZIP')
        finally:
            pass

    def test_adls_write_parquet_nopart_brotli(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(table=self._test_df,
                                             file_format="parquet",
                                             path=f"{self._base_path}/write/parquet/test_brotli",
                                             write_options=ParquetWriteOptions(
                                                 partitions=[],
                                                 compression_codec="brotli",
                                                 existing_data_behavior="overwrite_or_ignore")
                                             )

        try:
            paths = self._filesystem.ls(path=f"{self._base_path}/write/parquet/test_brotli")

            for path in paths:
                # print(path)
                # read the file metadata
                metadata = pq.read_metadata(
                    where=f"{path}",
                    filesystem=self._filesystem
                )
                compression_type = metadata.row_group(0).column(0).compression
                print(f"Filename: {path}, compression_type: {compression_type}")
                assert (compression_type == 'BROTLI')
        finally:
            pass

    def test_adls_write_parquet_nopart_zstd(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(table=self._test_df,
                                             file_format="parquet",
                                             path=f"{self._base_path}/write/parquet/test_zstd",
                                             write_options=ParquetWriteOptions(
                                                 partitions=[],
                                                 compression_codec="zstd",
                                                 existing_data_behavior="overwrite_or_ignore")
                                             )

        try:
            paths = self._filesystem.ls(path=f"{self._base_path}/write/parquet/test_zstd")

            for path in paths:
                # print(path)
                # read the file metadata
                metadata = pq.read_metadata(
                    where=f"{path}",
                    filesystem=self._filesystem
                )
                compression_type = metadata.row_group(0).column(0).compression
                print(f"Filename: {path}, compression_type: {compression_type}")
                assert (compression_type == 'ZSTD')
        finally:
            pass

    def test_adls_write_parquet_nopart_lz4(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(table=self._test_df,
                                             file_format="parquet",
                                             path=f"{self._base_path}/write/parquet/test_lz4",
                                             write_options=ParquetWriteOptions(
                                                 partitions=[],
                                                 compression_codec="lz4",
                                                 existing_data_behavior="overwrite_or_ignore")
                                             )

        try:
            paths = self._filesystem.ls(path=f"{self._base_path}/write/parquet/test_lz4")

            for path in paths:
                # print(path)
                # read the file metadata
                metadata = pq.read_metadata(
                    where=f"{path}",
                    filesystem=self._filesystem
                )
                compression_type = metadata.row_group(0).column(0).compression
                print(f"Filename: {path}, compression_type: {compression_type}")
                assert (compression_type == 'LZ4')
        finally:
            pass

    def test_adls_write_deltalake_nopart_no_compression(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(table=self._test_df,
                                             file_format="deltalake",
                                             path=f"{self._base_path}/write/deltalake/test_nocompression",
                                             write_options=DeltaLakeWriteOptions(
                                                 partitions=[],
                                                 compression_codec="None",
                                                 existing_data_behavior="overwrite")
                                             )

        try:
            paths = self._filesystem.ls(path=f"{self._base_path}/write/deltalake/test_nocompression")

            for path in paths:
                # print(path)
                # read the file metadata
                if not self._filesystem.isdir(path):
                    metadata = pq.read_metadata(
                        where=f"{path}",
                        filesystem=self._filesystem
                    )

                    compression_type = metadata.row_group(0).column(0).compression
                    print(f"Filename: {path}, compression_type: {compression_type}")
                    assert (compression_type == 'UNCOMPRESSED')

        finally:
            pass

    def test_adls_write_deltalake_nopart_snappy(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(table=self._test_df,
                                             file_format="deltalake",
                                             path=f"{self._base_path}/write/deltalake/test_snappy",
                                             write_options=DeltaLakeWriteOptions(
                                                 partitions=[],
                                                 compression_codec="snappy",
                                                 existing_data_behavior="overwrite")
                                             )

        try:
            paths = self._filesystem.ls(path=f"{self._base_path}/write/deltalake/test_snappy")

            for path in paths:
                # print(path)
                # read the file metadata
                if not self._filesystem.isdir(path):
                    metadata = pq.read_metadata(
                        where=f"{path}",
                        filesystem=self._filesystem
                    )

                    compression_type = metadata.row_group(0).column(0).compression
                    print(f"Filename: {path}, compression_type: {compression_type}")
                    assert (compression_type == 'SNAPPY')
        finally:
            pass

    def test_adls_write_deltalake_nopart_gzip(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(table=self._test_df,
                                             file_format="deltalake",
                                             path=f"{self._base_path}/write/deltalake/test_gzip",
                                             write_options=DeltaLakeWriteOptions(
                                                 partitions=[],
                                                 compression_codec="gzip",
                                                 existing_data_behavior="overwrite")
                                             )

        try:
            paths = self._filesystem.ls(path=f"{self._base_path}/write/deltalake/test_gzip")

            for path in paths:
                # print(path)
                # read the file metadata
                if not self._filesystem.isdir(path):
                    metadata = pq.read_metadata(
                        where=f"{path}",
                        filesystem=self._filesystem
                    )

                    compression_type = metadata.row_group(0).column(0).compression
                    print(f"Filename: {path}, compression_type: {compression_type}")
                    assert (compression_type == 'GZIP')
        finally:
            pass

    def test_adls_write_deltalake_nopart_brotli(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(table=self._test_df,
                                             file_format="deltalake",
                                             path=f"{self._base_path}/write/deltalake/test_brotli",
                                             write_options=DeltaLakeWriteOptions(
                                                 partitions=[],
                                                 compression_codec="brotli",
                                                 existing_data_behavior="overwrite")
                                             )

        try:
            paths = self._filesystem.ls(path=f"{self._base_path}/write/deltalake/test_brotli")

            for path in paths:
                # print(path)
                # read the file metadata
                if not self._filesystem.isdir(path):
                    metadata = pq.read_metadata(
                        where=f"{path}",
                        filesystem=self._filesystem
                    )

                    compression_type = metadata.row_group(0).column(0).compression
                    print(f"Filename: {path}, compression_type: {compression_type}")
                    assert (compression_type == 'BROTLI')
        finally:
            pass

    def test_adls_write_deltalake_nopart_zstd(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(table=self._test_df,
                                             file_format="deltalake",
                                             path=f"{self._base_path}/write/deltalake/test_zstd",
                                             write_options=DeltaLakeWriteOptions(
                                                 partitions=[],
                                                 compression_codec="zstd",
                                                 existing_data_behavior="overwrite")
                                             )

        try:
            paths = self._filesystem.ls(path=f"{self._base_path}/write/deltalake/test_zstd")

            for path in paths:
                # print(path)
                # read the file metadata
                if not self._filesystem.isdir(path):
                    metadata = pq.read_metadata(
                        where=f"{path}",
                        filesystem=self._filesystem
                    )

                    compression_type = metadata.row_group(0).column(0).compression
                    print(f"Filename: {path}, compression_type: {compression_type}")
                    assert (compression_type == 'ZSTD')
        finally:
            pass

    def test_adls_write_deltalake_nopart_lz4(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(table=self._test_df,
                                             file_format="deltalake",
                                             path=f"{self._base_path}/write/deltalake/test_lz4",
                                             write_options=DeltaLakeWriteOptions(
                                                 partitions=[],
                                                 compression_codec="lz4",
                                                 existing_data_behavior="overwrite")
                                             )

        try:
            paths = self._filesystem.ls(path=f"{self._base_path}/write/deltalake/test_lz4")

            for path in paths:
                # print(path)
                # read the file metadata
                if not self._filesystem.isdir(path):
                    metadata = pq.read_metadata(
                        where=f"{path}",
                        filesystem=self._filesystem
                    )

                    compression_type = metadata.row_group(0).column(0).compression
                    print(f"Filename: {path}, compression_type: {compression_type}")
                    assert (compression_type == 'LZ4')
        finally:
            pass
