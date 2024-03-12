import unittest

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from cloud.core import ParquetWriteOptions, DeltaLakeWriteOptions
from tests.core import LocalFilesystemTestBase

if __name__ == '__main__':
    unittest.main()


class TestLocalFilesystemWrite(LocalFilesystemTestBase):

    @classmethod
    def setUpClass(cls):
        LocalFilesystemTestBase.setUpClass()

    @classmethod
    def tearDownClass(cls):
        # Remove write directory recursively
        cls._filesystem.delete(f"{cls._base_path}", recursive=True)

    def validate_compression(self, path, compression):
        try:
            paths = self._filesystem.ls(path=path)

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
                    assert (compression_type == compression)
        finally:
            pass

    def test_adls_write_parquet_nopart_no_compression_from_arrow_table(self):
        # write the table to local filesystem
        self._local_filesystem_storage.write(data=self._test_table,
                                             file_format="parquet",
                                             path=f"{self._base_path}/write/parquet/test_nocompression",
                                             write_options=ParquetWriteOptions(
                                                 partitions=[],
                                                 compression_codec="None",
                                                 existing_data_behavior="overwrite_or_ignore")
                                             )

        self.validate_compression(path=f"{self._base_path}/write/parquet/test_nocompression",
                                  compression='UNCOMPRESSED')

    def test_adls_write_parquet_nopart_no_compression_from_arrow_pandas(self):
        # write the table to local filesystem
        self._local_filesystem_storage.write(data=self._test_table.to_pandas(),
                                             file_format="parquet",
                                             path=f"{self._base_path}/write/parquet/test_nocompression_pandas",
                                             write_options=ParquetWriteOptions(
                                                 partitions=[],
                                                 compression_codec="None",
                                                 existing_data_behavior="overwrite_or_ignore")
                                             )

        self.validate_compression(path=f"{self._base_path}/write/parquet/test_nocompression_pandas",
                                  compression='UNCOMPRESSED')

    def test_adls_write_parquet_nopart_no_compression_from_arrow_recordBatches(self):
        batch_reader = self.mock_random_diabetes_arrow_batchReader()

        # write the table to local filesystem
        self._local_filesystem_storage.write(data=batch_reader,
                                             file_format="parquet",
                                             path=f"{self._base_path}/write/parquet/test_nocompression_batch_reader",
                                             write_options=ParquetWriteOptions(
                                                 partitions=[],
                                                 compression_codec="None",
                                                 existing_data_behavior="overwrite_or_ignore")
                                             )

        self.validate_compression(path=f"{self._base_path}/write/parquet/test_nocompression_batch_reader",
                                  compression='UNCOMPRESSED')

    def test_adls_write_parquet_nopart_snappy_arrow_table(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(data=self._test_table,
                                             file_format="parquet",
                                             path=f"{self._base_path}/write/parquet/test_snappy",
                                             write_options=ParquetWriteOptions(
                                                 partitions=[],
                                                 compression_codec="snappy",
                                                 existing_data_behavior="overwrite_or_ignore")
                                             )

        self.validate_compression(path=f"{self._base_path}/write/parquet/test_snappy",
                                  compression='SNAPPY')

    def test_adls_write_parquet_nopart_gzip_arrow_table(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(data=self._test_table,
                                             file_format="parquet",
                                             path=f"{self._base_path}/write/parquet/test_gzip",
                                             write_options=ParquetWriteOptions(
                                                 partitions=[],
                                                 compression_codec="gzip",
                                                 existing_data_behavior="overwrite_or_ignore")
                                             )

        self.validate_compression(path=f"{self._base_path}/write/parquet/test_gzip",
                                  compression='GZIP')

    def test_adls_write_parquet_nopart_brotli_arrow_table(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(data=self._test_table,
                                             file_format="parquet",
                                             path=f"{self._base_path}/write/parquet/test_brotli",
                                             write_options=ParquetWriteOptions(
                                                 partitions=[],
                                                 compression_codec="brotli",
                                                 existing_data_behavior="overwrite_or_ignore")
                                             )

        self.validate_compression(path=f"{self._base_path}/write/parquet/test_brotli",
                                  compression='BROTLI')

    def test_adls_write_parquet_nopart_zstd_arrow_table(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(data=self._test_table,
                                             file_format="parquet",
                                             path=f"{self._base_path}/write/parquet/test_zstd",
                                             write_options=ParquetWriteOptions(
                                                 partitions=[],
                                                 compression_codec="zstd",
                                                 existing_data_behavior="overwrite_or_ignore")
                                             )

        self.validate_compression(path=f"{self._base_path}/write/parquet/test_zstd",
                                  compression='ZSTD')

    def test_adls_write_parquet_nopart_lz4_arrow_table(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(data=self._test_table,
                                             file_format="parquet",
                                             path=f"{self._base_path}/write/parquet/test_lz4",
                                             write_options=ParquetWriteOptions(
                                                 partitions=[],
                                                 compression_codec="lz4",
                                                 existing_data_behavior="overwrite_or_ignore")
                                             )

        self.validate_compression(path=f"{self._base_path}/write/parquet/test_lz4",
                                  compression='LZ4')

    def test_adls_write_deltalake_nopart_no_compression_arrow_table(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(data=self._test_table,
                                             file_format="deltalake",
                                             path=f"{self._base_path}/write/deltalake/test_nocompression",
                                             write_options=DeltaLakeWriteOptions(
                                                 partitions=[],
                                                 compression_codec="None",
                                                 existing_data_behavior="overwrite")
                                             )

        self.validate_compression(path=f"{self._base_path}/write/deltalake/test_nocompression",
                                  compression='UNCOMPRESSED')

    def test_adls_write_deltalake_nopart_no_compression_from_arrow_pandas(self):
        # write the table to local filesystem
        self._local_filesystem_storage.write(data=self._test_table.to_pandas(),
                                             file_format="deltalake",
                                             path=f"{self._base_path}/write/deltalake/test_nocompression_pandas",
                                             write_options=ParquetWriteOptions(
                                                 partitions=[],
                                                 compression_codec="None",
                                                 existing_data_behavior="overwrite_or_ignore")
                                             )

        self.validate_compression(path=f"{self._base_path}/write/deltalake/test_nocompression_pandas",
                                  compression='UNCOMPRESSED')

    def test_adls_write_deltalake_nopart_no_compression_from_arrow_recordBatches(self):
        batch_reader = self.mock_random_diabetes_arrow_batchReader()

        # write the table to local filesystem
        self._local_filesystem_storage.write(data=batch_reader,
                                             file_format="deltalake",
                                             path=f"{self._base_path}/write/deltalake/test_nocompression_batch_reader",
                                             write_options=ParquetWriteOptions(
                                                 partitions=[],
                                                 compression_codec="None",
                                                 existing_data_behavior="overwrite_or_ignore")
                                             )

        self.validate_compression(path=f"{self._base_path}/write/deltalake/test_nocompression_batch_reader",
                                  compression='UNCOMPRESSED')

    def test_adls_write_deltalake_nopart_snappy_arrow_table(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(data=self._test_table,
                                             file_format="deltalake",
                                             path=f"{self._base_path}/write/deltalake/test_snappy",
                                             write_options=DeltaLakeWriteOptions(
                                                 partitions=[],
                                                 compression_codec="snappy",
                                                 existing_data_behavior="overwrite")
                                             )
        self.validate_compression(path=f"{self._base_path}/write/deltalake/test_snappy",
                                  compression='SNAPPY')

    def test_adls_write_deltalake_nopart_gzip_arrow_table(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(data=self._test_table,
                                             file_format="deltalake",
                                             path=f"{self._base_path}/write/deltalake/test_gzip",
                                             write_options=DeltaLakeWriteOptions(
                                                 partitions=[],
                                                 compression_codec="gzip",
                                                 existing_data_behavior="overwrite")
                                             )

        self.validate_compression(path=f"{self._base_path}/write/deltalake/test_gzip",
                                  compression='GZIP')

    def test_adls_write_deltalake_nopart_brotli_arrow_table(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(data=self._test_table,
                                             file_format="deltalake",
                                             path=f"{self._base_path}/write/deltalake/test_brotli",
                                             write_options=DeltaLakeWriteOptions(
                                                 partitions=[],
                                                 compression_codec="brotli",
                                                 existing_data_behavior="overwrite")
                                             )

        self.validate_compression(path=f"{self._base_path}/write/deltalake/test_brotli",
                                  compression='BROTLI')

    def test_adls_write_deltalake_nopart_zstd_arrow_table(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(data=self._test_table,
                                             file_format="deltalake",
                                             path=f"{self._base_path}/write/deltalake/test_zstd",
                                             write_options=DeltaLakeWriteOptions(
                                                 partitions=[],
                                                 compression_codec="zstd",
                                                 existing_data_behavior="overwrite")
                                             )

        self.validate_compression(path=f"{self._base_path}/write/deltalake/test_zstd",
                                  compression='ZSTD')

    def test_adls_write_deltalake_nopart_lz4_arrow_table(self):
        # write the table to ADLS
        self._local_filesystem_storage.write(data=self._test_table,
                                             file_format="deltalake",
                                             path=f"{self._base_path}/write/deltalake/test_lz4",
                                             write_options=DeltaLakeWriteOptions(
                                                 partitions=[],
                                                 compression_codec="lz4",
                                                 existing_data_behavior="overwrite")
                                             )

        self.validate_compression(path=f"{self._base_path}/write/deltalake/test_lz4",
                                  compression='LZ4')
