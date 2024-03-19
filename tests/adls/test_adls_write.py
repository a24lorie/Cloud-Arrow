import unittest

import pyarrow.parquet as pq
import pyarrow.dataset as ds

from cloud.core import ParquetWriteOptions, DeltaLakeWriteOptions
from tests.core import ADLSTestBase

if __name__ == '__main__':
    unittest.main()


class TestADLSWrite(ADLSTestBase):
    
    _base_path = None

    @classmethod
    def setUpClass(cls):
        ADLSTestBase.setUpClass()

    @classmethod
    def tearDownClass(cls):
        # Remove write directory recursively
        cls._delete_adls_dir(f"{cls._base_path}")

    def validate_number_of_records(self, path):
        table_rows = ds.dataset(source=f"{path}", filesystem=self._filesystem).count_rows()
        assert (table_rows == 25)

    def test_adls_write_parquet_nopart_no_compression_from_arrow_table(self):
        # write the table to ADLS
        self._adls_object_storage.write(data=self._test_table,
                                        file_format="parquet",
                                        path=f"{self._base_path}/parquet/test_nocompression",
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="None",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        self.validate_number_of_records(path=f"{self._container_name}/{self._base_path}/parquet/test_nocompression")
        self.validate_compression(path=f"{self._container_name}/{self._base_path}/parquet/test_nocompression",
                                  compression='UNCOMPRESSED')

    def test_adls_write_parquet_nopart_no_compression_from_arrow_pandas(self):
        # write the table to local filesystem
        self._adls_object_storage.write(data=self._test_table.to_pandas(),
                                             file_format="parquet",
                                             path=f"{self._base_path}/parquet/test_nocompression_pandas",
                                             write_options=ParquetWriteOptions(
                                                 partitions=[],
                                                 compression_codec="None",
                                                 existing_data_behavior="overwrite_or_ignore")
                                             )

        self.validate_number_of_records(path=f"{self._container_name}/{self._base_path}/parquet/test_nocompression_pandas")
        self.validate_compression(path=f"{self._container_name}/{self._base_path}/parquet/test_nocompression_pandas",
                                  compression='UNCOMPRESSED')

    def test_adls_write_parquet_nopart_no_compression_from_arrow_recordBatches(self):
        batch_reader = self.mock_random_diabetes_arrow_batchReader()

        # write the table to local filesystem
        self._adls_object_storage.write(data=batch_reader,
                                         file_format="parquet",
                                         path=f"{self._base_path}/parquet/test_nocompression_batch_reader",
                                         write_options=ParquetWriteOptions(
                                             partitions=[],
                                             compression_codec="None",
                                             existing_data_behavior="overwrite_or_ignore")
                                         )

        self.validate_number_of_records(path=f"{self._container_name}/{self._base_path}/parquet/test_nocompression_batch_reader")
        self.validate_compression(path=f"{self._container_name}/{self._base_path}/parquet/test_nocompression_batch_reader",
                                  compression='UNCOMPRESSED')

    def test_adls_write_parquet_nopart_snappy_arrow_table(self):
        # write the table to ADLS
        self._adls_object_storage.write(data=self._test_table,
                                        file_format="parquet",
                                        path=f"{self._base_path}/parquet/test_snappy",
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="snappy",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        self.validate_number_of_records(path=f"{self._container_name}/{self._base_path}/parquet/test_snappy")
        self.validate_compression(path=f"{self._container_name}/{self._base_path}/parquet/test_snappy",
                                  compression='SNAPPY')

    def test_adls_write_parquet_nopart_gzip_arrow_table(self):
        # write the table to ADLS
        self._adls_object_storage.write(data=self._test_table,
                                        file_format="parquet",
                                        path=f"{self._base_path}/parquet/test_gzip",
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="gzip",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        self.validate_number_of_records(path=f"{self._container_name}/{self._base_path}/parquet/test_gzip")
        self.validate_compression(path=f"{self._container_name}/{self._base_path}/parquet/test_gzip",
                                  compression='GZIP')

    def test_adls_write_parquet_nopart_brotli_arrow_table(self):
        # write the table to ADLS
        self._adls_object_storage.write(data=self._test_table,
                                        file_format="parquet",
                                        path=f"{self._base_path}/parquet/test_brotli",
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="brotli",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        self.validate_number_of_records(path=f"{self._container_name}/{self._base_path}/parquet/test_brotli")
        self.validate_compression(path=f"{self._container_name}/{self._base_path}/parquet/test_brotli",
                                  compression='BROTLI')

    def test_adls_write_parquet_nopart_zstd_arrow_table(self):
        # write the table to ADLS
        self._adls_object_storage.write(data=self._test_table,
                                        file_format="parquet",
                                        path=f"{self._base_path}/parquet/test_zstd",
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="zstd",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        self.validate_number_of_records(path=f"{self._container_name}/{self._base_path}/parquet/test_zstd")
        self.validate_compression(path=f"{self._container_name}/{self._base_path}/parquet/test_zstd",
                                  compression='ZSTD')

    def test_adls_write_parquet_nopart_lz4_arrow_table(self):
        # write the table to ADLS
        self._adls_object_storage.write(data=self._test_table,
                                        file_format="parquet",
                                        path=f"{self._base_path}/parquet/test_lz4",
                                        write_options=ParquetWriteOptions(
                                            partitions=[],
                                            compression_codec="lz4",
                                            existing_data_behavior="overwrite_or_ignore")
                                        )

        self.validate_number_of_records(path=f"{self._container_name}/{self._base_path}/parquet/test_lz4")
        self.validate_compression(path=f"{self._container_name}/{self._base_path}/parquet/test_lz4",
                                  compression='LZ4')

    def test_adls_write_deltalake_nopart_no_compression_arrow_table(self):
        # write the table to ADLS
        self._adls_object_storage.write(data=self._test_table,
                                        file_format="deltalake",
                                        path=f"{self._base_path}/deltalake/test_nocompression",
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="None",
                                            existing_data_behavior="overwrite")
                                        )

        self.validate_number_of_records(path=f"{self._container_name}/{self._base_path}/deltalake/test_nocompression")
        self.validate_compression(path=f"{self._container_name}/{self._base_path}/deltalake/test_nocompression",
                                  compression='UNCOMPRESSED')

    def test_adls_write_deltalake_nopart_no_compression_from_arrow_pandas(self):
        # write the table to local filesystem
        self._adls_object_storage.write(data=self._test_table.to_pandas(),
                                             file_format="deltalake",
                                             path=f"{self._base_path}/deltalake/test_nocompression_pandas",
                                             write_options=ParquetWriteOptions(
                                                 partitions=[],
                                                 compression_codec="None",
                                                 existing_data_behavior="overwrite_or_ignore")
                                             )

        self.validate_number_of_records(path=f"{self._container_name}/{self._base_path}/deltalake/test_nocompression_pandas")
        self.validate_compression(path=f"{self._container_name}/{self._base_path}/deltalake/test_nocompression_pandas",
                                  compression='UNCOMPRESSED')

    def test_adls_write_deltalake_nopart_no_compression_from_arrow_recordBatches(self):
        batch_reader = self.mock_random_diabetes_arrow_batchReader()

        # write the table to local filesystem
        self._adls_object_storage.write(data=batch_reader,
                                             file_format="deltalake",
                                             path=f"{self._base_path}/deltalake/test_nocompression_batch_reader",
                                             write_options=ParquetWriteOptions(
                                                 partitions=[],
                                                 compression_codec="None",
                                                 existing_data_behavior="overwrite_or_ignore")
                                             )

        self.validate_number_of_records(path=f"{self._container_name}/{self._base_path}/deltalake/test_nocompression_batch_reader")
        self.validate_compression(path=f"{self._container_name}/{self._base_path}/deltalake/test_nocompression_batch_reader",
                                  compression='UNCOMPRESSED')

    def test_adls_write_deltalake_nopart_snappy_arrow_table(self):
        # write the table to ADLS
        self._adls_object_storage.write(data=self._test_table,
                                        file_format="deltalake",
                                        path=f"{self._base_path}/deltalake/test_snappy",
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="snappy",
                                            existing_data_behavior="overwrite")
                                        )

        self.validate_number_of_records(path=f"{self._container_name}/{self._base_path}/deltalake/test_snappy")
        self.validate_compression(path=f"{self._container_name}/{self._base_path}/deltalake/test_snappy",
                                  compression='SNAPPY')

    def test_adls_write_deltalake_nopart_gzip_arrow_table(self):
        # write the table to ADLS
        self._adls_object_storage.write(data=self._test_table,
                                        file_format="deltalake",
                                        path=f"{self._base_path}/deltalake/test_gzip",
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="gzip",
                                            existing_data_behavior="overwrite")
                                        )

        self.validate_number_of_records(path=f"{self._container_name}/{self._base_path}/deltalake/test_gzip")
        self.validate_compression(path=f"{self._container_name}/{self._base_path}/deltalake/test_gzip",
                                  compression='GZIP')

    def test_adls_write_deltalake_nopart_brotli_arrow_table(self):
        # write the table to ADLS
        self._adls_object_storage.write(data=self._test_table,
                                        file_format="deltalake",
                                        path=f"{self._base_path}/deltalake/test_brotli",
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="brotli",
                                            existing_data_behavior="overwrite")
                                        )

        self.validate_number_of_records(path=f"{self._container_name}/{self._base_path}/deltalake/test_brotli")
        self.validate_compression(path=f"{self._container_name}/{self._base_path}/deltalake/test_brotli",
                                  compression='BROTLI')

    def test_adls_write_deltalake_nopart_zstd_arrow_table(self):
        # write the table to ADLS
        self._adls_object_storage.write(data=self._test_table,
                                        file_format="deltalake",
                                        path=f"{self._base_path}/deltalake/test_zstd",
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="zstd",
                                            existing_data_behavior="overwrite")
                                        )

        self.validate_number_of_records(path=f"{self._container_name}/{self._base_path}/deltalake/test_zstd")
        self.validate_compression(path=f"{self._container_name}/{self._base_path}/deltalake/test_zstd",
                                  compression='ZSTD')

    def test_adls_write_deltalake_nopart_lz4_arrow_table(self):
        # write the table to ADLS
        self._adls_object_storage.write(data=self._test_table,
                                        file_format="deltalake",
                                        path=f"{self._base_path}/deltalake/test_lz4",
                                        write_options=DeltaLakeWriteOptions(
                                            partitions=[],
                                            compression_codec="lz4",
                                            existing_data_behavior="overwrite")
                                        )

        self.validate_number_of_records(path=f"{self._container_name}/{self._base_path}/deltalake/test_lz4")
        self.validate_compression(path=f"{self._container_name}/{self._base_path}/deltalake/test_lz4",
                                  compression='LZ4')
