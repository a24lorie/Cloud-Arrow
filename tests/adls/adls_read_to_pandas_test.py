import deltalake as dlt
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from cloud.core import ParquetWriteOptions, DeltaLakeWriteOptions
from tests.core import ADLSTestBase


class TestADLSReadToPandas(ADLSTestBase):
    _base_path = None
    _test_df = None

    @classmethod
    def setUpClass(cls):
        ADLSTestBase.setUpClass()

        cls._base_path = "write"
        cls._test_df = pd.read_csv("data/diabetes/csv/nopart/diabetes.csv")

        try:
            # write parquet directories
            ADLSTestBase._get_filesystem_client().create_directory(f"{cls._base_path}/parquet/test_nocompression")
            ADLSTestBase._get_filesystem_client().create_directory(f"{cls._base_path}/parquet/test_snappy")
            ADLSTestBase._get_filesystem_client().create_directory(f"{cls._base_path}/parquet/test_gzip")
            ADLSTestBase._get_filesystem_client().create_directory(f"{cls._base_path}/parquet/test_brotli")
            ADLSTestBase._get_filesystem_client().create_directory(f"{cls._base_path}/parquet/test_zstd")
            ADLSTestBase._get_filesystem_client().create_directory(f"{cls._base_path}/parquet/test_lz4")

            # write deltalake directories
            ADLSTestBase._get_filesystem_client().create_directory(f"{cls._base_path}/deltalake/test_nocompression")
            ADLSTestBase._get_filesystem_client().create_directory(f"{cls._base_path}/deltalake/test_snappy")
            ADLSTestBase._get_filesystem_client().create_directory(f"{cls._base_path}/deltalake/test_gzip")
            ADLSTestBase._get_filesystem_client().create_directory(f"{cls._base_path}/deltalake/test_brotli")
            ADLSTestBase._get_filesystem_client().create_directory(f"{cls._base_path}/deltalake/test_zstd")
            ADLSTestBase._get_filesystem_client().create_directory(f"{cls._base_path}/deltalake/test_lz4")

            arr_table = pa.Table.from_pandas(cls._test_df)

            # use pyarrow library to write parquet files
            pq.write_to_dataset(arr_table, filesystem=cls._filesystem, compression='none',
                                existing_data_behavior='error',
                                root_path=cls._adls_object_storage._get_filesystem_base_path(
                                    f"{cls._base_path}/parquet/test_nocompression"))
            pq.write_to_dataset(arr_table, filesystem=cls._filesystem, compression='snappy',
                                existing_data_behavior='error',
                                root_path=cls._adls_object_storage._get_filesystem_base_path(
                                    f"{cls._base_path}/parquet/test_snappy"))
            pq.write_to_dataset(arr_table, filesystem=cls._filesystem, compression='gzip',
                                existing_data_behavior='error',
                                root_path=cls._adls_object_storage._get_filesystem_base_path(
                                    f"{cls._base_path}/parquet/test_gzip"))
            pq.write_to_dataset(arr_table, filesystem=cls._filesystem, compression='brotli',
                                existing_data_behavior='error',
                                root_path=cls._adls_object_storage._get_filesystem_base_path(
                                    f"{cls._base_path}/parquet/test_brotli"))
            pq.write_to_dataset(arr_table, filesystem=cls._filesystem, compression='zstd',
                                existing_data_behavior='error',
                                root_path=cls._adls_object_storage._get_filesystem_base_path(
                                    f"{cls._base_path}/parquet/test_zstd"))
            pq.write_to_dataset(arr_table, filesystem=cls._filesystem, compression='lz4',
                                existing_data_behavior='error',
                                root_path=cls._adls_object_storage._get_filesystem_base_path(
                                    f"{cls._base_path}/parquet/test_lz4"))

            # use deltalake library to write deltalake files
            dlt.write_deltalake(
                table_or_uri=cls._adls_object_storage._get_deltalake_url(f"{cls._base_path}/deltalake/test_nocompression"),
                data=arr_table, mode="error",
                storage_options=cls._adls_object_storage._get_deltalake_storage_options(),
                file_options=ds.ParquetFileFormat().make_write_options(compression='none'))
            dlt.write_deltalake(
                table_or_uri=cls._adls_object_storage._get_deltalake_url(f"{cls._base_path}/deltalake/test_snappy"),
                data=arr_table, mode="error",
                storage_options=cls._adls_object_storage._get_deltalake_storage_options(),
                file_options=ds.ParquetFileFormat().make_write_options(compression='snappy'))
            dlt.write_deltalake(
                table_or_uri=cls._adls_object_storage._get_deltalake_url(f"{cls._base_path}/deltalake/test_gzip"),
                data=arr_table, mode="error",
                storage_options=cls._adls_object_storage._get_deltalake_storage_options(),
                file_options=ds.ParquetFileFormat().make_write_options(compression='gzip'))
            dlt.write_deltalake(
                table_or_uri=cls._adls_object_storage._get_deltalake_url(f"{cls._base_path}/deltalake/test_brotli"),
                data=arr_table, mode="error",
                storage_options=cls._adls_object_storage._get_deltalake_storage_options(),
                file_options=ds.ParquetFileFormat().make_write_options(compression='brotli'))
            dlt.write_deltalake(
                table_or_uri=cls._adls_object_storage._get_deltalake_url(f"{cls._base_path}/deltalake/test_zstd"),
                data=arr_table, mode="error",
                storage_options=cls._adls_object_storage._get_deltalake_storage_options(),
                file_options=ds.ParquetFileFormat().make_write_options(compression='zstd'))
            dlt.write_deltalake(
                table_or_uri=cls._adls_object_storage._get_deltalake_url(f"{cls._base_path}/deltalake/test_lz4"),
                data=arr_table, mode="error",
                storage_options=cls._adls_object_storage._get_deltalake_storage_options(),
                file_options=ds.ParquetFileFormat().make_write_options(compression='lz4'))
        finally:
            pass

    @classmethod
    def tearDownClass(cls):
        # Remove write directory recursively
        cls._delete_adls_dir(f"{cls._base_path}")

    def test_adls_read_parquet_nopart_no_compression(self):
        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="parquet",
                path=f"{self._base_path}/parquet/test_nocompression",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(self._test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            pass

    def test_adls_read_parquet_nopart_snappy(self):
        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="parquet",
                path=f"{self._base_path}/parquet/test_snappy",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(self._test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            pass

    def test_adls_read_parquet_nopart_gzip(self):
        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="parquet",
                path=f"{self._base_path}/parquet/test_gzip",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(self._test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            pass

    def test_adls_read_parquet_nopart_brotli(self):
        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="parquet",
                path=f"{self._base_path}/parquet/test_brotli",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(self._test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            pass

    def test_adls_read_parquet_nopart_zstd(self):
        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="parquet",
                path=f"{self._base_path}/parquet/test_zstd",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(self._test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            pass

    def test_adls_read_parquet_nopart_lz4(self):
        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="parquet",
                path=f"{self._base_path}/parquet/test_lz4",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(self._test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
           pass

    def test_adls_read_deltalake_nopart_no_compression(self):
        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="deltalake",
                path=f"{self._base_path}/deltalake/test_nocompression",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(self._test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            pass

    def test_adls_read_deltalake_nopart_snappy(self):
        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="deltalake",
                path=f"{self._base_path}/deltalake/test_snappy",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(self._test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            pass

    def test_adls_read_deltalake_nopart_gzip(self):
        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="deltalake",
                path=f"{self._base_path}/deltalake/test_gzip",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(self._test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            pass

    def test_adls_read_deltalake_nopart_brotli(self):
        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="deltalake",
                path=f"{self._base_path}/deltalake/test_brotli",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(self._test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            pass

    def test_adls_read_deltalake_nopart_zstd(self):
        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="deltalake",
                path=f"{self._base_path}/deltalake/test_zstd",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(self._test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            pass

    def test_adls_read_deltalake_nopart_lz4(self):
        try:
            input_df = self._adls_object_storage.read_to_pandas(
                file_format="deltalake",
                path=f"{self._base_path}/deltalake/test_lz4",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_df = len(self._test_df.index)

            self.assertEqual(
                count_input_df,
                count_test_df,
                "Should match"
            )
        finally:
            pass
