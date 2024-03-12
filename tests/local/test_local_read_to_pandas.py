import deltalake as dlt
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from tests.core import LocalFilesystemTestBase


class TestLocalFilesystemReadToPandas(LocalFilesystemTestBase):

    @classmethod
    def setUpClass(cls):
        LocalFilesystemTestBase.setUpClass()

        # write parquet directories
        cls._filesystem.mkdir(f"{cls._base_path}/write/parquet/test_nocompression", create_parents=True)
        cls._filesystem.mkdir(f"{cls._base_path}/write/parquet/test_snappy", create_parents=True)
        cls._filesystem.mkdir(f"{cls._base_path}/write/parquet/test_gzip", create_parents=True)
        cls._filesystem.mkdir(f"{cls._base_path}/write/parquet/test_brotli", create_parents=True)
        cls._filesystem.mkdir(f"{cls._base_path}/write/parquet/test_zstd", create_parents=True)
        cls._filesystem.mkdir(f"{cls._base_path}/write/parquet/test_lz4", create_parents=True)
        # write deltalake directories
        cls._filesystem.mkdir(f"{cls._base_path}/write/deltalake/test_nocompression", create_parents=True)
        cls._filesystem.mkdir(f"{cls._base_path}/write/deltalake/test_snappy", create_parents=True)
        cls._filesystem.mkdir(f"{cls._base_path}/write/deltalake/test_gzip", create_parents=True)
        cls._filesystem.mkdir(f"{cls._base_path}/write/deltalake/test_brotli", create_parents=True)
        cls._filesystem.mkdir(f"{cls._base_path}/write/deltalake/test_zstd", create_parents=True)
        cls._filesystem.mkdir(f"{cls._base_path}/write/deltalake/test_lz4", create_parents=True)

        # use pyarrow library to write parquet files
        arr_table = cls._test_table
        cls._schema = arr_table.schema
        
        pq.write_to_dataset(arr_table, f"{cls._base_path}/write/parquet/test_nocompression", compression='none')
        pq.write_to_dataset(arr_table, f"{cls._base_path}/write/parquet/test_snappy", compression='snappy')
        pq.write_to_dataset(arr_table, f"{cls._base_path}/write/parquet/test_gzip", compression='gzip')
        pq.write_to_dataset(arr_table, f"{cls._base_path}/write/parquet/test_brotli", compression='brotli')
        pq.write_to_dataset(arr_table, f"{cls._base_path}/write/parquet/test_zstd", compression='zstd')
        pq.write_to_dataset(arr_table, f"{cls._base_path}/write/parquet/test_lz4", compression='lz4')

        # use deltalake library to write deltalake files
        dlt.write_deltalake(table_or_uri=f"{cls._base_path}/write/deltalake/test_nocompression", data=arr_table,
                            file_options=ds.ParquetFileFormat().make_write_options(compression='none'))
        dlt.write_deltalake(table_or_uri=f"{cls._base_path}/write/deltalake/test_snappy", data=arr_table,
                            file_options=ds.ParquetFileFormat().make_write_options(compression='snappy'))
        dlt.write_deltalake(table_or_uri=f"{cls._base_path}/write/deltalake/test_gzip", data=arr_table,
                            file_options=ds.ParquetFileFormat().make_write_options(compression='gzip'))
        dlt.write_deltalake(table_or_uri=f"{cls._base_path}/write/deltalake/test_brotli", data=arr_table,
                            file_options=ds.ParquetFileFormat().make_write_options(compression='brotli'))
        dlt.write_deltalake(table_or_uri=f"{cls._base_path}/write/deltalake/test_zstd", data=arr_table,
                            file_options=ds.ParquetFileFormat().make_write_options(compression='zstd'))
        dlt.write_deltalake(table_or_uri=f"{cls._base_path}/write/deltalake/test_lz4", data=arr_table,
                            file_options=ds.ParquetFileFormat().make_write_options(compression='lz4'))

    @classmethod
    def tearDownClass(cls):
        # Remove write directory create_parentsly
        cls._filesystem.delete(f"{cls._base_path}/write", recursive=True)

    def test_localfilesystem_read_parquet_nopart_no_compression(self):
        try:
            input_df = self._local_filesystem_storage.read_to_pandas(
                file_format="parquet",
                path=f"{self._base_path}/write/parquet/test_nocompression",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_table = self._test_table.num_rows

            self.assertEqual(
                count_input_df,
                count_test_table,
                "Should match"
            )
        finally:
            pass

    def test_localfilesystem_read_parquet_nopart_snappy(self):
        try:
            input_df = self._local_filesystem_storage.read_to_pandas(
                file_format="parquet",
                path=f"{self._base_path}/write/parquet/test_snappy",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_table = self._test_table.num_rows

            self.assertEqual(
                count_input_df,
                count_test_table,
                "Should match"
            )
        finally:
            pass

    def test_localfilesystem_read_parquet_nopart_gzip(self):
        try:
            input_df = self._local_filesystem_storage.read_to_pandas(
                file_format="parquet",
                path=f"{self._base_path}/write/parquet/test_gzip",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_table = self._test_table.num_rows

            self.assertEqual(
                count_input_df,
                count_test_table,
                "Should match"
            )
        finally:
            pass

    def test_localfilesystem_read_parquet_nopart_brotli(self):
        try:
            input_df = self._local_filesystem_storage.read_to_pandas(
                file_format="parquet",
                path=f"{self._base_path}/write/parquet/test_brotli",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_table = self._test_table.num_rows

            self.assertEqual(
                count_input_df,
                count_test_table,
                "Should match"
            )
        finally:
           pass

    def test_localfilesystem_read_parquet_nopart_zstd(self):
        try:
            input_df = self._local_filesystem_storage.read_to_pandas(
                file_format="parquet",
                path=f"{self._base_path}/write/parquet/test_zstd",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_table = self._test_table.num_rows

            self.assertEqual(
                count_input_df,
                count_test_table,
                "Should match"
            )
        finally:
            pass

    def test_localfilesystem_read_parquet_nopart_lz4(self):
        try:
            input_df = self._local_filesystem_storage.read_to_pandas(
                file_format="parquet",
                path=f"{self._base_path}/write/parquet/test_lz4",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_table = self._test_table.num_rows

            self.assertEqual(
                count_input_df,
                count_test_table,
                "Should match"
            )
        finally:
            pass

    def test_localfilesystem_read_deltalake_nopart_no_compression(self):
        try:
            input_df = self._local_filesystem_storage.read_to_pandas(
                file_format="deltalake",
                path=f"{self._base_path}/write/deltalake/test_nocompression",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_table = self._test_table.num_rows

            self.assertEqual(
                count_input_df,
                count_test_table,
                "Should match"
            )
        finally:
            pass

    def test_localfilesystem_read_deltalake_nopart_snappy(self):
        try:
            input_df = self._local_filesystem_storage.read_to_pandas(
                file_format="deltalake",
                path=f"{self._base_path}/write/deltalake/test_snappy",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_table = self._test_table.num_rows

            self.assertEqual(
                count_input_df,
                count_test_table,
                "Should match"
            )
        finally:
            pass

    def test_localfilesystem_read_deltalake_nopart_gzip(self):
        try:
            input_df = self._local_filesystem_storage.read_to_pandas(
                file_format="deltalake",
                path=f"{self._base_path}/write/deltalake/test_gzip",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_table = self._test_table.num_rows

            self.assertEqual(
                count_input_df,
                count_test_table,
                "Should match"
            )
        finally:
            pass

    def test_localfilesystem_read_deltalake_nopart_brotli(self):
        try:
            input_df = self._local_filesystem_storage.read_to_pandas(
                file_format="deltalake",
                path=f"{self._base_path}/write/deltalake/test_brotli",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_table = self._test_table.num_rows

            self.assertEqual(
                count_input_df,
                count_test_table,
                "Should match"
            )
        finally:
            pass

    def test_localfilesystem_read_deltalake_nopart_zstd(self):
        try:
            input_df = self._local_filesystem_storage.read_to_pandas(
                file_format="deltalake",
                path=f"{self._base_path}/write/deltalake/test_zstd",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_table = self._test_table.num_rows

            self.assertEqual(
                count_input_df,
                count_test_table,
                "Should match"
            )
        finally:
            pass

    def test_localfilesystem_read_deltalake_nopart_lz4(self):
        try:
            input_df = self._local_filesystem_storage.read_to_pandas(
                file_format="deltalake",
                path=f"{self._base_path}/write/deltalake/test_lz4",
                filters=None
            )

            count_input_df = len(input_df.index)
            count_test_table = self._test_table.num_rows

            self.assertEqual(
                count_input_df,
                count_test_table,
                "Should match"
            )
        finally:
            pass
