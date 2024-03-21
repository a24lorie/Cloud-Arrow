import deltalake as dlt
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from tests.core import LocalFilesystemTestBase
from tests.core.base_dbfs_test import DBFSTestBase


class TestDBFSReadToPandas(DBFSTestBase):
    _base_path = None
    _test_table = None
    _schema = None

    @classmethod
    def setUpClass(cls):
        DBFSTestBase.setUpClass()

        cls._base_path = "/FileStore/write"
        cls._test_table = cls.make_mock_diabetes_arrow_table()

        # write parquet directories
        cls._filesystem.mkdir(f"{cls._base_path}/parquet/test_nocompression", create_parents=True)

        # write deltalake directories
        cls._filesystem.mkdir(f"{cls._base_path}/deltalake/test_nocompression", create_parents=True)

        # use pyarrow library to write parquet files
        cls._schema = cls._test_table.schema
        
        pq.write_to_dataset(cls._test_table, f"{cls._base_path}/parquet/test_nocompression", compression='none')

        # use deltalake library to write deltalake files
        dlt.write_deltalake(table_or_uri=f"{cls._base_path}/deltalake/test_nocompression", data=cls._test_table,
                            file_options=ds.ParquetFileFormat().make_write_options(compression='none'), mode="overwrite")

    @classmethod
    def tearDownClass(cls):
        # Remove write directory create_parentsly
        cls._delete_dbfs_dir(f"{cls._base_path}")

    def test_localfilesystem_read_parquet_nopart_no_compression(self):
        try:
            input_df = self._dbfs_object_storage.read_to_pandas(
                file_format="parquet",
                path=f"{self._base_path}/parquet/test_nocompression",
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

    # def test_localfilesystem_read_parquet_nopart_snappy(self):
    #     try:
    #         input_df = self._local_filesystem_storage.read_to_pandas(
    #             file_format="parquet",
    #             path=f"{self._base_path}/parquet/test_snappy",
    #             filters=None
    #         )
    #
    #         count_input_df = len(input_df.index)
    #         count_test_table = self._test_table.num_rows
    #
    #         self.assertEqual(
    #             count_input_df,
    #             count_test_table,
    #             "Should match"
    #         )
    #     finally:
    #         pass
    #
    # def test_localfilesystem_read_parquet_nopart_gzip(self):
    #     try:
    #         input_df = self._local_filesystem_storage.read_to_pandas(
    #             file_format="parquet",
    #             path=f"{self._base_path}/parquet/test_gzip",
    #             filters=None
    #         )
    #
    #         count_input_df = len(input_df.index)
    #         count_test_table = self._test_table.num_rows
    #
    #         self.assertEqual(
    #             count_input_df,
    #             count_test_table,
    #             "Should match"
    #         )
    #     finally:
    #         pass
    #
    # def test_localfilesystem_read_parquet_nopart_brotli(self):
    #     try:
    #         input_df = self._local_filesystem_storage.read_to_pandas(
    #             file_format="parquet",
    #             path=f"{self._base_path}/parquet/test_brotli",
    #             filters=None
    #         )
    #
    #         count_input_df = len(input_df.index)
    #         count_test_table = self._test_table.num_rows
    #
    #         self.assertEqual(
    #             count_input_df,
    #             count_test_table,
    #             "Should match"
    #         )
    #     finally:
    #        pass
    #
    # def test_localfilesystem_read_parquet_nopart_zstd(self):
    #     try:
    #         input_df = self._local_filesystem_storage.read_to_pandas(
    #             file_format="parquet",
    #             path=f"{self._base_path}/parquet/test_zstd",
    #             filters=None
    #         )
    #
    #         count_input_df = len(input_df.index)
    #         count_test_table = self._test_table.num_rows
    #
    #         self.assertEqual(
    #             count_input_df,
    #             count_test_table,
    #             "Should match"
    #         )
    #     finally:
    #         pass
    #
    # def test_localfilesystem_read_parquet_nopart_lz4(self):
    #     try:
    #         input_df = self._local_filesystem_storage.read_to_pandas(
    #             file_format="parquet",
    #             path=f"{self._base_path}/parquet/test_lz4",
    #             filters=None
    #         )
    #
    #         count_input_df = len(input_df.index)
    #         count_test_table = self._test_table.num_rows
    #
    #         self.assertEqual(
    #             count_input_df,
    #             count_test_table,
    #             "Should match"
    #         )
    #     finally:
    #         pass
    #
    # def test_localfilesystem_read_deltalake_nopart_no_compression(self):
    #     try:
    #         input_df = self._local_filesystem_storage.read_to_pandas(
    #             file_format="deltalake",
    #             path=f"{self._base_path}/deltalake/test_nocompression",
    #             filters=None
    #         )
    #
    #         count_input_df = len(input_df.index)
    #         count_test_table = self._test_table.num_rows
    #
    #         self.assertEqual(
    #             count_input_df,
    #             count_test_table,
    #             "Should match"
    #         )
    #     finally:
    #         pass
    #
    # def test_localfilesystem_read_deltalake_nopart_snappy(self):
    #     try:
    #         input_df = self._local_filesystem_storage.read_to_pandas(
    #             file_format="deltalake",
    #             path=f"{self._base_path}/deltalake/test_snappy",
    #             filters=None
    #         )
    #
    #         count_input_df = len(input_df.index)
    #         count_test_table = self._test_table.num_rows
    #
    #         self.assertEqual(
    #             count_input_df,
    #             count_test_table,
    #             "Should match"
    #         )
    #     finally:
    #         pass
    #
    # def test_localfilesystem_read_deltalake_nopart_gzip(self):
    #     try:
    #         input_df = self._local_filesystem_storage.read_to_pandas(
    #             file_format="deltalake",
    #             path=f"{self._base_path}/deltalake/test_gzip",
    #             filters=None
    #         )
    #
    #         count_input_df = len(input_df.index)
    #         count_test_table = self._test_table.num_rows
    #
    #         self.assertEqual(
    #             count_input_df,
    #             count_test_table,
    #             "Should match"
    #         )
    #     finally:
    #         pass
    #
    # def test_localfilesystem_read_deltalake_nopart_brotli(self):
    #     try:
    #         input_df = self._local_filesystem_storage.read_to_pandas(
    #             file_format="deltalake",
    #             path=f"{self._base_path}/deltalake/test_brotli",
    #             filters=None
    #         )
    #
    #         count_input_df = len(input_df.index)
    #         count_test_table = self._test_table.num_rows
    #
    #         self.assertEqual(
    #             count_input_df,
    #             count_test_table,
    #             "Should match"
    #         )
    #     finally:
    #         pass
    #
    # def test_localfilesystem_read_deltalake_nopart_zstd(self):
    #     try:
    #         input_df = self._local_filesystem_storage.read_to_pandas(
    #             file_format="deltalake",
    #             path=f"{self._base_path}/deltalake/test_zstd",
    #             filters=None
    #         )
    #
    #         count_input_df = len(input_df.index)
    #         count_test_table = self._test_table.num_rows
    #
    #         self.assertEqual(
    #             count_input_df,
    #             count_test_table,
    #             "Should match"
    #         )
    #     finally:
    #         pass
    #
    # def test_localfilesystem_read_deltalake_nopart_lz4(self):
    #     try:
    #         input_df = self._local_filesystem_storage.read_to_pandas(
    #             file_format="deltalake",
    #             path=f"{self._base_path}/deltalake/test_lz4",
    #             filters=None
    #         )
    #
    #         count_input_df = len(input_df.index)
    #         count_test_table = self._test_table.num_rows
    #
    #         self.assertEqual(
    #             count_input_df,
    #             count_test_table,
    #             "Should match"
    #         )
    #     finally:
    #         pass
