import deltalake as dlt
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from tests.core.base_local_test import LocalFilesystemTestBase


class TestLocalFilesystemDataset(LocalFilesystemTestBase):

    @classmethod
    def setUpClass(cls):
        LocalFilesystemTestBase.setUpClass()

        try:
            # write parquet directories
            cls._filesystem.mkdir(f"{cls._base_path}/parquet/part", create_parents=True)
            cls._filesystem.mkdir(f"{cls._base_path}/parquet/nopart", create_parents=True)
            cls._filesystem.mkdir(f"{cls._base_path}/deltalake/part", create_parents=True)
            cls._filesystem.mkdir(f"{cls._base_path}/deltalake/nopart", create_parents=True)

            cls._schema = cls._test_table.schema

            # use pyarrow library to write parquet files
            pq.write_to_dataset(cls._test_table, filesystem=cls._filesystem, compression='none',
                                existing_data_behavior='error', partition_cols=["Pregnancies"],
                                root_path=f"{cls._base_path}/parquet/part")
            pq.write_to_dataset(cls._test_table, filesystem=cls._filesystem, compression='none',
                                existing_data_behavior='error',
                                root_path=f"{cls._base_path}/parquet/nopart")

            # use deltalake library to write deltalake files
            dlt.write_deltalake(
                table_or_uri=f"{cls._base_path}/deltalake/part",
                data=cls._test_table, mode="error", partition_by=["Pregnancies"],
                storage_options=cls._local_filesystem_storage._get_deltalake_storage_options(),
                file_options=ds.ParquetFileFormat().make_write_options(compression='none'))
            dlt.write_deltalake(
                table_or_uri=f"{cls._base_path}/deltalake/nopart",
                data=cls._test_table, mode="error",
                storage_options=cls._local_filesystem_storage._get_deltalake_storage_options(),
                file_options=ds.ParquetFileFormat().make_write_options(compression='none'))
        finally:
            pass

    def test_localfilesystem_dataset_from_parquet_nopart(self):
        dataset = self._local_filesystem_storage.dataset(
            file_format="parquet",
            path=f"{self._base_path}/parquet/nopart"
        )

        self.assertEqual(
            first=set(self._schema.names).difference(set(dataset.schema.names)),
            second=set(),
            msg="Should match"
        )

    def test_localfilesystem_dataset_from_deltalake_nopart(self):
        dataset = self._local_filesystem_storage.dataset(
            file_format="deltalake",
            path=f"{self._base_path}/deltalake/nopart"
        )

        self.assertEqual(
            first=set(self._schema.names).difference(set(dataset.schema.names)),
            second=set(),
            msg="Should match"
        )

    def test_localfilesystem_dataset_from_parquet_part(self):
        dataset = self._local_filesystem_storage.dataset(
            file_format="parquet",
            path=f"{self._base_path}/parquet/part"
        )

        self.assertEqual(
            first=set(self._schema.names).difference(set(dataset.schema.names)),
            second=set(),
            msg="should match"
        )

    def test_localfilesystem_dataset_from_deltalake_part(self):
        dataset = self._local_filesystem_storage.dataset(
            file_format="deltalake",
            path=f"{self._base_path}/deltalake/part"
        )

        self.assertEqual(
            first=set(self._schema.names).difference(set(dataset.schema.names)),
            second=set(),
            msg="Should match"
        )