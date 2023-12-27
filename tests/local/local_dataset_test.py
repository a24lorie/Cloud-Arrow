import pandas as pd
import pyarrow as pa


from tests.core.base_local_test import LocalFilesystemTestBase


class TestLocalFilesystemDataset(LocalFilesystemTestBase):

    _base_path = None
    _schema = None

    @classmethod
    def setUpClass(cls):
        LocalFilesystemTestBase.setUpClass()

        cls._base_path = "data/diabetes"
        arr_table = pa.Table.from_pandas(pd.read_csv(f"{cls._base_path}/csv/nopart/diabetes.csv"))
        cls._schema = arr_table.schema

        # import pyarrow.parquet as pq
        # pq.write_to_dataset(arr_table, filesystem=cls._filesystem, compression='none',
        #                     existing_data_behavior='error', partition_cols=["Pregnancies"],
        #                     root_path=cls._local_filesystem_storage._get_filesystem_base_path(f"{cls._base_path}/parquet/part"))
        # pq.write_to_dataset(arr_table, filesystem=cls._filesystem, compression='none', existing_data_behavior='error',
        #                     root_path=cls._local_filesystem_storage._get_filesystem_base_path(f"{cls._base_path}/parquet/nopart"))

        # import deltalake as dlt
        # import pyarrow.dataset as ds
        # dlt.write_deltalake(
        #     table_or_uri=cls._local_filesystem_storage._get_deltalake_url(f"{cls._base_path}/deltalake/part"),
        #     data=arr_table, mode="error", partition_by=["Pregnancies"],
        #     storage_options=cls._local_filesystem_storage._get_deltalake_storage_options(),
        #     file_options=ds.ParquetFileFormat().make_write_options(compression='none'))
        # dlt.write_deltalake(
        #     table_or_uri=cls._local_filesystem_storage._get_deltalake_url(f"{cls._base_path}/deltalake/nopart"),
        #     data=arr_table, mode="error",
        #     storage_options=cls._local_filesystem_storage._get_deltalake_storage_options(),
        #     file_options=ds.ParquetFileFormat().make_write_options(compression='none'))

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
