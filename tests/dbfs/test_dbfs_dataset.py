import traceback

import deltalake as dlt
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from tests.core.base_dbfs_test import DBFSTestBase


class TestDBFSDataset(DBFSTestBase):
    _base_path = None
    _test_df = None
    _schema = None

    @classmethod
    def setUpClass(cls):
        DBFSTestBase.setUpClass()

        cls._base_path = "/FileStore/write"
        cls._test_df = pd.read_csv("../../../filesystem_spec/fsspec/implementations/tests/data/diabetes.csv")

        try:
            # write parquet directories
            cls._filesystem.makedir(path=f"{cls._base_path}/parquet/part", create_parents=True)
            cls._filesystem.makedir(path=f"{cls._base_path}/parquet/nopart", create_parents=True)
            cls._filesystem.makedir(path=f"{cls._base_path}/deltalake/part", create_parents=True)
            cls._filesystem.makedir(path=f"{cls._base_path}/deltalake/nopart", create_parents=True)

            arr_table = pa.Table.from_pandas(cls._test_df)
            cls._schema = arr_table.schema

            # use pyarrow library to write parquet files
            pq.write_to_dataset(arr_table, filesystem=cls._filesystem, compression='none',
                    existing_data_behavior='error', partition_cols=["Pregnancies"],
                    root_path=cls._dbfs_object_storage._get_filesystem_base_path(f"{cls._base_path}/parquet/part"),
                    use_threads= False)
            pq.write_to_dataset(arr_table, filesystem=cls._filesystem, compression='none',
                    existing_data_behavior='error',
                    root_path=cls._dbfs_object_storage._get_filesystem_base_path(f"{cls._base_path}/parquet/nopart"),
                    use_threads= False)

            # use deltalake library to write deltalake files
            dlt.write_deltalake(
                table_or_uri=cls._dbfs_object_storage._get_deltalake_url(f"{cls._base_path}/deltalake/part"),
                data=arr_table, mode="error", partition_by=["Pregnancies"],
                storage_options=cls._dbfs_object_storage._get_deltalake_storage_options(),
                file_options=ds.ParquetFileFormat().make_write_options(compression='none'))
            dlt.write_deltalake(
                table_or_uri=cls._dbfs_object_storage._get_deltalake_url(f"{cls._base_path}/deltalake/nopart"),
                data=arr_table, mode="error",
                storage_options=cls._dbfs_object_storage._get_deltalake_storage_options(),
                file_options=ds.ParquetFileFormat().make_write_options(compression='none'))
        except Exception as err:
            traceback.print_exc()
            raise  err
        finally:
            pass

    @classmethod
    def tearDownClass(cls):
        # Remove write directory recursively
        cls._delete_dbfs_dir(f"{cls._base_path}")

    def test_dbfs_dataset_from_parquet_nopart(self):
        try:
            dataset = self._dbfs_object_storage.dataset(
                file_format="parquet",
                path=f"{self._base_path}/parquet/nopart"
            )

            self.assertEqual(
                first=set(self._schema.names).difference(set(dataset.schema.names)),
                second=set(),
                msg="Should match"
            )
        finally:
            pass


    # def test_dbfs_dataset_from_deltalake_nopart(self):
    #     dataset = self._dbfs_object_storage.dataset(
    #         file_format="deltalake",
    #         path=f"{self._base_path}/deltalake/nopart"
    #     )
    #
    #     self.assertEqual(
    #         first=set(self._schema.names).difference(set(dataset.schema.names)),
    #         second=set(),
    #         msg="Should match"
    #     )
    #
    # def test_dbfs_dataset_from_parquet_part(self):
    #     dataset = self._dbfs_object_storage.dataset(
    #         file_format="parquet",
    #         path=f"{self._base_path}/parquet/part"
    #     )
    #
    #     self.assertEqual(
    #         first=set(self._schema.names).difference(set(dataset.schema.names)),
    #         second=set(),
    #         msg="Should match"
    #     )
    #
    # def test_dbfs_dataset_from_deltalake_part(self):
    #     dataset = self._dbfs_object_storage.dataset(
    #         file_format="deltalake",
    #         path=f"{self._base_path}/deltalake/part"
    #     )
    #
    #     self.assertEqual(
    #         first=set(self._schema.names).difference(set(dataset.schema.names)),
    #         second=set(),
    #         msg="Should match"
    #     )