import deltalake as dlt
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from tests.core import ADLSTestBase


class TestADLSDataset(ADLSTestBase):
    _base_path = None
    _test_table = None
    _schema = None

    @classmethod
    def setUpClass(cls):
        ADLSTestBase.setUpClass()

        cls._base_path = "write"
        cls._test_table = cls.make_mock_diabetes_arrow_table()

        try:
            # write parquet directories
            ADLSTestBase._get_filesystem_client().create_directory(f"{cls._base_path}/parquet/part")
            ADLSTestBase._get_filesystem_client().create_directory(f"{cls._base_path}/parquet/nopart")
            ADLSTestBase._get_filesystem_client().create_directory(f"{cls._base_path}/deltalake/part")
            ADLSTestBase._get_filesystem_client().create_directory(f"{cls._base_path}/deltalake/nopart")

            arr_table = cls._test_table
            cls._schema = arr_table.schema

            # use pyarrow library to write parquet files
            pq.write_to_dataset(arr_table, filesystem=cls._filesystem, compression='none',
                    existing_data_behavior='error', partition_cols=["Pregnancies"],
                    root_path=cls._adls_object_storage._get_filesystem_base_path(f"{cls._base_path}/parquet/part"))
            pq.write_to_dataset(arr_table, filesystem=cls._filesystem, compression='none', existing_data_behavior='error',
                    root_path=cls._adls_object_storage._get_filesystem_base_path(f"{cls._base_path}/parquet/nopart"))

            # use deltalake library to write deltalake files
            dlt.write_deltalake(
                table_or_uri=cls._adls_object_storage._get_deltalake_url(f"{cls._base_path}/deltalake/part"),
                data=arr_table, mode="error", partition_by=["Pregnancies"],
                storage_options=cls._adls_object_storage._get_deltalake_storage_options(),
                file_options=ds.ParquetFileFormat().make_write_options(compression='none'))
            dlt.write_deltalake(
                table_or_uri=cls._adls_object_storage._get_deltalake_url(f"{cls._base_path}/deltalake/nopart"),
                data=arr_table, mode="error",
                storage_options=cls._adls_object_storage._get_deltalake_storage_options(),
                file_options=ds.ParquetFileFormat().make_write_options(compression='none'))
        finally:
            pass

    @classmethod
    def tearDownClass(cls):
        # Remove write directory recursively
        cls._delete_adls_dir(f"{cls._base_path}")

    def test_adls_dataset_from_parquet_nopart(self):
        try:
            dataset = self._adls_object_storage.dataset(
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

    def test_adls_dataset_from_deltalake_nopart(self):
        dataset = self._adls_object_storage.dataset(
            file_format="deltalake",
            path=f"{self._base_path}/deltalake/nopart"
        )

        self.assertEqual(
            first=set(self._schema.names).difference(set(dataset.schema.names)),
            second=set(),
            msg="Should match"
        )

    def test_adls_dataset_from_parquet_part(self):
        dataset = self._adls_object_storage.dataset(
            file_format="parquet",
            path=f"{self._base_path}/parquet/part"
        )

        self.assertEqual(
            first=set(self._schema.names).difference(set(dataset.schema.names)),
            second=set(),
            msg="Should match"
        )

    def test_adls_dataset_from_deltalake_part(self):
        dataset = self._adls_object_storage.dataset(
            file_format="deltalake",
            path=f"{self._base_path}/deltalake/part"
        )

        self.assertEqual(
            first=set(self._schema.names).difference(set(dataset.schema.names)),
            second=set(),
            msg="Should match"
        )