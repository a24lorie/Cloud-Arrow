import deltalake as dlt
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from tests.core import ADLSTestBase


class TestADLSReadBatches(ADLSTestBase):

    _base_path = None
    _test_df = None

    @classmethod
    def setUpClass(cls):
        ADLSTestBase.setUpClass()

        cls._base_path = "write"
        cls._test_df = pd.read_csv("../data/diabetes/csv/nopart/diabetes.csv")

        try:
            # write parquet directories
            ADLSTestBase._get_filesystem_client().create_directory(f"{cls._base_path}/parquet/part")
            ADLSTestBase._get_filesystem_client().create_directory(f"{cls._base_path}/parquet/nopart")
            ADLSTestBase._get_filesystem_client().create_directory(f"{cls._base_path}/deltalake/part")
            ADLSTestBase._get_filesystem_client().create_directory(f"{cls._base_path}/deltalake/nopart")

            arr_table = pa.Table.from_pandas(cls._test_df)
            cls._schema = arr_table.schema

            # use pyarrow library to write parquet files
            pq.write_to_dataset(arr_table, filesystem=cls._filesystem, compression='none',
                                existing_data_behavior='error', partition_cols=["Pregnancies"],
                                root_path=cls._adls_object_storage._get_filesystem_base_path(
                                    f"{cls._base_path}/parquet/part"))
            pq.write_to_dataset(arr_table, filesystem=cls._filesystem, compression='none',
                                existing_data_behavior='error',
                                root_path=cls._adls_object_storage._get_filesystem_base_path(
                                    f"{cls._base_path}/parquet/nopart"))

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

    def test_adls_read_batches_parquet_nopart(self):
        batches = self._adls_object_storage.read_batches(
            file_format="parquet",
            path=f"{self._base_path}/parquet/nopart",
            batch_size=1000
        )

        count = 0

        for batch in batches:
            count += batch.num_rows

        # for scan_batch in dataset.scanner(batch_size=100):
        #     table = scan_batch.to_table()
        #     count += table.num_rows

        count_test_df = len(self._test_df.index)

        self.assertEqual(
            count,
            count_test_df,
            "Should match"
        )

    def test_adls_read_batches_parquet_part(self):
        batches = self._adls_object_storage.read_batches(
            file_format="parquet",
            path=f"{self._base_path}/parquet/part",
            batch_size=1000
        )

        count = 0

        for batch in batches:
            count += batch.num_rows

        # for scan_batch in dataset.scanner(batch_size=100):
        #     table = scan_batch.to_table()
        #     count += table.num_rows

        count_test_df = len(self._test_df.index)

        self.assertEqual(
            count,
            count_test_df,
            "Should match"
        )

    def test_adls_read_batches_deltalake_nopart(self):
        batches = self._adls_object_storage.read_batches(
            file_format="deltalake",
            path=f"{self._base_path}/deltalake/nopart",
            batch_size=1000
        )

        count = 0

        for batch in batches:
            count += batch.num_rows

        # for scan_batch in dataset.scanner(batch_size=100):
        #     table = scan_batch.to_table()
        #     count += table.num_rows

        count_test_df = len(self._test_df.index)

        self.assertEqual(
            count,
            count_test_df,
            "Should match"
        )

    def test_adls_read_batches_deltalake_part(self):
        batches = self._adls_object_storage.read_batches(
            file_format="deltalake",
            path=f"{self._base_path}/deltalake/part",
            batch_size=1000
        )

        count = 0

        for batch in batches:
            count += batch.num_rows

        # for scan_batch in dataset.scanner(batch_size=100):
        #     table = scan_batch.to_table()
        #     count += table.num_rows

        count_test_df = len(self._test_df.index)

        self.assertEqual(
            count,
            count_test_df,
            "Should match"
        )