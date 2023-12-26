import pandas as pd

from cloud.core import ParquetWriteOptions
from tests.core import ADLSTestBase


class TestADLSReadBatches(ADLSTestBase):

    _base_path = None

    @classmethod
    def setUpClass(cls):
        cls._base_path = "../data/diabetes"

    def test_read_batches_parquet_nopart(self):
        try:

            batches = self._adls_object_storage.read_batches(
                file_format="parquet",
                path=self._base_path,
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
        finally:
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=self._base_path
            )