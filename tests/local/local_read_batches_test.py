import pandas as pd

from tests.core import LocalFilesystemTestBase


class TestLocalFilesystemReadBatches(LocalFilesystemTestBase):

    _base_path = None

    @classmethod
    def setUpClass(cls):
        LocalFilesystemTestBase.setUpClass()

        cls._base_path = "data/diabetes"
        cls._test_df = pd.read_csv('data/diabetes/csv/nopart/diabetes.csv')

    def test_localfilesystem_read_batches_parquet_nopart(self):

        batches = self._local_filesystem_storage.read_batches(
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

    def test_localfilesystem_read_batches_parquet_part(self):

        batches = self._local_filesystem_storage.read_batches(
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

    def test_localfilesystem_read_batches_deltalake_nopart(self):

        batches = self._local_filesystem_storage.read_batches(
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

    def test_localfilesystem_read_batches_deltalake_part(self):

        batches = self._local_filesystem_storage.read_batches(
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