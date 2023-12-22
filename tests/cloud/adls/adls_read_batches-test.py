import os
import unittest

import pandas as pd
import pyarrow.parquet as pq
from adlfs import AzureBlobFileSystem
from azure.identity import ClientSecretCredential
from dotenv import load_dotenv

from cloud.adls import ADLSObjectStorage
from cloud.core import ParquetWriteOptions, DeltaLakeWriteOptions
from tests.cloud.core import ADLSTestBase

if __name__ == '__main__':
    unittest.main()


class TestADLSReadBatches(ADLSTestBase):
    _container_name = None
    _storage_account_name = None
    _base_path = None

    def __init__(self, *args, **kwargs):
        super(TestADLSReadBatches, self).__init__(*args, **kwargs)

        self._test_df = pd.read_csv('../../data/hmeq.csv')
        self._base_path = "test_adls_dataset"

    def test_read_batches_parquet_nopart(self):
        try:
            # write the table to ADLS
            self._adls_object_storage.write(table=self._test_df,
                                            file_format="parquet",
                                            path=self._base_path,
                                            write_options=ParquetWriteOptions(
                                                partitions=[],
                                                compression_codec="None",
                                                existing_data_behavior="overwrite_or_ignore")
                                            )

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