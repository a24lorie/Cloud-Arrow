import pandas as pd

from cloud.core import ParquetWriteOptions
from tests.core import ADLSTestBase


class TestADLSDataset(ADLSTestBase):

    _base_path = None

    def __init__(self, *args, **kwargs):
        super(TestADLSDataset, self).__init__(*args, **kwargs)

        self._test_df = pd.read_csv('data/hmeq.csv')
        self._base_path = "test_adls_dataset"

    def test_dataset_from_parquet_nopart(self):

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

            dataset = self._adls_object_storage.dataset(
                file_format="parquet",
                path=self._base_path
            )

            schema_str = dataset.schema.to_string(show_field_metadata=False, show_schema_metadata=False)

            self.assertEqual(
                first="BAD: int64\nLOAN: int64\nMORTDUE: double\nVALUE: double\nREASON: string\nJOB: string\nYOJ: "
                      "double\nDEROG: double\nDELINQ: double\nCLAGE: double\nNINQ: double\nCLNO: double\nDEBTINC: "
                      "double",
                second=schema_str,
                msg="Should match"
            )
        finally:
            self._delete_adls_dir(
                storage_account=self._storage_account_name,
                container=self._container_name,
                path=self._base_path
            )
