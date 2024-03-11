import ctypes
import uuid

import pandas as pd
import sas7bdat
from dotenv import load_dotenv
from sas7bdat import SAS7BDAT
import pyarrow as pa
import pyarrow.dataset as ds
from pandas.io.sas.sas7bdat import SAS7BDATReader

from tests.core.base_local_test import LocalFilesystemTestBase


class TestLocalFilesystemDataset(LocalFilesystemTestBase):

    @classmethod
    def setUpClass(cls):
        load_dotenv()

        cls._base_path = "../data/sas"
        cls._test_table = cls.make_mock_diabetes_arrow_table(random=True)

    @classmethod
    def tearDownClass(cls):
        pass

    def test_read_sas7bdat(self):
        with SAS7BDAT(f'{self._base_path}/manuf.sas7bdat', skip_header=True) as reader:
            print(reader.column_names)
            print(reader.column_types)
            print(reader.to_data_frame().info())

    def test_read_sas7bdat_pandas(self):
        def arrow_schema():
            with SAS7BDATReader(
                    path_or_buf=f'{self._base_path}/manuf.sas7bdat',
                    convert_dates=True,
                    blank_missing=True,
                    chunksize=100,
                    encoding='utf-8',
                    convert_text=True,
                ) as reader:

                def sasb7dat_col_pa_mapping(sas7bdat_col: sas7bdat.Column):
                    if sas7bdat_col.format == "": return (sas7bdat_col.name, pa.string())
                    elif sas7bdat_col.format == "int":
                        if sas7bdat_col.length == 8: return (sas7bdat_col.name, pa.int8())
                        elif sas7bdat_col.length == 16: return (sas7bdat_col.name, pa.int16())
                        elif sas7bdat_col.length == 32: return (sas7bdat_col.name, pa.int32())
                        return (sas7bdat_col.name, pa.in64())
                    elif sas7bdat_col.format == "DATETIME":
                        return (sas7bdat_col.name, pa.date32())
                    # elif isinstance(c_type, ctypes.c_bool): return pa.bool_(),
                    # elif isinstance(c_type, ctypes.c_float): return pa.float32(),
                    # elif isinstance(c_type, bytes):return pa.string()
                    # else: return None

                return pa.schema([sasb7dat_col_pa_mapping(column) for column in reader.columns])

        def iter_record_batches():
            # with pd.read_sas(filepath_or_buffer=f'{self._base_path}/manuf.sas7bdat',
            #                  chunksize=10, iterator=True) as reader:
            #     for df_chunk in reader:
            #         yield pa.RecordBatch.from_pandas(
            #             df_chunk
            #         )

            with SAS7BDATReader(
                    path_or_buf=f'{self._base_path}/manuf.sas7bdat',
                    convert_dates=True,
                    blank_missing=True,
                    chunksize=10,
                    encoding='utf-8',
                    convert_text=True,
            ) as reader:
                i = 0
                for df_chunk in reader:
                    print(i)
                    i+=0
                    yield pa.RecordBatch.from_pandas(df=df_chunk)

        ds.write_dataset(data=pa.RecordBatchReader.from_batches(arrow_schema(), iter_record_batches()),
                         format="parquet",
                         existing_data_behavior='delete_matching',
                         base_dir=f'{self._base_path}/write/table.parquet')