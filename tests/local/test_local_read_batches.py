import deltalake as dlt
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from tests.core import LocalFilesystemTestBase


class TestLocalFilesystemReadBatches(LocalFilesystemTestBase):
    _base_path = None
    _schema = None

    @classmethod
    def setUpClass(cls):
        LocalFilesystemTestBase.setUpClass()

        try:
            # write parquet directories
            cls._filesystem.mkdir(f"{cls._base_path}/parquet/part", create_parents=True)
            cls._filesystem.mkdir(f"{cls._base_path}/parquet/static/part/", create_parents=True)
            cls._filesystem.mkdir(f"{cls._base_path}/parquet/nopart", create_parents=True)
            cls._filesystem.mkdir(f"{cls._base_path}/parquet/static/nopart/", create_parents=True)
            cls._filesystem.mkdir(f"{cls._base_path}/deltalake/part", create_parents=True)
            cls._filesystem.mkdir(f"{cls._base_path}/deltalake/static/part/", create_parents=True)
            cls._filesystem.mkdir(f"{cls._base_path}/deltalake/nopart", create_parents=True)
            cls._filesystem.mkdir(f"{cls._base_path}/deltalake/static/nopart/", create_parents=True)


            cls._schema = cls._test_table.schema

            # use pyarrow library to write parquet files
            # use pyarrow library to write parquet files
            pq.write_to_dataset(cls._test_table, filesystem=cls._filesystem, compression='none',
                                existing_data_behavior='error', partition_cols=["Pregnancies"],
                                root_path=f"{cls._base_path}/parquet/part")
            pq.write_to_dataset(cls._fixed_table, filesystem=cls._filesystem, compression='none',
                                existing_data_behavior='error', partition_cols=["Pregnancies"],
                                root_path=f"{cls._base_path}/parquet/static/part")
            pq.write_to_dataset(cls._test_table, filesystem=cls._filesystem, compression='none',
                                existing_data_behavior='error',
                                root_path=f"{cls._base_path}/parquet/nopart")
            pq.write_to_dataset(cls._fixed_table, filesystem=cls._filesystem, compression='none',
                                existing_data_behavior='error',
                                root_path=f"{cls._base_path}/parquet/static/nopart")

            # use deltalake library to write deltalake files
            dlt.write_deltalake(
                table_or_uri=f"{cls._base_path}/deltalake/part",
                data=cls._test_table, mode="error", partition_by=["Pregnancies"],
                storage_options=cls._local_filesystem_storage._get_deltalake_storage_options(),
                file_options=ds.ParquetFileFormat().make_write_options(compression='none'))
            dlt.write_deltalake(
                table_or_uri=f"{cls._base_path}/deltalake/static/part",
                data=cls._fixed_table, mode="error", partition_by=["Pregnancies"],
                storage_options=cls._local_filesystem_storage._get_deltalake_storage_options(),
                file_options=ds.ParquetFileFormat().make_write_options(compression='none'))
            dlt.write_deltalake(
                table_or_uri=f"{cls._base_path}/deltalake/nopart",
                data=cls._test_table, mode="error",
                storage_options=cls._local_filesystem_storage._get_deltalake_storage_options(),
                file_options=ds.ParquetFileFormat().make_write_options(compression='none'))
            dlt.write_deltalake(
                table_or_uri=f"{cls._base_path}/deltalake/static/nopart",
                data=cls._fixed_table, mode="error",
                storage_options=cls._local_filesystem_storage._get_deltalake_storage_options(),
                file_options=ds.ParquetFileFormat().make_write_options(compression='none'))
        finally:
            pass

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

        count_test_table = self._test_table.num_rows

        self.assertEqual(
            count,
            count_test_table,
            "Should match"
        )

    def test_localfilesystem_read_batches_parquet_nopart_filters(self):

        batches = self._local_filesystem_storage.read_batches(
            file_format="parquet",
            path=f"{self._base_path}/parquet/static/nopart",
            filters=(ds.field("Pregnancies") == 0),
            batch_size=40
        )

        count = 0

        for batch in batches:
            count += batch.num_rows

        self.assertEqual(
            count,
            111,
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

        count_test_table = self._test_table.num_rows

        self.assertEqual(
            count,
            count_test_table,
            "Should match"
        )

    def test_localfilesystem_read_batches_parquet_part_filters(self):
        batches = self._local_filesystem_storage.read_batches(
            file_format="parquet",
            path=f"{self._base_path}/parquet/static/part",
            filters=(ds.field("Pregnancies") == 0),
            batch_size=40
        )

        count = 0

        for batch in batches:
            count += batch.num_rows

        self.assertEqual(
            count,
            111,
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

        count_test_table = self._test_table.num_rows

        self.assertEqual(
            count,
            count_test_table,
            "Should match"
        )

    def test_localfilesystem_read_batches_deltalake_nopart_filters(self):

        batches = self._local_filesystem_storage.read_batches(
            file_format="deltalake",
            path=f"{self._base_path}/deltalake/static/nopart",
            filters=(ds.field("Pregnancies") == 0),
            batch_size=40
        )

        count = 0

        for batch in batches:
            count += batch.num_rows

        self.assertEqual(
            count,
            111,
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

        count_test_table = self._test_table.num_rows

        self.assertEqual(
            count,
            count_test_table,
            "Should match"
        )

    def test_localfilesystem_read_batches_deltalake_part_filters(self):

        batches = self._local_filesystem_storage.read_batches(
            file_format="deltalake",
            path=f"{self._base_path}/deltalake/static/part",
            filters=(ds.field("Pregnancies") == 0),
            batch_size=40
        )

        count = 0

        for batch in batches:
            count += batch.num_rows

        self.assertEqual(
            count,
            111,
            "Should match"
        )