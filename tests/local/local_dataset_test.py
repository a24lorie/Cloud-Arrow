from tests.core.base_local_test import LocalFilesystemTestBase


class TestLocalFilesystemDataset(LocalFilesystemTestBase):
    _base_path = None

    def __init__(self, *args, **kwargs):
        super(TestLocalFilesystemDataset, self).__init__(*args, **kwargs)

        self._base_path = "../data/diabetes"

    def test_dataset_from_parquet_nopart(self):
        dataset = self._local_filesystem_storage.dataset(
            file_format="parquet",
            path=f"{self._base_path}/parquet/nopart"
        )

        schema_str = dataset.schema.to_string(show_field_metadata=False, show_schema_metadata=False)

        self.assertEqual(
            first="Glucose: int32\nBloodPressure: int32\nSkinThickness: int32\nInsulin: int32\nBMI: double\n"
                  "DiabetesPedigreeFunction: double\nAge: int32\nOutcome: int32\nPregnancies: int32",
            second=schema_str,
            msg="Should match"
        )

    def test_dataset_from_deltalake_nopart(self):
        dataset = self._local_filesystem_storage.dataset(
            file_format="deltalake",
            path=f"{self._base_path}/deltalake/nopart"
        )

        schema_str = dataset.schema.to_string(show_field_metadata=False, show_schema_metadata=False)

        self.assertEqual(
            first="Glucose: int32\nBloodPressure: int32\nSkinThickness: int32\nInsulin: int32\nBMI: double\n"
                  "DiabetesPedigreeFunction: double\nAge: int32\nOutcome: int32\nPregnancies: int32",
            second=schema_str,
            msg="Should match"
        )

    def test_dataset_from_parquet_part(self):
        dataset = self._local_filesystem_storage.dataset(
            file_format="parquet",
            path=f"{self._base_path}/parquet/part"
        )

        schema_str = dataset.schema.to_string(show_field_metadata=False, show_schema_metadata=False)

        self.assertEqual(
            first="Glucose: int32\nBloodPressure: int32\nSkinThickness: int32\nInsulin: int32\nBMI: double\n"
                  "DiabetesPedigreeFunction: double\nAge: int32\nOutcome: int32\nPregnancies: int32",
            second=schema_str,
            msg="Should match"
        )

    def test_dataset_from_deltalake_part(self):
        dataset = self._local_filesystem_storage.dataset(
            file_format="deltalake",
            path=f"{self._base_path}/deltalake/part"
        )

        schema_str = dataset.schema.to_string(show_field_metadata=False, show_schema_metadata=False)

        self.assertEqual(
            first="Glucose: int32\nBloodPressure: int32\nSkinThickness: int32\nInsulin: int32\nBMI: double\n"
                  "DiabetesPedigreeFunction: double\nAge: int32\nOutcome: int32\nPregnancies: int32",
            second=schema_str,
            msg="Should match"
        )
