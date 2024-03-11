import unittest

import numpy
import pandas as pd
import pyarrow as pa
from dotenv import load_dotenv


class TestBase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        load_dotenv()

    @classmethod
    def mock_random_diabetes_arrow_batchReader(cls) -> pa.RecordBatchReader:
        schema = pa.schema([
                ("Pregnancies", pa.int64()),
                ("Glucose", pa.int64()),
                ("BloodPressure", pa.int64()),
                ("SkinThickness", pa.int64()),
                ("Insulin", pa.int64()),
                ("BMI", pa.float64()),
                ("DiabetesPedigreeFunction", pa.float64()),
                ("Age", pa.int64()),
                ("Outcome", pa.int64())]
        )
        pregnancies = pa.array(numpy.random.randint(low=0, high=17, size=5))
        glucose = pa.array(numpy.random.randint(low=0, high=199, size=5))
        blood_pressure = pa.array(numpy.random.randint(low=0, high=122, size=5))
        skin_thickness = pa.array(numpy.random.randint(low=0, high=99, size=5))
        insulin = pa.array(numpy.random.randint(low=0, high=846, size=5))
        bmi = pa.array(numpy.random.uniform(0.0, 67.1, size=5))
        diabetes_pedigree_function = pa.array(numpy.random.uniform(0.08, 2.42, size=5))
        age = pa.array(numpy.random.randint(low=21, high=81, size=5))
        outcome = pa.array(numpy.random.randint(low=0, high=1, size=5))

        def iter_record_batches():
            for i in range(5):
                yield pa.RecordBatch.from_arrays([
                    pregnancies, glucose, blood_pressure, skin_thickness,
                    insulin, bmi, diabetes_pedigree_function, age, outcome
                ], schema=schema)

        return pa.RecordBatchReader.from_batches(schema, iter_record_batches())

    @classmethod
    def mock_static_diabetes_arrow_batchReader(cls) -> pa.RecordBatchReader:
        schema = pa.schema([
            ("Pregnancies", pa.int64()),
            ("Glucose", pa.int64()),
            ("BloodPressure", pa.int64()),
            ("SkinThickness", pa.int64()),
            ("Insulin", pa.int64()),
            ("BMI", pa.float64()),
            ("DiabetesPedigreeFunction", pa.float64()),
            ("Age", pa.int64()),
            ("Outcome", pa.int64())]
        )

        def iter_record_batches():
            with pd.read_csv("../data/diabetes/csv/nopart/diabetes.csv", chunksize=100) as reader:
                for chunk in reader:
                    yield pa.RecordBatch.from_arrays([
                        chunk["Pregnancies"],
                        chunk["Glucose"],
                        chunk["BloodPressure"],
                        chunk["SkinThickness"],
                        chunk["Insulin"],
                        chunk["BMI"],
                        chunk["DiabetesPedigreeFunction"],
                        chunk["Age"],
                        chunk["Outcome"]
                    ], schema=schema)

        return pa.RecordBatchReader.from_batches(schema, iter_record_batches())

    @classmethod
    def make_mock_diabetes_arrow_table(cls, random=True) -> pa.Table:
        if random:
            return pa.Table.from_batches(batches=cls.mock_random_diabetes_arrow_batchReader())
        else:
            return pa.Table.from_batches(batches=cls.mock_static_diabetes_arrow_batchReader())
