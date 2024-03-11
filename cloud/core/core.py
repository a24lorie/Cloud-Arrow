import logging
from abc import ABCMeta, abstractmethod
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from deltalake import DeltaTable, write_deltalake
from pandas import DataFrame

__all__ = ['Condition',
           'ConditionFactory',
           'ParquetWriteOptions',
           'DeltaLakeWriteOptions',
           'WriteOptions',
           'AbstractStorage']

"""
The following is a context-free grammar for DNF:
    DNF → (Conjunction) ∨ DNF
    DNF → (Conjunction)
    Conjunction → Literal ∧  Conjunction
    Conjunction → Literal
    Literal → ¬ Variable
    Literal → Variable
"""


class Condition(metaclass=ABCMeta):
    def __init__(self, key, value_1):
        """
        :param key:
        :param value_1:
        """
        self._key = key
        self._value_1 = value_1
        self._type_1 = type(value_1)

    @abstractmethod
    def gen_expression(self, **kwargs):
        """
        Abstract definition for getting the operation tuple
        Returns
        -------
        :return: condition : Tuple
        """
        pass


class UnaryCondition(Condition, metaclass=ABCMeta):
    def __init__(self, key, value_1):
        """
        :param key:
        :param value_1:
        """
        super().__init__(key, value_1)

    @abstractmethod
    def gen_expression(self, **kwargs):
        """
        Abstract definition for getting the operation tuple
        Returns
        -------
        :return: condition : Tuple
        """
        pass


class BinaryCondition(Condition, metaclass=ABCMeta):
    def __init__(self, key, value_1, value_2):
        """
        :param key:
        :param value_1:
        :param value_2:
        """
        super().__init__(key, value_1)
        self._value_2 = value_2
        self._type_2 = type(value_2)

    @abstractmethod
    def gen_expression(self, **kwargs):
        """
        Abstract definition for getting the operation tuple
        Returns
        -------
        :return: condition : Tuple
        """
        pass


class Eq(UnaryCondition, metaclass=ABCMeta):
    def gen_expression(self, **kwargs):
        """
        Definition for getting the operation tuple for the EQ operator
        Returns
        -------
        :return: condition : Tuple
        """
        return ds.field(self._key) == self._value_1


class Neq(UnaryCondition, metaclass=ABCMeta):
    def gen_expression(self, **kwargs):
        """
        Definition for getting the operation tuple for the NEQ operator
        Returns
        -------
        :return: condition : Tuple
        """
        return (ds.field(self._key) != self._value_1)


class Lt(UnaryCondition, metaclass=ABCMeta):
    def __init__(self, key, value_1, equals=False):
        super().__init__(key, value_1)
        self._equals = equals

    def gen_expression(self, **kwargs):
        """
        Abstract definition for getting the operation tuple
        Returns
        -------
        :return: condition : Tuple
        """
        if self._equals:
            return (ds.field(self._key) <= self._value_1)
        else:
            return (ds.field(self._key) < self._value_1)


class Gt(UnaryCondition, metaclass=ABCMeta):
    def __init__(self, key, value_1, equals=False):
        super().__init__(key, value_1)
        self._equals = equals

    def gen_expression(self, **kwargs):
        """
        Definition for getting the operation tuple for the GT operator
        Returns
         -------
        :return: condition : Tuple
        """
        if self._equals:
            return (ds.field(self._key) >= self._value_1)
        else:
            return (ds.field(self._key) > self._value_1)


class Btw(BinaryCondition, metaclass=ABCMeta):
    def gen_expression(self, **kwargs):
        """
        Definition for getting the operation tuple for the BTW operator
        Returns
        -------
        :return: condition : Tuple
        """
        return ((ds.field(self._key) >= self._value_1) & (ds.field(self._key) <= self._value_2))


class ConditionFactory:
    @staticmethod
    def get_condition(condition_name, key, value_1, value_2) -> Condition:
        """
        :param condition_name: Literal value representing the condition to be instanced
        :param key: Name of the field
        :param value_1: Value of the field1
        :param value_2: Value od the field2 only valid for the btw condition
        :return:
        """

        if condition_name == "eq":
            condition = Eq(key, value_1)
        elif condition_name == "neq":
            condition = Neq(key, value_1)
        elif condition_name == "lt":
            condition = Lt(key, value_1, equals=False)
        elif condition_name == "lte":
            condition = Lt(key, value_1, equals=True)
        elif condition_name == "gt":
            condition = Gt(key, value_1, equals=False)
        elif condition_name == "gte":
            condition = Gt(key, value_1, equals=True)
        elif condition_name == "btw":
            condition = Btw(key, value_1, value_2)
        else:
            raise ValueError("type should be one of: ['eq', 'neq', 'lt', 'lte', 'gt', 'gte', 'btw'].")
        return condition


class WriteOptions(metaclass=ABCMeta):
    def __init__(self,
                 partitions: list[str],
                 compression_codec: str):
        self._partitions = partitions
        self._compression_codec = compression_codec

    @property
    def compression_codec(self) -> str:
        return self._compression_codec

    @property
    def partitions(self) -> list[str]:
        return self._partitions

    @abstractmethod
    def existing_data_behavior(self):
        pass


class ParquetWriteOptions(WriteOptions, metaclass=ABCMeta):
    def __init__(self,
                 partitions: list[str],
                 compression_codec: str,
                 existing_data_behavior: str):
        super().__init__(partitions=partitions, compression_codec=compression_codec)

        if existing_data_behavior not in ['error', 'overwrite_or_ignore', 'delete_matching']:
            raise ValueError("existing_data_behavior should be one of ['error', 'overwrite_or_ignore', "
                             "'delete_matching'].")

        self._existing_data_behavior = existing_data_behavior

    def existing_data_behavior(self):
        return self._existing_data_behavior


class DeltaLakeWriteOptions(WriteOptions, metaclass=ABCMeta):
    def __init__(self,
                 partitions: list[str],
                 compression_codec: str,
                 existing_data_behavior: str):
        super().__init__(partitions=partitions, compression_codec=compression_codec)

        if existing_data_behavior not in ['error', 'append', 'overwrite', 'ignore']:
            raise ValueError("existing_data_behavior should be one of ['error', 'append', 'overwrite', 'ignore']")

        self._existing_data_behavior = existing_data_behavior

    def existing_data_behavior(self):
        return self._existing_data_behavior


class AbstractStorage(metaclass=ABCMeta):
    def __init__(self):
        self._logger = logging.getLogger('cloud_arrow')

    def _validate_format(self, file_format):
        if file_format not in ["parquet", "deltalake"]:
            error_msg = f"The file_format must be one of: 'parquet', 'deltalake'"
            self._logger.error(error_msg)
            raise ValueError(error_msg)

    @staticmethod
    def _normalize_path(path) -> str:
        """

        :param path:
        :return:
        """
        if path is None or path == "":
            raise "The path argument can not be empty"

        return path[:-1] if path.endswith("/") else path

    @abstractmethod
    def _get_filesystem_base_path(self, path) -> str:
        pass

    @abstractmethod
    def _get_filesystem(self) -> Any:
        """

        :return: fsspec or pyarrow filesystem, default None
        """
        pass

    @abstractmethod
    def _get_deltalake_storage_options(self):
        pass

    @abstractmethod
    def _get_deltalake_url(self, path) -> str:
        pass

    def dataset(self,
                file_format: str,
                path: str,
                partitioning: str = "hive") -> ds.Dataset:
        """
        Open a dataset.

        Datasets provides functionality to efficiently work with tabular,
        potentially larger than memory and multi-file dataset.

        - A unified interface for different sources, like Parquet and Deltalake
        Parameters
        ----------
        :param file_format: str
            Currently "parquet", "deltalake" supported.
        :param path: str
            Path pointing to a single file:
                Open a Dataset from a single file.
            Path pointing to a directory:
                The directory gets discovered recursively according to a
                partitioning scheme if given.
        :param partitioning: Partitioning, PartitioningFactory, str, list of str default "hive"
            The partitioning scheme specified with the ``partitioning()``
            function. A flavor string can be used as shortcut, and with a list of
            field names a DirectionaryPartitioning will be inferred.
        :return:
        -------
        dataset : Dataset
            pyarrow.Dataset
        """
        self._validate_format(file_format=file_format)
        filesystem = self._get_filesystem()

        if file_format == "parquet":
            return ds.dataset(
                source=self._get_filesystem_base_path(path=path),
                filesystem=filesystem,
                partitioning=partitioning
            )
        elif file_format == "deltalake":
            return DeltaTable(
                table_uri=self._get_deltalake_url(path=AbstractStorage._normalize_path(path)),
                storage_options=self._get_deltalake_storage_options()
            ).to_pyarrow_dataset()

    def read_batches(self,
                     file_format: str,
                     path: str,
                     partitioning: str = "hive",
                     batch_size: int = 1000,
                     filters=None) -> pa.RecordBatch:
        """
        Read the dataset as materialized record batches.

        Parameters
        ----------
        :param file_format: str
            Currently "parquet", "deltalake" supported.
        :param path: str
            Path pointing to a single file:
                Open a Dataset from a single file.
            Path pointing to a directory:
                The directory gets discovered recursively according to a
                partitioning scheme if given.
        :param partitioning: Partitioning, PartitioningFactory, str, list of str default "hive"
            The partitioning scheme specified with the ``partitioning()``
            function. A flavor string can be used as shortcut, and with a list of
            field names a DirectionaryPartitioning will be inferred.
        :param batch_size:  int, default 16
            The number of batches to read ahead in a file. This might not work
            for all file formats. Increasing this number will increase
            RAM usage but could also improve IO utilization.
        :param filters: Expression, default None
            Scan will return only the rows matching the filter.
            If possible the predicate will be pushed down to exploit the
            partition information or internal metadata found in the data
            source, e.g. Parquet statistics. Otherwise filters the loaded
            RecordBatches before yielding them.
        :return:
            record_batches : iterator of RecordBatch
        """

        return self.dataset(
            file_format=file_format,
            path=path,
            partitioning=partitioning
        ).to_batches(
            batch_size=batch_size,
            filter=filters
        )

    def read_to_arrow_table(self,
                            file_format: str,
                            path: str,
                            partitioning: str = "hive",
                            filters=None) -> pa.Table:
        """
        Read the dataset as arrow table.

        Parameters
        ----------
        :param file_format: str
            Currently "parquet", "deltalake" supported.
        :param path: str
            Path pointing to a single file:
                Open a Dataset from a single file.
            Path pointing to a directory:
                The directory gets discovered recursively according to a
                partitioning scheme if given.
        :param partitioning: Partitioning, PartitioningFactory, str, list of str default "hive"
            The partitioning scheme specified with the ``partitioning()``
            function. A flavor string can be used as shortcut, and with a list of
            field names a DirectionaryPartitioning will be inferred.
        :param filters: Expression, default None
            Scan will return only the rows matching the filter.
            If possible the predicate will be pushed down to exploit the
            partition information or internal metadata found in the data
            source, e.g. Parquet statistics. Otherwise filters the loaded
            RecordBatches before yielding them.
        :return:
            table : arrow.Table
        """

        if filters is not None:
            return self.dataset(
                file_format=file_format,
                path=path,
                partitioning=partitioning
            ).to_table(filter=filters)
        else:
            return self.dataset(
                file_format=file_format,
                path=path,
                partitioning=partitioning
            ).to_table()

    def read_to_pandas(self,
                       file_format: str,
                       path: str,
                       partitioning: str = "hive",
                       filters=None) -> DataFrame:
        """
        Read the dataset as pandas dataframe.

        Parameters
        ----------
        :param file_format: str
            Currently "parquet", "deltalake" supported.
        :param path: str
            Path pointing to a single file:
                Open a Dataset from a single file.
            Path pointing to a directory:
                The directory gets discovered recursively according to a
                partitioning scheme if given.
        :param partitioning: Partitioning, PartitioningFactory, str, list of str default "hive"
            The partitioning scheme specified with the ``partitioning()``
            function. A flavor string can be used as shortcut, and with a list of
            field names a DirectionaryPartitioning will be inferred.
        :param filters: Expression, default None
            Scan will return only the rows matching the filter.
            If possible the predicate will be pushed down to exploit the
            partition information or internal metadata found in the data
            source, e.g. Parquet statistics. Otherwise filters the loaded
            RecordBatches before yielding them.
        :return:
            dataframe : pandas.Dataframe
        """
        return self.read_to_arrow_table(
            file_format=file_format,
            path=path,
            partitioning=partitioning,
            filters=filters
        ).to_pandas()

    def write(self, data, file_format, path, write_options: WriteOptions):
        """
        :param data: pandas.DataFrame, pyarrow.Dataset, Table/RecordBatch, RecordBatchReader, list of \
                    Table/RecordBatch, or iterable of RecordBatch

        :param file_format: str
            Currently "parquet", "deltalake" supported.
        :param path: str
            Path pointing to a single file:
                Open a Dataset from a single file.
            Path pointing to a directory:
                The directory gets discovered recursively according to a
                partitioning scheme if given.
        :param write_options:
        """

        def convert_pandas_to_arrow_table(dataframe):
            if isinstance(dataframe, pd.DataFrame):
                return pa.Table.from_pandas(dataframe)
            else:
                raise TypeError(f"The parameter 'table' type must be one of [pandas.DataFrame, pyarrow.Table]")

        self._validate_format(file_format=file_format)

        try:
            pyarr_data = convert_pandas_to_arrow_table(data)
        except TypeError:
            pyarr_data = data

        filesystem = self._get_filesystem()

        if file_format == "parquet":
            self._logger.debug(f""" Write to Dataset: 
                                   root_path: '{self._get_filesystem_base_path(path=path)}'
                                   filesystem: {type(filesystem)}
                                   file_format:{write_options.compression_codec}
                                   existing_data_behavior:{write_options.existing_data_behavior}
                                   partition_cols: {write_options.partitions}
                               """)

            pa.dataset.write_dataset(
                data=pyarr_data,
                format=file_format,
                filesystem=filesystem,
                base_dir=self._get_filesystem_base_path(path=path),
                partitioning=write_options.partitions,
                existing_data_behavior=write_options.existing_data_behavior(),
                file_options=ds.ParquetFileFormat().make_write_options(
                    compression=write_options.compression_codec
                )
            )
        elif file_format == "deltalake":
            write_deltalake(
                table_or_uri=self._get_deltalake_url(path=path),
                data=pyarr_data,
                partition_by=write_options.partitions,
                file_options=ds.ParquetFileFormat().make_write_options(
                    compression=write_options.compression_codec
                ),
                storage_options=self._get_deltalake_storage_options(),
                mode=write_options.existing_data_behavior()
            )



    def write_batch(self, batch, file_format, path, write_options: WriteOptions):
        """
        :param table

        :param file_format: str
            Currently "parquet", "deltalake" supported.
        :param path: str
            Path pointing to a single file:
                Open a Dataset from a single file.
            Path pointing to a directory:
                The directory gets discovered recursively according to a
                partitioning scheme if given.
        :param write_options:
        """
