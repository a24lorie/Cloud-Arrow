import logging
from abc import ABCMeta, abstractmethod
from typing import Any, List, Tuple

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
           'AbstractObjectStorage']

from pyarrow._dataset_parquet import ParquetFileWriteOptions, ParquetFileFormat

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


class AbstractObjectStorage(metaclass=ABCMeta):
    def __init__(self):
        self._logger = logging.getLogger('cloud_arrow')

    def _validate_format(self, file_format):
        if file_format not in ["parquet", "deltalake"]:
            error_msg = f"The format must be one of: 'parquet', 'deltalake'"
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
    def _get_cloud_path(self, path) -> str:
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
    def _get_delta_lake_url(self, path) -> str:
        pass

    def read_to_arrow(self, file_format, path, filters) -> pa.Table:
        """

        :param file_format:
        :param path:
        :param filters
        :return:
        """
        self._validate_format(file_format=file_format)

        filesystem = self._get_filesystem()

        if file_format == "parquet":
            if filters is not None:
                return pq.read_table(
                    source=self._get_cloud_path(path=path),
                    filesystem=filesystem,
                    partitioning='hive',
                    filters=filters
                )
            else:
                return pq.read_table(
                    source=self._get_cloud_path(path=path),
                    filesystem=filesystem
                )
        elif file_format == "deltalake":
            # Read the Delta table from the storage account
            if filters is not None:
                return DeltaTable(
                    table_uri=self._get_delta_lake_url(path=path),
                    storage_options=self._get_deltalake_storage_options()
                ).to_pyarrow_dataset().to_table(filter=filters)
            else:
                return DeltaTable(
                    table_uri=self._get_delta_lake_url(path=path),
                    storage_options=self._get_deltalake_storage_options()
                ).to_pyarrow_dataset().to_table()

    def read_to_pandas(self, file_format, path, filters) -> DataFrame:
        """

        :param file_format:
        :param path:
        :param filters
        :return:
        """
        return self.read_to_arrow(file_format, path, filters).to_pandas()

    def write(self, table, file_format, path, write_options: WriteOptions):
        """

        :param table:
        :param file_format:
        :param path:
        :param write_options:
        :return:
        """

        def convert_to_arrow_table(table) -> pa.Table:
            if isinstance(table, pd.DataFrame):
                return pa.Table.from_pandas(table)
            elif isinstance(table, pa.Table):
                return table
            else:
                raise f"The parameter 'table' type must be one of [pandas.DataFrame, pyarrow.Table]"

        self._validate_format(file_format=file_format)
        pyarr_table = convert_to_arrow_table(table)
        filesystem = self._get_filesystem()

        if file_format == "parquet":
            self._logger.debug(f""" Write to Dataset: 
                                                   root_path: '{self._get_cloud_path(path=path)}'
                                                   filesystem: {type(filesystem)}
                                                   file_format:{write_options.compression_codec}
                                                   existing_data_behavior:{write_options.existing_data_behavior}
                                                   partition_cols: {write_options.partitions}
                                               """)

            if write_options.compression_codec == "snappy":
                pq.write_to_dataset(
                    pyarr_table,
                    root_path=self._get_cloud_path(path=path),
                    filesystem=filesystem,
                    existing_data_behavior=write_options.existing_data_behavior(),
                    partition_cols=write_options.partitions
                )
            else:
                pq.write_to_dataset(
                    pyarr_table,
                    root_path=self._get_cloud_path(path=path),
                    filesystem=filesystem,
                    compression=write_options.compression_codec,
                    existing_data_behavior=write_options.existing_data_behavior(),
                    partition_cols=write_options.partitions
                )
        elif file_format == "deltalake":
            if write_options.compression_codec == "snappy":
                write_deltalake(table_or_uri=self._get_delta_lake_url(path=path),
                                data=table,
                                partition_by=write_options.partitions,
                                storage_options=self._get_deltalake_storage_options(),
                                mode=write_options.existing_data_behavior()
                                )
            else:
                write_deltalake(table_or_uri=self._get_delta_lake_url(path=path),
                                data=table,
                                partition_by=write_options.partitions,
                                file_options=ds.ParquetFileFormat().make_write_options(
                                    compression=write_options.compression_codec
                                ),
                                storage_options=self._get_deltalake_storage_options(),
                                mode=write_options.existing_data_behavior()
                )
