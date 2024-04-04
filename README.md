<p style="text-align: center;">
   April 2024 <br/>
   Alfredo Lorie Bernardo, Ignacio Rodriguez Sanchez <br/>
   version 0.8.1
</p>

# Cloud Arrow
Python Library to read and write Parquet and Deltalake files from the main Cloud Providers 
Object-Store and Local Filesystem

## Introduction

`Cloud Arrow` is a python library to provide read and write capabilities for **Parquet** and **Deltalake** files 
(without relying on spark) from/to the main cloud providers object storage service (Azure ADLSGen2, Google GCSFS, AWS S3 -> coming soon) 
and the Local FileSystem. 
The main goal is to provide a single and unified API for reading and writing files from python programs. 
This library is available in PyPI and distributed under the GNU license.4 

Up to date, Cloud Arrow relies on the Apache Arrow under the hood and some fsspec-compatible filesystems implementations to connect 
and interact with the cloud providers object storage. 

## Download

GitHub: <https://github.com/a24lorie/Cloud-Arrow>

## Highlights

1. Read, write, and manage Parquet files and Delta Lake tables with Python without Spark
2. Unified API for all cloud providers object storage and local filesystem. 
3. Fast file access with transparent multi-threading.
4. Support for reading and writing partitioned datasets
5. Filtering and partition pruning to avoid unnecessary data movement

## Using Cloud-Arrow

The Cloud Arrow library provides an **Unified API** to read and write parquet files and delta-lake tables from the
main cloud providers object storage and the local filesystem. 
The current filesystem implementations are:

```
AbstractStorage 
  └─── ADLSStorage
  └─── GCSFSStorage
  └─── LocalFileSystemStorage
  └─── DBFSStorage (Future *)
  └─── S3Storage (Future *)
```

When using the Cloud Arrow library the first step is to create an instance of one of the implementations above, let's 
see some examples

### Azure ADLSGen2 

To read and write from Azure ADLS Gen2 create an instance of ADLSStorage object providing the following arguments: 

1. **tentant_id***: (*Required*) - The Azure Tenant Id used in oauth flows 
2. **client_id***: (*Required*) - Service principal client id for authorizing requests
3. **client_secret***: (*Required*) - Service principal client secret for authorizing requests
4. **account_name**: (*Required*) - The name of the azure storage account 
5. **container**: (*Required*) - Container name

**Currently, only the client-secret authentication mechanism supported, we plan to extend the authentication mechanisms 
to support other types in the future* 

``` python
from cloud_arrow.adls import ADLSStorage

object_storage = ADLSStorage(
    tenant_id="AzureTentantId",
    client_id="AzureClientId",
    client_secret="AzureClientSecret",
    account_name="storage-account",
    container="container"
)
```

### Google GCSFS  

To read and write from Google GCSFS create an instance of GCSFSStorage object, by providing the following arguments: 

1. **project**: (*Required*) - Project id of the Google Cloud project  
2. **access**: (*Required*) -  Access method to the file system (read_only, read_write or full_control)
3. **token**: (*Required*) - Authentication method for Google Cloud Storage (None, google_default, cache, anon, browser, cloud or credentials filename path). See [GCSFileSystem](https://gcsfs.readthedocs.io/en/latest/api.html#gcsfs.core.GCSFileSystem) for more details.
4. **bucket**: (*Required*) - Name of the bucket or container that hold the data.
5. **default_location**: (*Optional*) - Default location where buckets are created.

``` python
from cloud_arrow.gcsfs import GCSFSStorage

object_storage = GCSFSStorage(
    project="GCSFSProjectId",
    access="read_only",
    bucket="GCSFSBucket",
    token="/path/google-secret.json",
    default_location=""
)
```
#### `google-secret.json` 

In the previous example the ***credentials filename path*** was used to authenticate. You can create the `google-secret.json` file [here](https://console.cloud.google.com/iam-admin/serviceaccounts). 
You don't need to manually fill in JSON by hand, the below example is provided to show you what the end result should look like. 
You should be able to read, write, and delete objects from at least one bucket.

```json
{
  "type": "service_account",
  "project_id": "$YOUR_GOOGLE_PROJECT_ID",
  "private_key_id": "...",
  "private_key": "...",
  "client_email": "...",
  "client_id": "...",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://accounts.google.com/o/oauth2/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": ""
}
```

### Local Filesystem

To read and write from the Local Filesystem create an instance of LocalFileSystemStorage object: 

``` python
from cloud_arrow.local import LocalFileSystemStorage

object_storage = LocalFileSystemStorage()
```

## Reading  Data
To read files from ADLSGen2, GCSFS or the Local Filesystem use the **object_storage** instance configured. 
The Cloud Arrow library provides an unified and consistence experience across all the filesystem implementations 
to read parquet files or delta-lake tables, the following methods are available for reading:

* read_batches(file_format: str, path: str, partitioning: str, filters=None, batch_size: int) -> pa.RecordBatch
* read_to_arrow_table(file_format: str, path: str, partitioning: str, filters=None) -> pa.Table
* read_to_pandas(file_format: str, path: str, partitioning: str, filters=None) -> DataFrame
* dataset(file_format: str, path: str, partitioning: str) -> ds.Dataset

Let's take a look at some examples 

### Read from parquet file or delta table to an Arrow Record Batch

read_batches(file_format: str, path: str, partitioning: str, filters=None, batch_size: int) -> pa.RecordBatch
1. **file_format**: (*Required*) - Can be on of "parquet" or "deltalake"
2. **path**:  (*Required*)  - The object storage location to the dataset (can be a single file name or directory name)
3. **filters**: (*Optional*) - pyarrow.compute.Expression or List[Tuple] or List[List[Tuple]]. See [pyarrow filter](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html) for more details
4. **batch_size**: (*Optional*) - The number of batches to read ahead in a file

``` python
           
batches_parquet = object_storage.read_batches(file_format="parquet", path="path_to_parquet", batch_size=2000)
batches_delta = object_storage.read_batches(file_format="deltalake", path="path_to_deltalake", batch_size=2000) 

for batch_parquet in batches_parquet:
   count_parquet += batch_parquet.num_rows

for batch_delta in batches_delta:
   count_delta += batch_delta.num_rows
   
print(count_parquet)
print(count_delta)
```

### Read parquet file or delta table to an Arrow Table
read_to_arrow_table(file_format: str, path: str, partitioning: str, filters=None) -> pa.Table
1. **file_format**: (*Required*) - Can be one of "parquet" or "deltalake"
2. **path**:  (*Required*)  - The object storage location to the dataset (can be a single file name or directory name)
3. **filters**: (*Optional*) - pyarrow.compute.Expression or List[Tuple] or List[List[Tuple]]. See [pyarrow filter](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html) for more details

``` python
result_parquet = object_storage.read_to_arrow_table(file_format="parquet", path="path_to_parquet")
result_delta = object_storage.read_to_arrow_table(file_format="deltalake", path="path_to_deltalake") 

print(result_parquet.num_rows)
print(result_delta.num_rows)
```

### Read parquet file or delta table to a Pandas Dataframe
read_to_pandas(file_format: str, path: str, partitioning: str, filters=None) -> DataFrame
1. **file_format**: (*Required*) - Can be one of "parquet" or "deltalake"
2. **path**:  (*Required*)  - The object storage location to the dataset (can be a single file name or directory name)
3. **filters**: (*Optional*) - pyarrow.compute.Expression or List[Tuple] or List[List[Tuple]]. See [pyarrow filter](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html) for more details

``` python
result_parquet = object_storage.read_to_pandas(file_format="parquet", path="path_to_parquet")
result_delta = object_storage.read_to_pandas(file_format="deltalake", path="path_to_deltalake") 

print(result_parquet.info())
print(result_delta.info())
```

### Filtering 
By specifying the argument **filters** to any of the methods described previously the source data can be filtered during 
the reading. Filters can be applied to any given column on the source data. I f the source data is partitioned and a 
filter expression contains any partition column, the files within the partitions that does not match filtering condition
will be skipped, resulting in better performance, this is known as partition pruning.  

**filters**: (*Optional*) - pyarrow.compute.Expression or List[Tuple] or List[List[Tuple]]. 
See [pyarrow filter](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html) for more details

``` python
import pyarrow.dataset as ds

batches_parquet_batch = object_storage.read_batches(
                                             file_format="parquet", 
                                             path="path_to_parquet", 
                                             filters=(ds.field("FieldName") == 0),
                                             batch_size=2000)
batches_delta_batch = object_storage.read_batches(
                                             file_format="deltalake", 
                                             path="path_to_deltalake", 
                                             filters=(ds.field("FieldName") == 0),
                                             batch_size=2000) 

for batch_parquet in batches_parquet_batch:
   count_parquet += batch_parquet.num_rows

for batch_delta in batches_delta_batch:
   count_delta += batch_delta.num_rows
   
print(count_parquet)
print(count_delta)

# Read parquet with filtering expression to an Arrow Table
result_parquet_arr_table = object_storage.read_to_arrow_table(
                                       file_format="parquet", 
                                       path="path_to_parquet"
                                       filters=(ds.field("FieldName") == 0))
                                       
# Read delta lake with filtering expression to an Arrow Table                                       
result_delta_arr_table = object_storage.read_to_arrow_table(
                                       file_format="parquet", 
                                       path="path_to_parquet"
                                       filters=(ds.field("FieldName") == 0)) 

print(result_parquet_arr_table.num_rows)
print(result_delta_arr_table.num_rows)

# Read parquet with filtering expression to a Pandas Dataframe
result_parquet_df = object_storage.read_to_pandas(
                                       file_format="parquet", 
                                       path="path_to_parquet"
                                       filters=(ds.field("FieldName") == 0))
                                       
# Read delta lake with filtering expression to a Pandas Dataframe                                 
result_delta_df = object_storage.read_to_pandas(
                                       file_format="parquet", 
                                       path="path_to_parquet"
                                       filters=(ds.field("FieldName") == 0)) 
                                       
print(result_parquet_df.info())
print(result_delta_df.info())
```

## Writing Data
To write files to ADLSGen2, GCSFS or the Local Filesystem use the **object_storage** instance configured. 
The Cloud Arrow library provides an unified and consistence experience across all the filesystem implementations 
to write parquet files or delta-lake tables, the following method is available for writing:

* write(data, file_format, path, basename_template, write_options: WriteOptions)
1. **data**: (*Required*) - Can be one of pandas.DataFrame, pyarrow.Dataset, Table/RecordBatch, RecordBatchReader, list of \
                   Table/RecordBatch, or iterable of RecordBatch 
2. **file_format**: (*Required*) - Can be one of "parquet" or "deltalake"
3. **path**:  (*Required*)  - The object storage location to the dataset (can be a single file name or directory name)
4. **basename_template** (*Optional*) - A template string used to generate basenames of written data files.
5. **write_options** - (*Required*) - Options to write the files including: 
    ```
    WriteOptions 
    |     * partitions: List of the names of columns to split the dataset. 
    |     * compression_codec: Allow to specify the compression codec (None, snappy, sz4, brotli, gzip, zstd)
    └─── ParquetWriteOptions
            *  existing_data_behavior: Can be one of ['error', 'overwrite_or_ignore', 'delete_matching']              
    └─── DeltaLakeWriteOptions
            *  existing_data_behavior: Can be one of [['error', 'append', 'overwrite', 'ignore']]
    ```

Let's take a look at some examples 

### Write parquet file or delta table without partitions nor compression and overwrite source 
 
``` python
# Example writing parquet
object_storage.write(
                data=table,
                file_format="parquet",
                path=path,
                write_options=ParquetWriteOptions(
                    compression_codec="None",
                    existing_data_behavior="overwrite_or_ignore") # 'error', 'overwrite_or_ignore', 'delete_matching'
                )
                    
# Example writing deltalake
object_storage.write(
                data=table,
                file_format="deltalake",
                path=path,
                write_options=DeltaLakeWriteOptions(
                    compression_codec="None",
                    existing_data_behavior="overwrite") # 'error', 'append', 'overwrite', 'ignore'
                )
          
```

### Write parquet file or delta table partitioned with "snappy" compression and overwrite source 
 
``` python
# Example writing parquet
object_storage.write(
                data=table,
                file_format="parquet",
                path=path,
                write_options=ParquetWriteOptions(
                    partitions=["col1", "col2"],
                    compression_codec="snappy",
                    existing_data_behavior="overwrite_or_ignore") # 'error', 'overwrite_or_ignore', 'delete_matching'
                )
                    
# Example writing deltalake
object_storage.write(
                data=table,
                file_format="deltalake",
                path=path,
                write_options=DeltaLakeWriteOptions(
                    partitions=["col1", "col2"],
                    compression_codec="snappy",
                    existing_data_behavior="overwrite") # 'error', 'append', 'overwrite', 'ignore'
                )
          
```

### Write parquet file or delta table without partitions nor compression and overwrite source in batches
 
``` python
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

batch_reader = pa.RecordBatchReader.from_batches(schema, iter_record_batches())
        
# Example writing parquet in batches  
object_storage.write(
             data=batch_reader,
             file_format="parquet",
             path=path,
             write_options=ParquetWriteOptions(
                 partitions=[],
                 compression_codec="None",
                 existing_data_behavior="overwrite_or_ignore") # 'error', 'overwrite_or_ignore', 'delete_matching'
             )
                    
# Example writing deltalake in batches
object_storage.write(
             data=batch_reader,
             file_format="deltalake",
             path=path,
             write_options=ParquetWriteOptions(
                 partitions=[],
                 compression_codec="None",
                 existing_data_behavior="overwrite_or_ignore") # 'error', 'overwrite_or_ignore', 'delete_matching'
             )  
```

### Write parquet file or delta table partitioned with "snappy" compression and overwrite source in batches
 
``` python
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

batch_reader = pa.RecordBatchReader.from_batches(schema, iter_record_batches())
        
# Example writing parquet in batches  
object_storage.write(
             data=batch_reader,
             file_format="parquet",
             path=path,
             write_options=ParquetWriteOptions(
                 partitions=[],
                 compression_codec="snappy",
                 existing_data_behavior="overwrite_or_ignore") # 'error', 'overwrite_or_ignore', 'delete_matching'
             )
                    
# Example writing deltalake in batches
object_storage.write(
             data=batch_reader,
             file_format="deltalake",
             path=path,
             write_options=ParquetWriteOptions(
                 partitions=[],
                 compression_codec="snappy",
                 existing_data_behavior="overwrite_or_ignore") # 'error', 'overwrite_or_ignore', 'delete_matching'
             )       
```

# Contributing
The Cloud Arrow library welcomes contributors from all developers, regardless of your experience or programming background.
If you find a bug, send a [pull request](https://github.com/a24lorie/Cloud-Arrow/pulls) and we'll discuss things. If you are not familiar with "***pull request***" term I recommend reading the following [article](https://yangsu.github.io/pull-request-tutorial/) for better understanding
We value kind communication and building a productive, friendly environment for maximum collaboration and fun.
