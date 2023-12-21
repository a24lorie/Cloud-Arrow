***
                                     Cloud Arrow
    Python Library to read and write Parquet and Deltalake files from ADLS and GCFS 

                                    Dic 2023
			        Alfredo Lorie Bernardo, Ignacio Rodriguez Sanchez					

                                 version 0.4.0

***

# Introduction

`Cloud Arrow` is a python library to provide read and write capabilities for **Parquet** and **Deltalake** files 
(without relying on spark) from/to the main cloud providers object storage service (Azure ADLSGen2, Google GCSFS, AWS S3 -> coming soon). 
The main goal is to provide a single and unified API for reading and writing files from python programs. 
This library is available in PyPI and distributed under the GNU license.4 

Up to date, Cloud Arrow heavily relies on the Apache Arrow under the hood and some fsspec-compatible filesystems implementations to connect 
and interact with the cloud providers object storage. 

# Download

GitHub: <https://github.com/a24lorie/Cloud-Arrow>

# Highlights

1. Fast file access with transparent multi-threading.
2. Support for reading and writing partitioned datasets
3. Filtering and partition pruning to avoid unnecessary data movement
4. Read, write, and manage Delta Lake tables with Python or Rust without Spark or Java

# Using Cloud-Arrow

## Reading from ADLSGen2

To read a parquet o deltalake files from Azure ADLSGen2 we first must need to create an instance of ADLSObjectStorage 
using the client-secret authentication mechanism by providing the following arguments:

 1. **tentant_id** Required - The Azure Tenant Id used in oauth flows 
 2. **client_id**: Required - Service principal client id for authorizing requests
 3. **client_secret**: Required - Service principal client secret for authorizing requests
 4. **account_name**: Required - The name of the azure storage account 
 5. **container**: Required - Container name

``` python
from cloud_arrow.adls import ADLSObjectStorage

adls_object_storage = ADLSObjectStorage(
    tenant_id="AzureTentantId",
    client_id="AzureClientId",
    client_secret="AzureClientSecret",
    account_name="storage-account",
    container="container"
)
```

Use the adls_object_storage object created and call the read_to_pandas method to retrieve files in a pandas DataFrame by specifying:

1. **file_format**: Required - can be on of "parquet" or "deltalake"
2. **path**: Required - The object storage location to the dataset (can be a single file name or directory name)
3. **filters**: Optional - pyarrow.compute.Expression or List[Tuple] or List[List[Tuple]]. See [pyarrow filter](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html) for more details

``` python
resultParquet = adls_object_storage.read_to_pandas(file_format="parquet", path="path_to_parquet")
resultDelta = adls_object_storage.read_to_pandas(file_format="deltalake", path="path_to_deltalake") 

print(resultParquet.info())
print(resultDelta.info())
```
## Writing to ADLS Gen2

To write a parquet o deltalake files from Azure ADLSGen2 we first must need to create an instance of ADLSObjectStorage 
using the client-secret authentication mechanism by providing the following arguments:

 1. **tentant_id** Required - The Azure Tenant Id used in oauth flows 
 2. **client_id**: Required - Service principal client id for authorizing requests
 3. **client_secret**: Required - Service principal client secret for authorizing requests
 4. **account_name**: Required - The name of the azure storage account 
 5. **container**: Required - Container name

``` python
from cloud_arrow.adls import ADLSObjectStorage
from cloud_arrow.core import DeltaLakeWriteOptions
from cloud_arrow.core import ParquetWriteOptions

adls_object_storage = ADLSObjectStorage(
    tenant_id="AzureTentantId",
    client_id="AzureClientId",
    client_secret="AzureClientSecret",
    account_name="storage-account",
    container="container"
)
```
Use the adls_object_storage object created and call the write method to save the dataset in parquet or deltalake format to ADLSGen2 by specifying::

1. **table**: Required - pandas DataFrame or Arrow Table  
2. **file_format**: Required -  Required - can be on of "parquet" or "deltalake"
3. **path**: Required - The object storage location to store the dataset
4. **write_options**: Required - Options to write the files including: 
   1. ***partitions*** : Allows to specify which columns to use to split the dataset.
   2. ***compression_codec***: Allow to specify the compression codec (None, snappy, sz4, brotli, gzip, zstd)
   3. ***existing_data_behavior***:  Allows to specify how to handle data that already exists in the destination.

``` python
# Example writing parquet
adls_object_storage.write(
                    table=table,
                    file_format="parquet",
                    path=path,
                    write_options=ParquetWriteOptions(
                        partitions=["col1", "col2", ...],
                        compression_codec="snappy",
                        existing_data_behavior="overwrite_or_ignore") # 'error', 'overwrite_or_ignore', 'delete_matching'
                    )
                    
# Example writing deltalake
adls_object_storage.write(
                    table=table,
                    file_format="deltalake",
                    path=path,
                    write_options=DeltaLakeWriteOptions(
                        partitions=["col1", "col2", ...],
                        compression_codec="snappy",
                        existing_data_behavior="overwrite") # 'error', 'append', 'overwrite', 'ignore'
                    )
          
```
## Reading from GCSFS

To write a parquet o deltalake files from Google GCSFS we first must need to create an instance of GCSFSObjectStorage 
by providing the following arguments:

 1. **project** Required - Project id of the Google Cloud project 
 2. **access**: Required - Access method to the file system (read_only, read_write or full_control)
 3. **token**: Required - Authentication method for Google Cloud Storage (None, google_default, cache, anon, browser, cloud or credentials filename path). See [GCSFileSystem](https://gcsfs.readthedocs.io/en/latest/api.html#gcsfs.core.GCSFileSystem) for more details.
 4. **bucket**: Required - Name of the bucket or container that hold the data.
 5. **default_location**: Optional - Default location where buckets are created.

``` python
from cloud_arrow.gcsfs import GCSFSObjectStorage

gcsfs_object_storage = GCSFSObjectStorage(
    project="GCSFSProjectId",
    access="read_only",
    bucket="GCSFSBucket",
    token="/path/google-secret.json",
    default_location=""
)
```
### `google-secret.json`
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
        
Use the gcsfs_object_storage object created and call the read_to_pandas method to retrieve files in a pandas DataFrame by specifying:

1. **file_format**: Required - can be on of "parquet" or "deltalake"
2. **path**: Required - The object storage location to the dataset (can be a single file name or directory name)
3. **filters**: Optional - pyarrow.compute.Expression or List[Tuple] or List[List[Tuple]]. See [pyarrow filter](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html) for more details

``` python
resultParquet = gcsfs_object_storage.read_to_pandas(file_format="parquet", path="path_to_parquet")
resultDelta = gcsfs_object_storage.read_to_pandas(file_format="deltalake", path="path_to_deltalake") 

print(resultParquet.info())
print(resultDelta.info())
```

## Writing to GCSFS

When creating a cloud-arrow writer to Google Cloud Storage create an instance of GCSFSObjectStorage with the arguments:

 1. **project** Required - Project id of the Google Cloud project 
 2. **access**: Required - Access method to the file system (read_only, read_write or full_control)
 3. **token**: Required - Authentication method for Google Cloud Storage (None, google_default, cache, anon, browser, cloud or credentials filename path). 
      See [GCSFileSystem](https://gcsfs.readthedocs.io/en/latest/api.html#gcsfs.core.GCSFileSystem) for more details.
 4. **bucket**: Required - Name of the bucket or container that hold the data.
 5. **default_location**: Optional - Default location where buckets are created.

``` python
from cloud_arrow.gcsfs import GCSFSObjectStorage
from cloud_arrow.core import DeltaLakeWriteOptions
from cloud_arrow.core import ParquetWriteOptions

gcsfs_object_storage = GCSFSObjectStorage(
    project="GCSFSProjectId",
    access="read_only",
    bucket="GCSFSBucket",
    token="/path/google-secret.json",
    default_location=""
)
```

Use the gcsfs_object_storage object created and call the write method to save the dataset in parquet or deltalake format to GCSFS by specifying::

1. **table**: Required - pandas DataFrame or Arrow Table  
2. **file_format**: Required -  Required - can be on of "parquet" or "deltalake"
3. **path**: Required - The object storage location to store the dataset
4. **write_options**: Required - Options to write the files including: 
   1. ***partitions*** : Allows to specify which columns to use to split the dataset.
   2. ***compression_codec***: Allow to specify the compression codec (None, snappy, sz4, brotli, gzip, zstd)
   3. ***existing_data_behavior***:  Allows to specify how to handle data that already exists in the destination.

``` python
# Example writing parquet
gcsfs_object_storage.write(
                    table=table,
                    file_format="parquet",
                    path=path,
                    write_options=ParquetWriteOptions(
                        partitions=["col1", "col2", ...],
                        compression_codec="snappy",
                        existing_data_behavior="overwrite_or_ignore") # 'error', 'overwrite_or_ignore', 'delete_matching'
                    )
                    
# Example writing deltalake
gcsfs_object_storage.write(
                    table=table,
                    file_format="deltalake",
                    path=path,
                    write_options=DeltaLakeWriteOptions(
                        partitions=["col1", "col2", ...],
                        compression_codec="snappy",
                        existing_data_behavior="overwrite") # 'error', 'append', 'overwrite', 'ignore'
                    )
```

# Contributing
The Cloud Arrow library welcomes contributors from all developers, regardless of your experience or programming background.
If you find a bug, send a [pull request](https://github.com/a24lorie/PyACL/pulls) and we'll discuss things. If you are not familiar with "***pull request***" term I recommend reading the following [article](https://yangsu.github.io/pull-request-tutorial/) for better understanding
We value kind communication and building a productive, friendly environment for maximum collaboration and fun.
