from setuptools import setup

setup(
    name='cloud-arrow',
    version='0.8.1',
    authors='Alfredo Lorie, Ignacio Rodriguez',
    author_email='a24lorie@gmail.com, nachorodriguez79@gmail.com',
    description="""Python library to provide an Unified cloud storage API for reading and writing parquet and  deltalake files from/to the main cloud provider's object storage using the arrow format""",
    long_description=open('README.md', encoding='UTF-8').read(),
    long_description_content_type='text/markdown',
    license='GNU',
    url='https://github.com/a24lorie/Cloud-Arrow',
    install_requires=['pyarrow', 'pandas', 'numpy', 'saspy', 'adlfs',
                      'gcsfs', 's3fs', 'azure-identity', 'fsspec', 'deltalake'],
    packages=['cloud_arrow',
              'cloud_arrow.core',
              'cloud_arrow.adls',
              'cloud_arrow.gcsfs',
              'cloud_arrow.local'
    ],
    package_dir={
        'cloud_arrow': 'cloud',
        'cloud_arrow.core': 'cloud/core',
        'cloud_arrow.adls': 'cloud/adls',
        'cloud_arrow.gcsfs': 'cloud/gcsfs',
        'cloud_arrow.local': 'cloud/local'
    }
)