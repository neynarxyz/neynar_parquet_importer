from setuptools import setup, find_packages

setup(
    name="example_neynar_parquet_importer",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "boto3",
        "ipdb",
        "pyarrow",
        "python-dotenv",
        "psycopg2",
        "rich",
        "sqlalchemy",
    ],
)
