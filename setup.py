from setuptools import setup, find_packages

setup(
    name="neynar_parquet_importer",
    version="0.1.1",
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
