# Neynar Parquet Importer

The MVP design is just going to print imported data to the terminal.

The next design should write the data into a SQL database. Make it easy to use Postgres/MySQL/MyRocks/etc. Anything supported by sqlalchemy

I think an example that uses Spark could be useful.

## Backfill

You'll need to load a "full" export at least once. If the schema ever changes, it will likely be necessary to load a "full" backup again.

## Stream

Once the backfill of the "full" export is complete, you can start importing the "incremental" exports.

Sometimes the network is quiet and the parquet file would be empty. When this happens, we upload a `.empty` file. We had some troubles with schema detection with actually empty `.parquet` files and this was a simple solution.
