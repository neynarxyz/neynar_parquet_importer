# Example Neynar Parquet Importer

Download parquet exports from Neynar and import them into a Postgres database.

The script will load a "full" export once at the start.

Once the backfill of the "full" export is complete, it will start importing the "incremental" exports.

Sometimes the network is quiet and the parquet file is empty. When this happens, Neynar exports a `.empty` file. We had some troubles with schema detection with actually empty `.parquet` files and this was a simple solution.

The database is set up by a simple `.sql` file that runs when the app starts. Theres so many ways to do migrations and I didn't want to force a library on you.

## Setup

Set up your configuration. Copy this file and then add your secrets to it:

    cp env.example .env

Run a postgres and the app inside of docker:

    docker compose up --build -d

NOTE: Older systems might use `docker-compose` instead of `docker compose`

## Developing

Set up the python environment

    python3.12 -m .venv venv
    . .venv/bin/activate
    pip install -U pip
    pip install --use-pep517 -r requirements.txt -e . 

Set up your configuration. Copy this file and then add your secrets to it. The database host/port will probably need to go to "localhost:15432" instead of "postgres:5432":

    cp env.example .env

Run the app:

    INTERACTIVE_DEBUG=true python -m example_neynar_parquet_importer.main

NOTE: INTERACTIVE_DEBUG makes python open a shell if an exception happens. This can be useful for debugging but shouldn't be used in production.


## Todo

- Download the latest full instead of hard coding the timestamp
- Track files that have already been imported
- If the schema ever changes, it will likely be necessary to load a "full" backup again. There will be an env var to force this
- Track SNS queue instead of polling
- Graceful shutdown (sometimes i have to hit ctrl+c a bunch of times)
- Crontab entry to delete old files
- "Downloaded:" log message should include file age
- allow custom postgres schema instead of always putting into public
- Store the ETAG in the database so we can compare file hashes

## Open Questions

- profile_with_addresses as a table or a view?
