# Example Neynar Parquet Importer

Download parquet exports from [Neynar](https://neynar.com) and import them into a Postgres database.

Credentials for downloading the parquet files are currently handed out manually. Reach out to us if you'd like to try it out!

The script will load a "full" export once at the start. Once the backfill of the "full" export is complete, it will start importing the "incremental" exports.

Sometimes the network is quiet and the parquet file is empty. When this happens, Neynar exports a `.empty` file. We had some troubles with schema detection with actually empty `.parquet` files and this was a simple solution.

The database is set up by a simple `.sql` file that runs when the app starts. There is so many ways to do migrations and I didn't want to force a library on you.

## Setup

Copy the example configuration:

    cp env.example .env

Edit the config to include the TABLES that you want, your DATABASE_URI, and your AWS credentials:

    open -t .env

Run a postgres and the app inside of docker:

    docker compose up --build -d

NOTE: Older systems might use `docker-compose` instead of `docker compose`

## Developing on your localhost

Stop the docker version of the app:

    docker compose stop app

Install `uv`:

    brew install uv

Edit your configuration. The database host/port will probably need to go to "localhost:15432" instead of "postgres:5432":

    open -t .env

Run the app:

    INTERACTIVE_DEBUG=true uv run python -m neynar_parquet_importer.main

NOTE: INTERACTIVE_DEBUG makes python open a shell if an exception happens. This can be useful for debugging but shouldn't be used in production.

Lint your code:

    ruff neynar_parquet_importer

Upgrade dependencies ([OfficialDocs](https://docs.astral.sh/uv/concepts/projects/sync/#upgrading-locked-package-versions)):

    uv lock --upgrade


## Notes and Todo

The "messages" table is very large and not available as a parquet file. Reach out if you need it.

This example repo was designed to be simple so that you can easily plug it into any existing code that you have. There are lots of improvements on the horizon.

- Skip already imported incrementals instead of checking every file
- If the schema ever changes, it will likely be necessary to load a "full" backup again. There will be an env var to force this
- Track SNS queue instead of polling
- Graceful shutdown (sometimes you will have to hit ctrl+c a bunch of times to exit)
- Crontab entry to delete old files
- "Downloaded:" log message should include file age
- allow custom postgres schema instead of always putting into `public`
- Store the ETAG in the database so we can compare file hashes
- recommended specs/storage for an EC2 server (disk size for parquet files)
- recommended specs/storage space for postgres cluster (disk size for postgres data)
- run on an EC2 and collect example timings
- Support more than just postgres. We need a generic way to do UPSERTS
