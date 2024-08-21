# Example Neynar Parquet Importer

Download parquet exports from Neynar and import them into a Postgres database.

The script will load a "full" export once at the start.

Once the backfill of the "full" export is complete, it will start importing the "incremental" exports.

Sometimes the network is quiet and the parquet file is empty. When this happens, Neynar exports a `.empty` file. We had some troubles with schema detection with actually empty `.parquet` files and this was a simple solution.

## Setup

Set up your configuration. Copy this file and then add your secrets to it:

    cp env.example .env

Run a postgres and the app inside of docker:

    docker compose up --build -d

NOTE: Older systems might use `docker-compose` instead of `docker compose`

## Todo

- Import everything in parallel
- Track files that have already been imported
- If the schema ever changes, it will likely be necessary to load a "full" backup again. There will be an env var to force this
- Track SNS queue instead of polling
