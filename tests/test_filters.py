# if this returns true, the row is included. that seems backwards from the "filter" methods
from neynar_parquet_importer.row_filters import include_row


EXAMPLE_FILTERS = {
    "farcaster.casts": {"data.fid": {"$in": [191, 194]}},
    "farcaster.fids": {"data.fid": {"$in": [191, 194]}},
    "farcaster.reactions": {
        "$or": [
            {"data.fid": {"$in": [191, 194]}},
            {"data.target_fid": {"$in": [191, 194]}},
        ]
    },
    "farcaster.verifications": {"data.fid": {"$in": [191, 194]}},
    "farcaster.followers": {
        "$or": [
            {"data.fid": {"$in": [191, 194]}},
            {"data.target_fid": {"$in": [191, 194]}},
        ]
    },
    "farcaster.profiles": {"data.fid": {"$in": [191, 194]}},
    "farcaster.follow_counts": {"data.fid": {"$in": [191, 194]}},
    "farcaster.neynar_user_scores": {
        "$or": [
            {"data.fid": {"$in": [191, 194]}},
            # # TODO: filtering on score is risky. if a score goes down, it will be ignored and NOT updated!
            # {"data.score": {"$gt": 0.5}},
        ]
    },
    "farcaster.channel_members": {
        "data.channel_id": {"$in": ["neynar", "farville"]},
    },
    "farcaster.user_labels": {
        "data.target_fid": {"$in": [191, 194]},
    },
}


def test_filter_casts():
    table_schema = "farcaster"
    table_name = "casts"

    parquet_name = f"{table_schema}.{table_name}"

    # TODO: real rows have more data, but i think this is enough of mocked data for these tests
    included_rows = [
        {
            "fid": 191,
        },
        {
            "fid": 194,
        },
    ]
    excluded_rows = [
        {
            "fid": 3,
        }
    ]

    filters = EXAMPLE_FILTERS[parquet_name]

    for row in included_rows:
        assert include_row(row, filters)

    for row in excluded_rows:
        assert not include_row(row, filters)


def test_filter_reactions():
    table_schema = "farcaster"
    table_name = "reactions"

    parquet_name = f"{table_schema}.{table_name}"

    # TODO: real rows have more data, but i think this is enough of mocked data for these tests
    included_rows = [
        {
            "fid": 191,
            "target_fid": 194,
        },
        {
            "fid": 194,
            "target_fid": 191,
        },
        {
            "fid": 191,
            "target_fid": 3,
        },
        {
            "fid": 3,
            "target_fid": 191,
        },
    ]
    excluded_rows = [
        {
            "fid": 3,
            "target_fid": 4,
        }
    ]

    filters = EXAMPLE_FILTERS[parquet_name]

    for row in included_rows:
        assert include_row(row, filters)

    for row in excluded_rows:
        assert not include_row(row, filters)


def test_filter_channel_members():
    table_schema = "farcaster"
    table_name = "channel_members"

    parquet_name = f"{table_schema}.{table_name}"

    # TODO: real rows have more data, but i think this is enough of mocked data for these tests
    included_rows = [
        {
            "channel_id": "neynar",
        },
        {
            "channel_id": "farville",
        },
    ]
    excluded_rows = [
        {
            "channel_id": "not_neynar",
        }
    ]

    filters = EXAMPLE_FILTERS[parquet_name]

    for row in included_rows:
        assert include_row(row, filters)

    for row in excluded_rows:
        assert not include_row(row, filters)
