# TODO: these tests should probably be in neynar_parquet_importer instead of here

# if this returns true, the row is included. that seems backwards from the "filter" methods
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
            {"data.score": {"$gt": 0.5}},
        ]
    },
    "farcaster.channel_members": {
        "data.channel_id": {"$in": ["neynar", "farville"]},
    },
    "farcaster.user_labels": {
        "data.target_fid": {"$in": [191, 194]},
    },
}


def allow_row_by_col_data(col_data, filters: dict) -> bool:
    # this returns after the first key. multiple keys will not work right!
    for key, value in filters.items():
        if key == "$in":
            if col_data in value:
                pass
            else:
                return False
        elif key == "$nin":
            if col_data not in value:
                pass
            else:
                return False
        elif key == "$lt":
            if col_data < value:
                pass
            else:
                return False
        elif key == "$lte":
            if col_data <= value:
                pass
            else:
                return False
        elif key == "$gt":
            if col_data > value:
                pass
            else:
                return False
        elif key == "$gte":
            if col_data >= value:
                pass
            else:
                return False
        elif key == "$eq":
            if col_data == value:
                pass
            else:
                return False
        elif key == "$ne":
            if col_data != value:
                pass
            else:
                return False

    return True


def include_row(row: dict, filters: dict | None) -> bool:
    """
    Filters a row based on the provided filters. Rows we want will return "True"
    """
    if filters is None:
        return False

    for key, value in filters.items():
        if key == "$and":
            if all(include_row(row, v) for v in value):
                pass
            else:
                return False
        elif key == "$or":
            if any(include_row(row, v) for v in value):
                pass
            else:
                return False
        elif key.startswith("data."):
            filter_col = key[5:]

            x = row[filter_col]

            if allow_row_by_col_data(x, value):
                pass
            else:
                return False
        else:
            raise ValueError(f"Unknown filter key: {key}")
    return True


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
