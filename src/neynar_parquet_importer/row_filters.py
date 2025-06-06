def include_by_col_data(col_data, filters: dict) -> bool:
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


def include_row(row: dict, filters: dict | None, backfill_start_timestamp: int | None = None, backfill_end_timestamp: int | None = None) -> bool:
    """
    Filters a row based on the provided filters. Rows we want will return "True"
    """
    if backfill_start_timestamp is not None:
        if row["updated_at"] < backfill_start_timestamp:
            return False
    if backfill_end_timestamp is not None:
        if row["updated_at"] > backfill_end_timestamp:
            return False
    if filters is None or len(filters) == 0:
        return True

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

            if include_by_col_data(x, value):
                pass
            else:
                return False
        else:
            raise ValueError(f"Unknown filter key: {key}")

    return True
