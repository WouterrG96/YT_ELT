from datetime import timedelta, datetime


def parse_duration(duration_str):
    """
    Parse a YouTube ISO 8601 duration string (e.g., 'PT1H2M3S', 'PT45S', 'P1DT2H')
    into a Python timedelta.

    Notes:
    - YouTube returns durations like 'PT#H#M#S' (and can include days as 'P#DT...').
    - This parser assumes components appear in descending order (D, H, M, S).
    """

    # Remove ISO 8601 designators so we can split on the unit letters
    # Example: 'PT1H2M3S' -> '1H2M3S'
    duration_str = duration_str.replace("P", "").replace("T", "")

    # Supported units in the order we expect to encounter them
    components = ["D", "H", "M", "S"]

    # Default all units to 0 so missing components are handled cleanly
    values = {"D": 0, "H": 0, "M": 0, "S": 0}

    # For each unit present, split at that unit and capture the preceding numeric value
    # Example: '1H2M3S' -> for 'H': value='1', remainder='2M3S'
    for component in components:
        if component in duration_str:
            value, duration_str = duration_str.split(component)
            values[component] = int(value)

    # Convert extracted parts into a timedelta representing the total duration
    total_duration = timedelta(
        days=values["D"], hours=values["H"], minutes=values["M"], seconds=values["S"]
    )

    return total_duration


def transform_data(row):
    """
    Transform a single record (dict-like) for the curated model:
    - Convert 'Duration' from ISO 8601 string to a Python time object.
    - Add 'Video_Type' derived from duration (Shorts <= 60 seconds).
    """

    # Parse the incoming duration string into a timedelta for easy math
    duration_td = parse_duration(row["Duration"])

    # Convert timedelta to a time-of-day object by adding it to a baseline datetime
    # (This works as long as duration is < 24 hours; longer durations would overflow.)
    row["Duration"] = (datetime.min + duration_td).time()

    # Classify video format based on total duration
    row["Video_Type"] = "Shorts" if duration_td.total_seconds() <= 60 else "Normal"

    return row
