from datetime import timedelta, datetime


def parse_duration(duration_str):
    """Break apart the duration_str in a way to extract the values needed"""

    duration_str = duration_str.replace("P", "").replace("T", "") # Method chaining; P is always there, T might not be there.

    components = ["D", "H", "M", "S"] # Order must be fixed
    values = {"D": 0, "H": 0, "M":0, "S": 0}

    for component in components:

        if component in duration_str:
            value, duration_str = duration_str.split(component) # Example "3H15M" -> value = "3", duration_str = "15M". component = "H"
                                                                # Example "1D3H30M" -> value = "1", duration_str = "3H30M". component = "D"
            values[component] = int(value) # Assign the extracted value to the corresponding component in the dictionary

    total_duration = timedelta(
        days = values["D"], hours = values["H"], minutes = values["M"], seconds = values["S"]
    ) # Output: example timedelta(days=1, hours=3, minutes=30)

    return total_duration



def transform_data(row):

    duration_td = parse_duration(row["Duration"])

    row["Duration"] = (datetime.min + duration_td).time() # datetime.min = 00:00:00. Adding a timedelta to it results in a datetime object, from which we extract only the time component.


    row["Video_Type"] = "Shorts" if duration_td.total_seconds() <= 60 else "Normal"

    return row