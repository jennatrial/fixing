def aggregate_by_ggid(data):
    # Sort the data by workbook priority in descending order
    data_sorted = sorted(data, key=lambda x: x['workbook_priority'], reverse=True)
    grouped_data = {}

    for entry in data_sorted:
        ggid = entry['ggid']
        if ggid not in grouped_data:
            grouped_data[ggid] = entry

    return list(grouped_data.values())