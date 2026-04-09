import gzip
import json
from collections import defaultdict
from datetime import date

YEARS = list(range(2018, 2026))


def year_metrics(path):
    totals = defaultdict(float)
    monthly_totals = defaultdict(float)
    sums = defaultdict(lambda: defaultdict(float))
    dates = defaultdict(set)
    weekday_cache = {}

    with gzip.open(path, "rt", newline="") as handle:
        for line in handle:
            row = line.rstrip("\n").split(",")
            if len(row) != 5:
                continue
            date_text, hour, origin, _dest, trips = row
            try:
                trips_value = float(trips)
            except ValueError:
                continue

            totals[origin] += trips_value
            monthly_totals[date_text[:7]] += trips_value

            is_weekday = weekday_cache.get(date_text)
            if is_weekday is None:
                is_weekday = date(
                    int(date_text[0:4]),
                    int(date_text[5:7]),
                    int(date_text[8:10]),
                ).weekday() < 5
                weekday_cache[date_text] = is_weekday

            if not is_weekday:
                continue

            sums[origin][hour] += trips_value
            dates[origin].add(date_text)

    annual = {station: int(round(value)) for station, value in sorted(totals.items())}
    averages = {}
    for station, hourly in sorted(sums.items()):
        weekday_count = len(dates[station]) or 1
        averages[station] = {
            str(hour): round(hourly.get(str(hour), 0.0) / weekday_count, 1)
            for hour in range(24)
        }

    monthly = [
        {"month": month, "total": int(round(total))}
        for month, total in sorted(monthly_totals.items())
    ]

    return annual, averages, monthly


station_totals = {}
station_hourly_avg = {}
monthly_system_totals = []
for year in YEARS:
    annual, averages, monthly = year_metrics(f"date-hour-soo-dest-{year}.csv.gz")
    station_totals[str(year)] = annual
    station_hourly_avg[str(year)] = averages
    monthly_system_totals.extend(monthly)

payload = {
    "years": YEARS,
    "station_totals": station_totals,
    "station_hourly_avg": station_hourly_avg,
    "monthly_system_totals": monthly_system_totals,
}

print(json.dumps(payload, separators=(",", ":")))
