
import csv
import os
from datetime import datetime

INPUT_FILE = "dataset/raw/US_Accidents_March23.csv"

ALL_OUTPUT = "dataset/processed/cleaned_accidents.tsv"
NIGHT_OUTPUT = "dataset/processed/nighttime_accidents.tsv"

os.makedirs("dataset/processed", exist_ok=True)

# Fields required for our analysis
FIELDS = [
    "ID",
    "Start_Time",
    "Year",
    "Month",
    "DayOfWeek",
    "Hour",
    "Severity",
    "State",
    "City",
    "Weather_Condition",
    "Temperature(F)",
    "Humidity(%)",
    "Visibility(mi)",
    "Start_Lat",
    "Start_Lng",
    "Sunrise_Sunset",
    "Traffic_Signal",
    "Junction",
    "Crossing"
]

def clean(value):
    """Remove unwanted whitespace and preserve TSV structure."""
    if value is None:
        return ""

    return str(value).replace("\t", " ").replace("\n", " ").replace("\r", " ").strip()


total_records = 0
night_records = 0
invalid_timestamps = 0

print("Starting preprocessing...")

with open(INPUT_FILE, "r", encoding="utf-8-sig", newline="") as source, \
     open(ALL_OUTPUT, "w", encoding="utf-8", newline="") as all_file, \
     open(NIGHT_OUTPUT, "w", encoding="utf-8", newline="") as night_file:

    reader = csv.DictReader(source)

    all_writer = csv.writer(all_file, delimiter="\t")
    night_writer = csv.writer(night_file, delimiter="\t")

    all_writer.writerow(FIELDS)
    night_writer.writerow(FIELDS)

    for row in reader:

        total_records += 1

        try:
            start_time = datetime.fromisoformat(
                row["Start_Time"].strip().replace("Z", "+00:00")
            )
        except (ValueError, AttributeError):
            invalid_timestamps += 1
            continue

        # Extract temporal attributes
        year = start_time.year
        month = start_time.month
        weekday = start_time.strftime("%A")
        hour = start_time.hour

        # Build standardized output record
        record = [
            clean(row.get("ID")),
            start_time.isoformat(sep=" "),
            year,
            month,
            weekday,
            hour,
            clean(row.get("Severity")),
            clean(row.get("State")),
            clean(row.get("City")),
            clean(row.get("Weather_Condition")),
            clean(row.get("Temperature(F)")),
            clean(row.get("Humidity(%)")),
            clean(row.get("Visibility(mi)")),
            clean(row.get("Start_Lat")),
            clean(row.get("Start_Lng")),
            clean(row.get("Sunrise_Sunset")),
            clean(row.get("Traffic_Signal")),
            clean(row.get("Junction")),
            clean(row.get("Crossing"))
        ]

        # Save every valid-timestamp accident
        all_writer.writerow(record)

        # Nighttime: 8 PM to 5:59 AM
        if hour >= 20 or hour < 6:
            night_writer.writerow(record)
            night_records += 1

        if total_records % 500000 == 0:
            print(f"Processed {total_records:,} records...")

print("\nPreprocessing completed!")

print(f"Total records read: {total_records:,}")
print(f"Nighttime records: {night_records:,}")
print(f"Invalid timestamps: {invalid_timestamps:,}")

print(f"\nAll accidents: {ALL_OUTPUT}")
print(f"Nighttime accidents: {NIGHT_OUTPUT}")
