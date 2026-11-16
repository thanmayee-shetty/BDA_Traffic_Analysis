
import csv
import os

file_path = "dataset/raw/US_Accidents_March23.csv"

print("Checking dataset...")

# 1. File size
size_gb = os.path.getsize(file_path) / (1024 ** 3)
print(f"\nFile size: {size_gb:.2f} GB")

# 2. Read column names
with open(file_path, "r", encoding="utf-8", newline="") as file:
    reader = csv.reader(file)
    headers = next(reader)

print(f"\nTotal columns: {len(headers)}")

print("\nCOLUMN NAMES:")
for index, column in enumerate(headers, start=1):
    print(f"{index}. {column}")

# 3. Display first 5 records
print("\nFIRST 5 RECORDS:")

with open(file_path, "r", encoding="utf-8", newline="") as file:
    reader = csv.DictReader(file)

    for index, row in enumerate(reader):
        if index == 5:
            break

        print(f"\nRecord {index + 1}:")
        for key in ["ID", "Severity", "Start_Time",
                    "State", "Weather_Condition",
                    "Start_Lat", "Start_Lng"]:
            print(f"{key}: {row.get(key)}")

