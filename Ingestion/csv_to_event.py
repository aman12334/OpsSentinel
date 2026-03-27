import csv
import json
from datetime import datetime

INPUT_CSV = "../data/trucks.csv"
OUTPUT_JSON = "../data/events.jsonl"

with open(INPUT_CSV, newline="") as csvfile, open(OUTPUT_JSON, "w") as outfile:
    reader = csv.DictReader(csvfile)

    for row in reader:
        event = {
            "schema_version": "1.0",
            "event_id": f"evt_{row['truck_id']}",
            "event_type": "truck_observed",
            "source": "csv",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "entity": {
                "type": "truck",
                "id": row["truck_id"]
            },
            "text": f"Truck {row['truck_id']} observed in dataset",
            "signals": row,
            "confidence": 1.0
        }

        outfile.write(json.dumps(event) + "\n")
