import csv
import json
import os
import time
from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "us_accidents")
CSV_PATH = os.getenv("CSV_PATH", "/data/US_Accidents_March23.csv")
LIMIT = int(os.getenv("LIMIT", "1000000"))
DELAY = float(os.getenv("DELAY", "0"))  # secondes entre 2 messages (0 = vitesse max)

# 15 colonnes, noms originaux du CSV (avec parentheses)
COLUMNS = [
    "ID", "Severity", "Start_Time", "End_Time", "Start_Lat", "Start_Lng",
    "Street", "City", "State", "Weather_Condition", "Temperature(F)",
    "Visibility(mi)", "Junction", "Traffic_Signal", "Sunrise_Sunset",
]
BOOL_COLS = {"Junction", "Traffic_Signal"}   # True/False -> 1/0 (TINYINT comme MySQL)
TIME_COLS = {"Start_Time", "End_Time"}       # on garde 'YYYY-MM-DD HH:MM:SS'


def clean(col, value):
    value = (value or "").strip()
    if value == "":
        return None                          # valeur manquante
    if col in BOOL_COLS:
        return 1 if value.lower() == "true" else 0
    if col in TIME_COLS:
        return value[:19]                    # supprime les fractions de secondes
    return value


def main():
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",
        "linger.ms": 50,
        "compression.type": "lz4",
    })
    stats = {"errors": 0}

    def on_delivery(err, msg):
        if err is not None:
            stats["errors"] += 1
            print(f"[ERREUR] {err}")

    sent = 0
    start = time.time()
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if sent >= LIMIT:
                break
            record = {c: clean(c, row.get(c)) for c in COLUMNS}
            payload = json.dumps(record, ensure_ascii=False).encode("utf-8")
            while True:
                try:
                    producer.produce(TOPIC, key=record["ID"], value=payload,
                                     on_delivery=on_delivery)
                    break
                except BufferError:          # file locale pleine : on laisse partir
                    producer.poll(0.5)
            producer.poll(0)
            sent += 1
            if sent % 50000 == 0:
                print(f"{sent} messages envoyes ({time.time() - start:.0f}s)")
            if DELAY > 0:
                time.sleep(DELAY)

    producer.flush()
    print(f"TERMINE : {sent} messages, {stats['errors']} erreurs, "
          f"{time.time() - start:.0f}s")


if __name__ == "__main__":
    main()