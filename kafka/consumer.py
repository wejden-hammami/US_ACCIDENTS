import csv
import io
import json
import os
import time
from confluent_kafka import Consumer
from hdfs import InsecureClient

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "us_accidents")
WEBHDFS_URL = os.getenv("WEBHDFS_URL", "http://namenode:9870")
HDFS_DIR = os.getenv("HDFS_DIR", "/data/us_accidents/raw/kafka")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50000"))
IDLE_TIMEOUT = int(os.getenv("IDLE_TIMEOUT", "60"))

# Ordre des colonnes dans les fichiers HDFS (identique pour tous : pas d'en-tete)
COLUMNS = [
    "id", "severity", "start_time", "end_time", "start_lat", "start_lng",
    "street", "city", "state", "weather_condition", "temperature_f",
    "visibility_mi", "junction", "traffic_signal", "sunrise_sunset",
]

# Anciens noms (messages envoyes par la 1re version du producer) -> lecture compatible
LEGACY = {
    "id": "ID", "severity": "Severity", "start_time": "Start_Time",
    "end_time": "End_Time", "start_lat": "Start_Lat", "start_lng": "Start_Lng",
    "street": "Street", "city": "City", "state": "State",
    "weather_condition": "Weather_Condition", "temperature_f": "Temperature(F)",
    "visibility_mi": "Visibility(mi)", "junction": "Junction",
    "traffic_signal": "Traffic_Signal", "sunrise_sunset": "Sunrise_Sunset",
}


def value_of(rec, name):
    if name in rec:
        return rec[name]
    return rec.get(LEGACY[name])


def main():
    hdfs = InsecureClient(WEBHDFS_URL, user="hadoop")
    hdfs.makedirs(HDFS_DIR)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "hdfs-writer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,         # commit APRES ecriture HDFS
    })
    consumer.subscribe([TOPIC])

    batch, total, file_no = [], 0, 0
    run_id = int(time.time())
    last_msg = time.time()

    def flush():
        nonlocal batch, total, file_no
        if not batch:
            return
        buf = io.StringIO()
        writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
        for rec in batch:
            row = []
            for c in COLUMNS:
                v = value_of(rec, c)
                row.append("" if v is None else v)
            writer.writerow(row)
        path = f"{HDFS_DIR}/part-{run_id}-{file_no:05d}.csv"
        hdfs.write(path, data=buf.getvalue(), encoding="utf-8", overwrite=False)
        consumer.commit(asynchronous=False)
        total += len(batch)
        file_no += 1
        print(f"ecrit {path} ({len(batch)} lignes, total {total})")
        batch = []

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if total + len(batch) > 0 and time.time() - last_msg > IDLE_TIMEOUT:
                    break                    # plus de nouveaux messages
                continue
            if msg.error():
                print(f"[ERREUR] {msg.error()}")
                continue
            last_msg = time.time()
            batch.append(json.loads(msg.value().decode("utf-8")))
            if len(batch) >= BATCH_SIZE:
                flush()
        flush()
    finally:
        consumer.close()
    print(f"TERMINE : {total} lignes ecrites dans {HDFS_DIR}")


if __name__ == "__main__":
    main()
