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

COLUMNS = [
    "ID", "Severity", "Start_Time", "End_Time", "Start_Lat", "Start_Lng",
    "Street", "City", "State", "Weather_Condition", "Temperature(F)",
    "Visibility(mi)", "Junction", "Traffic_Signal", "Sunrise_Sunset",
]


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
            writer.writerow(["" if rec[c] is None else rec[c] for c in COLUMNS])
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