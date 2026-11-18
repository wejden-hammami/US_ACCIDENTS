#!/usr/bin/env python3
"""generate_logs.py : transforme US_Accidents_March23.csv en lignes de log pour Flume.

Une ligne de log par accident, ajoutee a la fin de accidents.log.
Format : [Start_Time] ACCIDENT cle=valeur|cle=valeur|...
"""
import argparse
import csv
import os
import sys
import time

# (colonne du CSV, nom commun) - noms communs = ceux de la table MySQL de Sqoop, en minuscules
FIELDS = [
    ("ID", "id"), ("Severity", "severity"), ("End_Time", "end_time"),
    ("Start_Lat", "start_lat"), ("Start_Lng", "start_lng"), ("Street", "street"),
    ("City", "city"), ("State", "state"), ("Weather_Condition", "weather_condition"),
    ("Temperature(F)", "temperature_f"), ("Visibility(mi)", "visibility_mi"),
    ("Junction", "junction"), ("Traffic_Signal", "traffic_signal"),
    ("Sunrise_Sunset", "sunrise_sunset"),
]
BOOL_COLS = {"Junction", "Traffic_Signal"}   # True/False -> 1/0 (comme Kafka et MySQL)
TIME_COLS = {"Start_Time", "End_Time"}       # on garde YYYY-MM-DD HH:MM:SS
NEEDED = ["Start_Time"] + [c for c, _ in FIELDS]


def clean(col, value):
    value = (value or "").strip()
    if value == "":
        return ""                            # valeur manquante : vide (comme Kafka)
    if col in BOOL_COLS:
        return "1" if value.lower() == "true" else "0"
    if col in TIME_COLS:
        return value[:19]                    # supprime les fractions de secondes
    # le separateur du log est '|' et une ligne = un evenement
    return value.replace("|", " ").replace("\r", " ").replace("\n", " ")


def main():
    p = argparse.ArgumentParser(description="Genere accidents.log a partir du CSV US Accidents")
    p.add_argument("--csv", required=True, help="chemin du fichier US_Accidents_March23.csv")
    p.add_argument("--out", default=os.path.join("flume", "logs", "accidents.log"))
    p.add_argument("--limit", type=int, default=1000000, help="nombre de lignes (defaut 1000000)")
    p.add_argument("--delay", type=float, default=0.0, help="secondes entre 2 lignes (0 = vitesse max)")
    p.add_argument("--force", action="store_true", help="ecraser accidents.log s'il existe deja")
    args = p.parse_args()

    if os.path.exists(args.out) and os.path.getsize(args.out) > 0 and not args.force:
        print(f"[STOP] {args.out} existe deja et n'est pas vide.")
        print("       Arrete Flume, vide le dossier HDFS, puis relance avec --force.")
        sys.exit(1)
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)

    written = 0
    start = time.time()
    every = 50000 if args.delay == 0 else 100
    # newline="\n" : fins de ligne Linux, meme sous Windows (sinon un \r reste dans chaque evenement)
    with open(args.csv, newline="", encoding="utf-8-sig") as src, \
         open(args.out, "w", encoding="utf-8", newline="\n") as out:
        reader = csv.reader(src)
        header = next(reader)
        missing = [c for c in NEEDED if c not in header]
        if missing:
            print(f"[ERREUR] colonnes absentes du CSV : {missing}")
            sys.exit(1)
        idx = {c: header.index(c) for c in NEEDED}
        try:
            for row in reader:
                if written >= args.limit:
                    break
                get = lambda c: row[idx[c]] if idx[c] < len(row) else ""
                fields = "|".join(f"{name}={clean(col, get(col))}" for col, name in FIELDS)
                out.write(f"[{clean('Start_Time', get('Start_Time'))}] ACCIDENT {fields}\n")
                written += 1
                if args.delay > 0:
                    out.flush()              # Flume doit voir la ligne tout de suite
                    time.sleep(args.delay)
                elif written % 1000 == 0:
                    out.flush()
                if written % every == 0:
                    print(f"{written} lignes ecrites ({time.time() - start:.0f}s)")
        except KeyboardInterrupt:
            print("\n[INTERROMPU] Ctrl+C")
        out.flush()
    print(f"TERMINE : {written} lignes ecrites dans {args.out} en {time.time() - start:.0f}s")


if __name__ == "__main__":
    main()
