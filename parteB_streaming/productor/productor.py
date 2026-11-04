"""Trabajo 2, Parte B: publica escenas Landsat en un stream de Kinesis Data Streams.

Uso (en AWS CloudShell, después de subir este archivo y escenas_semilla.csv):
    python3 productor.py --prueba     # muestra algunos eventos y no llama a AWS
    python3 productor.py              # publica durante 6 minutos

Cada segundo publica --por-segundo eventos con PutRecords, durante --minutos minutos.
Cada evento es una escena real del catálogo Landsat 8-9 (escenas_semilla.csv). A propósito:
  - repetidos (--repetidos, 5 % por defecto): copia exacta de un evento válido ya
    publicado, con el mismo event_id. La aplicación de ventanas no debe contarlo dos veces.
  - inválidos (--invalidos, 5 % por defecto): un evento dañado de una de las formas de
    DAMAGES. El nombre de cada daño es el motivo con el que la Lambda lo descarta.
Al terminar escribe publicados.json con los conteos, que usa contar.py.
"""
import argparse
import csv
import json
import random
import time
import uuid
from collections import Counter
from datetime import datetime, timezone

STREAM = "landsat-escenas"
MAX_ATTEMPTS = 5

DAMAGES = [
    ("falta_campo:scene_id", lambda e: e.pop("scene_id")),
    ("falta_campo:land_cloud_cover", lambda e: e.pop("land_cloud_cover")),
    ("satelite_invalido", lambda e: e.update(satellite=7)),
    ("nubosidad_fuera_de_rango", lambda e: e.update(land_cloud_cover=150.0)),
    ("wrs_path_invalido", lambda e: e.update(wrs_path=999)),
]


def load_seed(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build_event(row):
    return {
        "event_id": str(uuid.uuid4()),
        "scene_id": row["product_id_l2"],
        "satellite": int(row["satellite"]),
        "wrs_path": int(row["wrs_path"]),
        "wrs_row": int(row["wrs_row"]),
        "date_acquired": row["date_acquired"].replace("/", "-"),
        "land_cloud_cover": float(row["land_cloud_cover"]),
        "station": row["station_id"],
        "sent_at_ms": int(time.time() * 1000),
    }


def to_record(kind, reason, event):
    """Un registro listo para PutRecords: el JSON en una línea, con salto de línea al final."""
    data = (json.dumps(event, ensure_ascii=False) + "\n").encode("utf-8")
    return {"tipo": kind, "motivo": reason, "data": data, "key": event["event_id"]}


def plan_second(rng, rows, n, sent_valid, args):
    """Los eventos de un segundo: únicos, repetidos e inválidos."""
    records = []
    for _ in range(n):
        draw = rng.random()
        if draw < args.repetidos and sent_valid:
            original = rng.choice(sent_valid)
            records.append({**original, "tipo": "repetido"})
            continue
        event = build_event(rng.choice(rows))
        if draw < args.repetidos + args.invalidos:
            reason, damage = rng.choice(DAMAGES)
            damage(event)
            records.append(to_record("invalido", reason, event))
        else:
            records.append(to_record("unico", None, event))
    return records


def put_with_retries(kinesis, records):
    """Publica con PutRecords y reintenta los registros que fallen. Devuelve (publicados, perdidos)."""
    sent, pending = [], records
    for attempt in range(MAX_ATTEMPTS):
        response = kinesis.put_records(
            StreamName=STREAM,
            Records=[{"Data": r["data"], "PartitionKey": r["key"]} for r in pending])
        failed = []
        for record, result in zip(pending, response["Records"]):
            (failed if "ErrorCode" in result else sent).append(record)
        if not failed:
            return sent, []
        pending = failed
        time.sleep(0.2 * (attempt + 1))
    return sent, pending


def show_sample(rng, rows, args):
    sent_valid, counts = [], Counter()
    for _ in range(3):
        for record in plan_second(rng, rows, args.por_segundo, sent_valid, args):
            counts[record["tipo"]] += 1
            if record["tipo"] == "unico":
                sent_valid.append(record)
            if counts[record["tipo"]] <= 2:
                print(record["tipo"], record["motivo"] or "", record["data"].decode().strip())
    print(f"En 3 segundos: {dict(counts)}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutos", type=float, default=6)
    parser.add_argument("--por-segundo", type=int, default=20)
    parser.add_argument("--repetidos", type=float, default=0.05)
    parser.add_argument("--invalidos", type=float, default=0.05)
    parser.add_argument("--semilla", default="escenas_semilla.csv")
    parser.add_argument("--prueba", action="store_true")
    args = parser.parse_args()

    rows = load_seed(args.semilla)
    rng = random.Random(42)
    if args.prueba:
        show_sample(rng, rows, args)
        return

    import boto3
    kinesis = boto3.client("kinesis")
    seconds = int(args.minutos * 60)
    sent_valid, counts, reasons, lost = [], Counter(), Counter(), 0
    start = time.time()
    print(f"Publicando {args.por_segundo} eventos por segundo en '{STREAM}' durante {seconds} s ...")
    for second in range(1, seconds + 1):
        records = plan_second(rng, rows, args.por_segundo, sent_valid, args)
        sent, failed = put_with_retries(kinesis, records)
        lost += len(failed)
        for record in sent:
            counts[record["tipo"]] += 1
            if record["tipo"] == "invalido":
                reasons[record["motivo"]] += 1
            elif record["tipo"] == "unico":
                sent_valid.append(record)
        if second % 30 == 0:
            print(f"  {second:>4} s: {sum(counts.values())} publicados {dict(counts)}")
        time.sleep(max(0.0, start + second - time.time()))

    summary = {
        "stream": STREAM,
        "inicio_utc": datetime.fromtimestamp(start, timezone.utc).isoformat(timespec="seconds"),
        "fin_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "segundos": round(time.time() - start, 1),
        "publicados": sum(counts.values()),
        "unicos": counts["unico"],
        "repetidos": counts["repetido"],
        "invalidos": counts["invalido"],
        "invalidos_por_motivo": dict(sorted(reasons.items())),
        "no_publicados": lost,
    }
    with open("publicados.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
