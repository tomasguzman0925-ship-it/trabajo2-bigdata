"""Trabajo 2, Parte B: Lambda que lee el stream de Kinesis y calcula ventanas de un minuto.

Flujo: productor -> Kinesis (landsat-escenas) -> esta Lambda -> DynamoDB.

El disparador de Kinesis tiene una ventana tumbling de 60 segundos. Dentro de cada ventana,
Lambda invoca la función varias veces (un lote de registros cada vez) y le pasa en
event["state"] lo acumulado hasta ese momento; la función devuelve el estado nuevo. En la
última invocación de la ventana (isFinalInvokeForWindow = true) guarda el resultado.

Cada registro pasa por dos filtros, en este orden:
  1. Validación: si no cumple el esquema, va a landsat_invalidos con su motivo.
  2. Repetido: si su event_id ya llegó en otro registro, va a landsat_repetidos.
Si pasa los dos es único: suma 1 al conteo y su nubosidad a la suma de su satélite.

La llave de landsat_invalidos y landsat_repetidos es el sequence number del registro en
Kinesis, que es único. Lambda puede procesar un lote más de una vez (por ejemplo, si una
invocación falla); en ese caso landsat_vistos ya tiene el event_id con el mismo sequence
number, y el registro se cuenta como único y no como repetido.
"""
import base64
import json
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
windows = dynamodb.Table(os.environ.get("TABLA_VENTANAS", "landsat_ventanas"))
seen = dynamodb.Table(os.environ.get("TABLA_VISTOS", "landsat_vistos"))
repeated = dynamodb.Table(os.environ.get("TABLA_REPETIDOS", "landsat_repetidos"))
invalid = dynamodb.Table(os.environ.get("TABLA_INVALIDOS", "landsat_invalidos"))

REQUIRED = ("event_id", "scene_id", "satellite", "wrs_path", "wrs_row",
            "date_acquired", "land_cloud_cover", "sent_at_ms")


def validate(event):
    """Return None if the event follows the schema, otherwise the reason it is discarded."""
    if not isinstance(event, dict):
        return "no_es_objeto_json"
    missing = [f for f in REQUIRED if event.get(f) in (None, "")]
    if missing:
        return "falta_campo:" + ",".join(missing)
    if event["satellite"] not in (8, 9):
        return "satelite_invalido"
    if not (isinstance(event["wrs_path"], int) and 1 <= event["wrs_path"] <= 233):
        return "wrs_path_invalido"
    if not (isinstance(event["wrs_row"], int) and 1 <= event["wrs_row"] <= 248):
        return "wrs_row_invalido"
    cloud = event["land_cloud_cover"]
    if not (isinstance(cloud, (int, float)) and 0 <= cloud <= 100):
        return "nubosidad_fuera_de_rango"
    if not isinstance(event["sent_at_ms"], int):
        return "sent_at_ms_invalido"
    return None


def is_repeated(event_id, sequence):
    """Registra el event_id la primera vez que llega. True si ya había llegado en otro registro."""
    try:
        seen.put_item(Item={"event_id": event_id, "seq": sequence},
                      ConditionExpression="attribute_not_exists(event_id)")
        return False
    except ClientError as error:
        if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
    first = seen.get_item(Key={"event_id": event_id}, ConsistentRead=True)["Item"]
    return first["seq"] != sequence


def save_window(window, state):
    for satellite, acc in state.items():
        windows.put_item(Item={
            "satelite": int(satellite),
            "inicio_ventana": window["start"],
            "fin_ventana": window["end"],
            "conteo": acc["conteo"],
            "suma_nubosidad": Decimal(str(round(acc["suma"], 2))),
            "promedio_nubosidad": Decimal(str(round(acc["suma"] / acc["conteo"], 2))),
        })


def lambda_handler(event, context):
    state = event.get("state") or {}
    batch = {"unicos": 0, "repetidos": 0, "invalidos": 0}
    for record in event.get("Records", []):
        sequence = record["kinesis"]["sequenceNumber"]
        arrival = datetime.fromtimestamp(record["kinesis"]["approximateArrivalTimestamp"],
                                         timezone.utc).isoformat(timespec="milliseconds")
        try:
            text = base64.b64decode(record["kinesis"]["data"]).decode("utf-8").strip()
            body = json.loads(text)
            reason = validate(body)
        except ValueError:
            text, reason = record["kinesis"]["data"], "json_invalido"

        if reason:
            invalid.put_item(Item={"seq": sequence, "motivo": reason, "evento": text,
                                   "llegada": arrival})
            batch["invalidos"] += 1
            continue

        if is_repeated(body["event_id"], sequence):
            repeated.put_item(Item={"seq": sequence, "event_id": body["event_id"],
                                    "llegada": arrival})
            batch["repetidos"] += 1
            continue

        acc = state.setdefault(str(body["satellite"]), {"conteo": 0, "suma": 0.0})
        acc["conteo"] += 1
        acc["suma"] += body["land_cloud_cover"]
        batch["unicos"] += 1

    final = bool(event.get("isFinalInvokeForWindow"))
    if final:
        save_window(event["window"], state)
    print(json.dumps({"ventana": event.get("window"), "final": final, "lote": batch,
                      "estado": state}))
    return {"state": state}
