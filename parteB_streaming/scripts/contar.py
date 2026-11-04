"""Trabajo 2, Parte B: comprueba que los conteos cuadran y responde la pregunta con DynamoDB.

Uso (en AWS CloudShell, en la carpeta donde quedó publicados.json):
    python3 contar.py --bucket landsat-trabajo2-123456789012

Cuenta:
  - publicados: los que el productor publicó con éxito (publicados.json).
  - únicos procesados, repetidos descartados e inválidos descartados: los ítems de las
    tablas landsat_vistos, landsat_repetidos y landsat_invalidos.
  - crudos: las líneas de los archivos que Firehose dejó en S3, uno por evento recibido.
Y consulta landsat_ventanas por satélite para responder la pregunta del proyecto.
"""
import argparse
import json
from collections import Counter

import boto3
from boto3.dynamodb.conditions import Key


def count_items(client, table):
    total = 0
    for page in client.get_paginator("scan").paginate(TableName=table, Select="COUNT"):
        total += page["Count"]
    return total


def invalid_reasons(table):
    reasons, kwargs = Counter(), {"ProjectionExpression": "#m",
                                  "ExpressionAttributeNames": {"#m": "motivo"}}
    while True:
        page = table.scan(**kwargs)
        reasons.update(item["motivo"] for item in page["Items"])
        if "LastEvaluatedKey" not in page:
            return reasons
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def windows_of(table, satellite):
    """Las ventanas de un satélite, en orden de tiempo: una consulta (Query) por la llave."""
    items, kwargs = [], {"KeyConditionExpression": Key("satelite").eq(satellite)}
    while True:
        page = table.query(**kwargs)
        items += page["Items"]
        if "LastEvaluatedKey" not in page:
            return items
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def count_raw(s3, bucket, prefix):
    lines = files = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            lines += sum(1 for line in body.splitlines() if line.strip())
            files += 1
    return lines, files


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefijo", default="crudos/")
    parser.add_argument("--publicados", default="publicados.json")
    args = parser.parse_args()

    with open(args.publicados, encoding="utf-8") as f:
        sent = json.load(f)
    client = boto3.client("dynamodb")
    dynamodb = boto3.resource("dynamodb")
    unique = count_items(client, "landsat_vistos")
    repeated = count_items(client, "landsat_repetidos")
    reasons = invalid_reasons(dynamodb.Table("landsat_invalidos"))
    invalid = sum(reasons.values())
    raw, files = count_raw(boto3.client("s3"), args.bucket, args.prefijo)
    windows = {sat: windows_of(dynamodb.Table("landsat_ventanas"), sat) for sat in (8, 9)}
    in_windows = sum(int(w["conteo"]) for items in windows.values() for w in items)

    print("CONTEOS")
    print(f"  Publicados por el productor ........ {sent['publicados']:>6}")
    print(f"  Únicos procesados (landsat_vistos) . {unique:>6}   (el productor envió {sent['unicos']})")
    print(f"  Repetidos descartados .............. {repeated:>6}   (el productor envió {sent['repetidos']})")
    print(f"  Inválidos descartados .............. {invalid:>6}   (el productor envió {sent['invalidos']})")
    total = unique + repeated + invalid
    verdict = "CUADRA" if total == sent["publicados"] else "NO CUADRA"
    print(f"  Únicos + repetidos + inválidos ..... {total:>6}   -> {verdict}")
    print(f"  Eventos crudos en S3 ............... {raw:>6}   ({files} archivos de Firehose)")
    print(f"  Suma de conteos de las ventanas .... {in_windows:>6}   (debe ser igual a los únicos)")

    print("\nINVÁLIDOS POR MOTIVO (descartados / enviados)")
    for reason in sorted(set(reasons) | set(sent["invalidos_por_motivo"])):
        print(f"  {reason:<32} {reasons[reason]:>5} / {sent['invalidos_por_motivo'].get(reason, 0)}")

    print("\nVENTANAS DE UN MINUTO (Query a landsat_ventanas por satélite)")
    for sat, items in windows.items():
        for w in items:
            print(f"  Landsat {sat}  {w['inicio_ventana']}  {int(w['conteo']):>4} escenas"
                  f"  nubosidad promedio {float(w['promedio_nubosidad']):6.2f} %")

    print("\nPREGUNTA: ¿qué satélite entregó escenas más despejadas y en qué minuto?")
    for sat, items in windows.items():
        if not items:
            continue
        count = sum(int(w["conteo"]) for w in items)
        average = sum(float(w["suma_nubosidad"]) for w in items) / count
        best = min(items, key=lambda w: w["promedio_nubosidad"])
        print(f"  Landsat {sat}: {count} escenas, nubosidad promedio {average:.2f} %;"
              f" su minuto más despejado fue {best['inicio_ventana']}"
              f" ({float(best['promedio_nubosidad']):.2f} %)")


if __name__ == "__main__":
    main()
