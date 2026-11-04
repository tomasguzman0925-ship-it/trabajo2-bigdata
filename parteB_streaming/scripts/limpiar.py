"""Trabajo 2, Parte B: vacía las tablas y los eventos crudos después de un ensayo.

Uso (en AWS CloudShell):
    python3 limpiar.py --bucket landsat-trabajo2-123456789012 --si

Borra todos los ítems de las cuatro tablas de DynamoDB y los archivos de crudos/ en S3,
para que la corrida que se graba empiece desde cero. No borra las tablas, el stream, la
Lambda ni el bucket. Sin --si solo muestra cuántos ítems y archivos borraría.
"""
import argparse

import boto3

TABLES = {
    "landsat_ventanas": ["satelite", "inicio_ventana"],
    "landsat_vistos": ["event_id"],
    "landsat_repetidos": ["seq"],
    "landsat_invalidos": ["seq"],
}


def keys_of(table, key_names):
    kwargs = {"ProjectionExpression": ", ".join(f"#k{i}" for i in range(len(key_names))),
              "ExpressionAttributeNames": {f"#k{i}": k for i, k in enumerate(key_names)}}
    while True:
        page = table.scan(**kwargs)
        yield from page["Items"]
        if "LastEvaluatedKey" not in page:
            return
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefijo", default="crudos/")
    parser.add_argument("--si", action="store_true")
    args = parser.parse_args()

    dynamodb = boto3.resource("dynamodb")
    for name, key_names in TABLES.items():
        table = dynamodb.Table(name)
        keys = list(keys_of(table, key_names))
        if args.si:
            with table.batch_writer() as batch:
                for key in keys:
                    batch.delete_item(Key=key)
        print(f"{name}: {len(keys)} ítems {'borrados' if args.si else 'por borrar'}")

    s3 = boto3.resource("s3")
    objects = list(s3.Bucket(args.bucket).objects.filter(Prefix=args.prefijo))
    if args.si:
        for obj in objects:
            obj.delete()
    print(f"s3://{args.bucket}/{args.prefijo}: {len(objects)} archivos "
          f"{'borrados' if args.si else 'por borrar'}")
    if not args.si:
        print("Para borrar de verdad, agrega --si")


if __name__ == "__main__":
    main()
