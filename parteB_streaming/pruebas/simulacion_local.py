"""Trabajo 2, Parte B: simulación local, sin AWS, de productor -> Lambda -> DynamoDB y S3.

Simula DynamoDB y S3 con moto. Pasa 6 minutos de eventos del productor por la Lambda,
ventana por ventana y en lotes de 100, con un lote que se procesa dos veces para imitar
un reintento de Lambda, y comprueba que:
  - las ventanas guardadas coinciden con un cálculo directo sobre los eventos únicos,
  - contar.py da CUADRA,
  - limpiar.py deja las tablas y crudos/ vacíos.

Uso, desde la raíz del repositorio:
    pip install boto3 "moto[dynamodb,s3]==5.1.4"
    python3 parteB_streaming/pruebas/simulacion_local.py
"""
import base64
import io
import json
import os
import sys
import tempfile
from argparse import Namespace
from collections import Counter, defaultdict
from contextlib import redirect_stdout
from pathlib import Path

os.environ.update(AWS_DEFAULT_REGION="us-east-1", AWS_ACCESS_KEY_ID="testing",
                  AWS_SECRET_ACCESS_KEY="testing")
PARTE_B = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(PARTE_B / "productor"), str(PARTE_B / "lambda"), str(PARTE_B / "scripts")]

import boto3
from moto import mock_aws

BUCKET = "landsat-trabajo2-123456789012"


def create_resources():
    ddb = boto3.client("dynamodb")
    specs = {"landsat_ventanas": [("satelite", "N", "HASH"), ("inicio_ventana", "S", "RANGE")],
             "landsat_vistos": [("event_id", "S", "HASH")],
             "landsat_repetidos": [("seq", "S", "HASH")],
             "landsat_invalidos": [("seq", "S", "HASH")]}
    for name, keys in specs.items():
        ddb.create_table(
            TableName=name, BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[{"AttributeName": k, "AttributeType": t} for k, t, _ in keys],
            KeySchema=[{"AttributeName": k, "KeyType": r} for k, _, r in keys])
    boto3.client("s3").create_bucket(Bucket=BUCKET)


def produce(seconds=360, per_second=20):
    """Los eventos del productor, como si Kinesis los hubiera aceptado todos."""
    import productor
    rows = productor.load_seed(PARTE_B / "productor" / "escenas_semilla.csv")
    args = Namespace(repetidos=0.05, invalidos=0.05)
    rng = productor.random.Random(42)
    sent_valid, counts, reasons, records = [], Counter(), Counter(), []
    t0 = 1793000015.0
    for second in range(seconds):
        for i, r in enumerate(productor.plan_second(rng, rows, per_second, sent_valid, args)):
            counts[r["tipo"]] += 1
            if r["tipo"] == "invalido":
                reasons[r["motivo"]] += 1
            records.append((t0 + second + i / per_second, r))
        sent_valid += [r for _, r in records[-per_second:] if r["tipo"] == "unico"]
    summary = {"publicados": len(records), "unicos": counts["unico"],
               "repetidos": counts["repetido"], "invalidos": counts["invalido"],
               "invalidos_por_motivo": dict(reasons)}
    return records, summary


def run_lambda(records):
    """Invoca la Lambda como lo hace el disparador: por ventana de 60 s y en lotes de 100."""
    import lambda_function
    kinesis = [{"kinesis": {"sequenceNumber": f"{n:056d}", "approximateArrivalTimestamp": ts,
                            "data": base64.b64encode(r["data"]).decode()}}
               for n, (ts, r) in enumerate(records, start=1)]
    by_window = defaultdict(list)
    for rec in kinesis:
        by_window[int(rec["kinesis"]["approximateArrivalTimestamp"] // 60) * 60].append(rec)
    retried = False
    for start, recs in sorted(by_window.items()):
        window = {"start": f"W{start}", "end": f"W{start + 60}"}
        state = {}
        for i in range(0, len(recs), 100):
            event = {"Records": recs[i:i + 100], "window": window,
                     "isFinalInvokeForWindow": False, "state": json.loads(json.dumps(state))}
            with redirect_stdout(io.StringIO()):
                result = lambda_function.lambda_handler(json.loads(json.dumps(event)), None)
                if not retried and i == 200:
                    # La invocación "falla" después de escribir: Lambda descarta su estado y reintenta el lote
                    retried = True
                    result = lambda_function.lambda_handler(json.loads(json.dumps(event)), None)
            state = result["state"]
        with redirect_stdout(io.StringIO()):
            lambda_function.lambda_handler({"Records": [], "window": window,
                                            "isFinalInvokeForWindow": True, "state": state}, None)
    return len(by_window)


def firehose(records):
    """Lo que haría Firehose: archivos en crudos/ con los eventos tal como llegaron."""
    s3 = boto3.client("s3")
    for i in range(0, len(records), 1200):
        body = b"".join(r["data"] for _, r in records[i:i + 1200])
        s3.put_object(Bucket=BUCKET, Key=f"crudos/2026/11/04/21/landsat-crudos-{i}", Body=body)


def expected_windows(records):
    """Lo que debería salir, calculado sin la Lambda: únicos por ventana y satélite."""
    seen, acc = set(), defaultdict(lambda: [0, 0.0])
    for ts, r in records:
        if r["tipo"] != "unico":
            continue
        e = json.loads(r["data"])
        assert e["event_id"] not in seen
        seen.add(e["event_id"])
        a = acc[(e["satellite"], f"W{int(ts // 60) * 60}")]
        a[0] += 1
        a[1] += e["land_cloud_cover"]
    return {k: (n, round(s / n, 2)) for k, (n, s) in acc.items()}


def main():
    with mock_aws():
        create_resources()
        records, summary = produce()
        n_windows = run_lambda(records)
        firehose(records)
        os.chdir(tempfile.mkdtemp())
        with open("publicados.json", "w", encoding="utf-8") as f:
            json.dump(summary, f)

        table = boto3.resource("dynamodb").Table("landsat_ventanas")
        got = {(int(i["satelite"]), i["inicio_ventana"]):
               (int(i["conteo"]), float(i["promedio_nubosidad"])) for i in table.scan()["Items"]}
        assert got == expected_windows(records)
        print(f"Ventanas simuladas: {n_windows}; filas en landsat_ventanas: {len(got)}; "
              "coinciden con el cálculo directo")

        import contar
        sys.argv = ["contar.py", "--bucket", BUCKET]
        out = io.StringIO()
        with redirect_stdout(out):
            contar.main()
        print(out.getvalue())
        assert "-> CUADRA" in out.getvalue()

        import limpiar
        sys.argv = ["limpiar.py", "--bucket", BUCKET, "--si"]
        limpiar.main()
        client = boto3.client("dynamodb")
        left = {t: client.scan(TableName=t, Select="COUNT")["Count"] for t in limpiar.TABLES}
        assert not any(left.values()), left
        assert "Contents" not in boto3.client("s3").list_objects_v2(Bucket=BUCKET, Prefix="crudos/")
        print("limpiar.py dejó las tablas y crudos/ vacíos")


if __name__ == "__main__":
    main()
