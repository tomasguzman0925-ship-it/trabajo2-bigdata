# Parte B: streaming con Kinesis, Lambda y DynamoDB

Esta parte usa escenas de los satélites Landsat 8 y 9, los mismos de los que dependen los índices de SIRA, el proyecto integrador.

- [Resumen](#resumen)
- [Cómo funciona](#cómo-funciona)
- [Paso a paso](#paso-a-paso)
  - [Antes de empezar](#antes-de-empezar)
  - [Bloque A: lo que no cobra por hora](#bloque-a-lo-que-no-cobra-por-hora)
  - [Bloque B: el stream y sus dos consumidores](#bloque-b-el-stream-y-sus-dos-consumidores)
  - [Bloque C: ensayo, corrida final y conteos](#bloque-c-ensayo-corrida-final-y-conteos)
- [Resultados de la corrida final](#resultados-de-la-corrida-final)
- [Prueba local sin AWS](#prueba-local-sin-aws)
- [Borrar todo](#borrar-todo)
- [Uso de IA](#uso-de-ia)

---

## Resumen

Un productor publica escenas Landsat 8-9 en un stream de **Kinesis Data Streams** durante 6 minutos, con repetidos e inválidos a propósito. Dos consumidores leen el mismo stream:

```text
productor.py (CloudShell)
   │  PutRecords, 20 eventos por segundo durante 6 minutos
   ▼
Kinesis Data Stream  landsat-escenas  (1 shard)
   ├──► consumidor 1: Lambda landsat-ventanas  (ventana tumbling de 60 s)
   │        ├─ inválido  ──► DynamoDB landsat_invalidos   (con su motivo)
   │        ├─ repetido  ──► DynamoDB landsat_repetidos
   │        └─ único     ──► DynamoDB landsat_vistos  +  suma a la ventana
   │                          └─ al cerrar la ventana ──► DynamoDB landsat_ventanas
   └──► consumidor 2: Firehose landsat-crudos ──► S3  crudos/  (todos los eventos tal como llegan)
```

| Requisito del enunciado | Dónde se cumple |
|---|---|
| 1. Productor continuo ≥ 5 min, con repetidos e inválidos | `productor/productor.py` (6 min, 5 % repetidos, 5 % inválidos) |
| 2. Ventanas de 1 min: conteo y promedio por categoría, sin contar repetidos | `lambda/lambda_function.py` (categoría: satélite 8 o 9; campo: `land_cloud_cover`) |
| 3. Inválidos apartados con su motivo | tabla `landsat_invalidos` |
| 4. Resultado de cada ventana en DynamoDB y una consulta | tabla `landsat_ventanas` y la Query por satélite (consola y `scripts/contar.py`) |
| 5. Segundo consumidor que guarda los crudos en S3 | Firehose `landsat-crudos` |
| 6. Conteos que cuadran | `scripts/contar.py` |

| Archivo | Qué hace |
|---|---|
| `productor/productor.py` | Publica las escenas en el stream con `PutRecords`, con repetidos e inválidos a propósito, y escribe `publicados.json` con lo que publicó |
| `productor/escenas_semilla.csv` | 10.000 escenas reales de 2026 del catálogo Landsat 8-9 (las mismas del trabajo 1) |
| `lambda/lambda_function.py` | La función Lambda: valida, descarta repetidos y calcula las ventanas de un minuto |
| `iam/politica_dynamodb.json` | La política en línea que deja a la Lambda leer y escribir en las cuatro tablas |
| `scripts/contar.py` | Comprueba que los conteos cuadran y responde la pregunta con una Query por satélite |
| `scripts/limpiar.py` | Vacía las tablas y `crudos/` después del ensayo |
| `pruebas/simulacion_local.py` | Simula todo el flujo sin AWS, para probar la lógica antes de crear los recursos |

**Evento** (una escena real de 2026, de `escenas_semilla.csv`):

```json
{"event_id": "0f0ba028-...", "scene_id": "LC09_L2SP_194055_20260202_20260203_02_T1", "satellite": 9,
 "wrs_path": 194, "wrs_row": 55, "date_acquired": "2026-02-02", "land_cloud_cover": 29.3,
 "station": "LGN", "sent_at_ms": 1793708570029}
```

**Pregunta del proyecto:** ¿qué satélite, Landsat 8 o Landsat 9, entregó escenas más despejadas (menor nubosidad promedio), y en qué minuto tuvo cada uno su mejor ventana?

---

## Cómo funciona

**Stream y no cola.** En el trabajo 1, SQS entregaba cada mensaje a un solo consumidor y lo borraba al procesarlo. Kinesis guarda los registros (24 horas por defecto) y cada consumidor los lee todos por su cuenta, como si cada uno tuviera su propio marcador de lectura. Por eso la Lambda y Firehose reciben los mismos eventos.

**Shard, partition key y sequence number.** Un shard es una "fila" ordenada del stream. La partition key de cada registro decide a qué shard va; usamos el `event_id`. Kinesis le pone a cada registro un **sequence number** único dentro del shard. Usamos **un solo shard** porque Lambda calcula la ventana por shard: con un shard hay un único resultado por minuto y por satélite.

**Ventana tumbling de 60 s.** Son ventanas de un minuto que no se solapan (10:00–10:01, 10:01–10:02, …). Lambda invoca la función varias veces dentro de cada minuto, una por lote de registros, y le pasa en `event["state"]` lo acumulado (conteo y suma de nubosidad por satélite). La función devuelve el estado nuevo. En la última invocación de la ventana (`isFinalInvokeForWindow = true`) escribe el conteo y el promedio en `landsat_ventanas`. La ventana se define por la hora en que el registro **llegó al stream**, no por el `sent_at_ms` del evento. Si deja de llegar tráfico, Lambda espera hasta 2 minutos antes de cerrar la última ventana.

**Cada registro pasa por dos filtros, en orden:**

1. **¿Es válido?** Se revisa con las mismas reglas del trabajo 1 (campos presentes, satélite 8 o 9, `wrs_path` 1–233, `wrs_row` 1–248, nubosidad 0–100). Si no, va a `landsat_invalidos` con el motivo.
2. **¿Es repetido?** Se intenta guardar su `event_id` en `landsat_vistos` con la condición `attribute_not_exists(event_id)`, es decir, "solo si no existe". Si ya existía, el evento es repetido y va a `landsat_repetidos`.

Si pasa los dos filtros es **único** y suma a su ventana.

**Por qué un reintento no se cuenta como repetido.** Lambda procesa cada registro *al menos una vez*: si una invocación falla, vuelve a procesar el lote. En `landsat_vistos` guardamos también el sequence number del registro que trajo el `event_id` por primera vez. Si el `event_id` ya está, pero con **el mismo** sequence number, es un reintento del mismo registro y se cuenta normal. Si está con **otro** sequence number, es un repetido de verdad.

**Por qué cuadran los conteos.** Cada registro termina en exactamente una de tres tablas (`landsat_vistos`, `landsat_repetidos` o `landsat_invalidos`), y la llave de cada una es única: `event_id` en la primera y el sequence number en las otras dos. Por eso publicados = únicos + repetidos + inválidos. Además, la suma de los conteos de todas las ventanas es igual a los únicos, y los crudos de S3 son igual a los publicados.

**Por qué Lambda y no Flink.** El enunciado sugiere Managed Service for Apache Flink, pero permite Lambda. Lambda tiene ventanas tumbling nativas para Kinesis, no cobra mientras no procesa y ya la usamos en el trabajo 1. Flink cobra por hora mientras la aplicación está corriendo.

---

## Paso a paso

### Antes de empezar

- **Cuenta:** `531640467230`. Si otra persona reproduce el trabajo, cambia ese número por el de su cuenta en el nombre del bucket.
- **Región:** **US East (N. Virginia) `us-east-1`**. Revísala arriba a la derecha de la consola **antes de cada paso**: si un recurso se crea en otra región, los demás no lo encuentran.
- **Plan pago:** Kinesis y Firehose no funcionan en el plan gratuito. Esta cuenta ya está en el plan pago.
- **Archivos:** los de esta carpeta, `trabajo2-bigdata/parteB_streaming/`.
- **Tiempo:**
  - Bloque A: unos 25 minutos, sin apuro.
  - Bloques B y C: unos 40 minutos, **de corrido**.
- **Costo:** lo único que cobra por hora es el stream de Kinesis (US$0,015 por hora). Por eso se crea en el bloque B, justo antes de usarlo, y se borra después de grabar. Todo junto cuesta unos US$0,10–0,15.

### Bloque A: lo que no cobra por hora

#### Paso 1: El bucket de S3

*Para qué:* aquí Firehose guarda todos los eventos tal como llegan.

1. En la barra de búsqueda de arriba escribe **S3** y entra.
2. Clic en **Create bucket**.
3. **Bucket type:** General purpose (viene marcado).
4. **Bucket name:** `landsat-trabajo2-531640467230`
5. No cambies nada más. Baja hasta el final y haz clic en **Create bucket**.

✅ **Debes ver:** el bucket en la lista de buckets, vacío.

#### Paso 2: Las cuatro tablas de DynamoDB

*Para qué:*
- `landsat_ventanas` guarda el resultado de cada minuto.
- `landsat_vistos` guarda los `event_id` que ya llegaron, para reconocer los repetidos.
- `landsat_repetidos` y `landsat_invalidos` guardan los eventos descartados.

1. Busca **DynamoDB** y entra. En el menú de la izquierda, **Tables** → **Create table**.
2. **Table name**, **Partition key** y **Sort key**: según la tabla de abajo. Al lado de cada llave hay un selector de tipo (String / Number).
3. **Table settings:** elige **Customize settings**.
4. **Read/write capacity settings → Capacity mode:** **On-demand**. Así solo se paga por lo que se escribe, que son centavos.
5. Lo demás como viene. Baja y haz clic en **Create table**.
6. Repite para las cuatro tablas:

| Table name | Partition key | Tipo | Sort key | Tipo |
|---|---|---|---|---|
| `landsat_ventanas` | `satelite` | **Number** | `inicio_ventana` | String |
| `landsat_vistos` | `event_id` | String | (vacío) | |
| `landsat_repetidos` | `seq` | String | (vacío) | |
| `landsat_invalidos` | `seq` | String | (vacío) | |

⚠️ `satelite` va como **Number**. Si queda como String, la Lambda falla al guardar las ventanas.

✅ **Debes ver:** las cuatro tablas en **Tables** con estado **Active**.

#### Paso 3: Crear la función Lambda

*Para qué:* es la aplicación que calcula las ventanas, el consumidor 1 del stream.

1. Busca **Lambda** y entra → **Create function**.
2. Elige **Author from scratch**.
3. **Function name:** `landsat-ventanas`
4. **Runtime:** **Python 3.13**, o el Python más nuevo de la lista.
5. **Architecture:** x86_64.
6. **Permissions:** deja **Create a new role with basic Lambda permissions**, que viene marcado.
7. Clic en **Create function**.

✅ **Debes ver:** la página de la función, con el mensaje verde "Successfully created the function landsat-ventanas".

#### Paso 4: Pegar el código

1. En la función, pestaña **Code**. A la izquierda está el archivo `lambda_function.py` con un código de ejemplo.
2. Selecciona todo el código de ejemplo (Ctrl+A) y bórralo.
3. Abre en tu PC `lambda/lambda_function.py`, cópialo completo (Ctrl+A, Ctrl+C) y pégalo en el editor (Ctrl+V).
4. Clic en **Deploy**.

✅ **Debes ver:** "Successfully updated the function landsat-ventanas".

#### Paso 5: Memoria y tiempo máximo

*Para qué:* por defecto la función tiene 3 segundos para responder, y un lote de 100 eventos con escrituras en DynamoDB puede tardar más.

1. Pestaña **Configuration** → a la izquierda **General configuration** → **Edit**.
2. **Memory:** `256` MB.
3. **Timeout:** `1` min `0` sec.
4. **Save**.

✅ **Debes ver:** Memory 256 MB y Timeout 1 min 0 sec.

#### Paso 6: Permisos de la Lambda

*Para qué:* la función necesita permiso para leer el stream y para escribir en las cuatro tablas.

1. Pestaña **Configuration** → a la izquierda **Permissions**.
2. En **Execution role**, haz clic en el nombre del rol (algo como `landsat-ventanas-role-xxxxxxx`). Se abre IAM en otra pestaña.
3. **Permiso para leer el stream:**
   - Clic en **Add permissions** → **Attach policies**.
   - En el buscador escribe `AWSLambdaKinesisExecutionRole`, marca la casilla y haz clic en **Add permissions**.
4. **Permiso para las tablas:**
   - Clic otra vez en **Add permissions** → **Create inline policy**.
   - En **Policy editor**, cambia de **Visual** a **JSON**.
   - Borra lo que haya y pega el contenido de `iam/politica_dynamodb.json`.
   - **Next** → **Policy name:** `landsat-dynamodb` → **Create policy**.

✅ **Debes ver:** en el rol, en **Permissions policies**, tres políticas: la básica de Lambda (`AWSLambdaBasicExecutionRole-…`), `AWSLambdaKinesisExecutionRole` y `landsat-dynamodb`.

#### Paso 7: CloudShell y una prueba sin AWS

*Para qué:* CloudShell es una terminal dentro de la consola, gratis y con Python y boto3 instalados, que usa tus permisos sin configurar llaves. Desde ahí corre el productor.

1. Abre **CloudShell**: es el ícono de terminal `>_` en la barra de arriba de la consola, junto a la campana. Revisa que diga `us-east-1`. La primera vez tarda un minuto en abrir.
2. Arriba a la derecha de CloudShell: **Actions** → **Upload file**. Sube, uno por uno, estos cuatro archivos:
   - `productor/productor.py`
   - `productor/escenas_semilla.csv`
   - `scripts/contar.py`
   - `scripts/limpiar.py`
3. Comprueba que están y haz una prueba que **no** usa AWS:

```bash
ls
python3 productor.py --prueba
```

✅ **Debes ver:** con `ls`, los cuatro archivos. Con la prueba, algunas líneas que empiezan por `unico`, `invalido` (con su motivo) y `repetido`, y al final `En 3 segundos: {...}`.

**Hasta aquí no se ha cobrado nada.** Si tienes que parar, puedes parar aquí y seguir más tarde.

### Bloque B: el stream y sus dos consumidores

Desde aquí el stream cobra US$0,015 por hora. Haz los bloques B y C de corrido.

#### Paso 8: Crear el stream de Kinesis

1. Busca **Kinesis** y entra → **Data streams** (menú de la izquierda) → **Create data stream**.
2. **Data stream name:** `landsat-escenas`
3. **Capacity mode:** **Provisioned**.
4. **Provisioned shards:** `1`
5. Lo demás como viene → **Create data stream**.

✅ **Debes ver:** después de unos 30 segundos, **Status: Active** y 1 shard abierto.

⚠️ No crees *enhanced fan-out consumers*: cobran aparte y no se necesitan.

#### Paso 9: Firehose, el consumidor 2

*Para qué:* guarda en S3 todos los eventos tal como llegan. **Debe existir antes de correr el productor**, porque empieza a leer desde el momento en que se crea.

1. Busca **Firehose** (Amazon Data Firehose) y entra → **Create Firehose stream**.
2. **Source:** **Amazon Kinesis Data Streams**. **Destination:** **Amazon S3**.
3. **Source settings → Kinesis data stream:** **Browse** → elige `landsat-escenas` → **Choose**.
4. **Firehose stream name:** borra el nombre automático y escribe `landsat-crudos`.
5. **Transform and convert records:** déjalo como viene, sin activar nada.
6. **Destination settings:**
   - **S3 bucket:** **Browse** → `landsat-trabajo2-531640467230` → **Choose**.
   - **New line delimiter** y **Dynamic partitioning:** déjalos desactivados. El productor ya pone un salto de línea al final de cada evento.
   - **S3 bucket prefix:** `crudos/`
   - **S3 bucket error output prefix:** `errores/`
7. Abre **Buffer hints, compression, file extension and encryption**:
   - **Buffer size:** `1` MiB.
   - **Buffer interval:** `60` seconds.
8. **Advanced settings → Service access:** deja **Create or update IAM role**, que viene marcado.
9. **Create Firehose stream**.

✅ **Debes ver:** después de uno o dos minutos, **Status: Active**.

#### Paso 10: Conectar la Lambda al stream (el disparador)

*Para qué:* hace que Lambda lea el stream y llame a la función con ventanas de 60 segundos.

1. Vuelve a **Lambda** → `landsat-ventanas` → en el diagrama de arriba, **+ Add trigger**.
2. **Select a source:** **Kinesis**.
3. **Kinesis stream:** `kinesis/landsat-escenas`.
4. **Consumer:** déjalo vacío. Si eliges uno, sería enhanced fan-out, que cobra aparte.
5. **Activate trigger:** marcado.
6. **Batch size:** `100`.
7. **Starting position:** **Trim horizon**.
8. Abre **Additional settings** y busca **Tumbling window duration**: `60`.
9. **Add**.

✅ **Debes ver:** "The trigger landsat-escenas was successfully added". En **Configuration → Triggers**, el disparador con estado **Enabled** (puede decir *Creating* durante unos segundos).

Si sale un error de permisos, revisa el paso 6: falta `AWSLambdaKinesisExecutionRole`.

### Bloque C: ensayo, corrida final y conteos

#### Paso 11: Ensayo de 1 minuto

*Para qué:* comprobar que todo funciona antes de la corrida que se graba.

En CloudShell:

```bash
BUCKET=landsat-trabajo2-531640467230
python3 productor.py --minutos 1
```

✅ **Debes ver:** `Publicando 20 eventos por segundo en 'landsat-escenas' durante 60 s ...`, una línea a los 30 y a los 60 segundos, y al final un resumen con `"publicados": 1200`.

**Espera 5 minutos.** Lambda cierra la última ventana hasta 2 minutos después del último evento, y Firehose entrega cada 60 segundos. Luego:

```bash
python3 contar.py --bucket $BUCKET
```

✅ **Debes ver:** `-> CUADRA`, `Eventos crudos en S3` igual a los publicados y `Suma de conteos de las ventanas` igual a los únicos. Si la suma de las ventanas todavía es menor, la última ventana no se ha cerrado: espera un minuto y corre `contar.py` otra vez.

**Si algo no cuadra o sale un error,** antes de seguir:
- Mira los logs: Lambda → `landsat-ventanas` → **Monitor** → **View CloudWatch logs** → el log stream más reciente.

Si cuadró y las ventanas suman lo mismo que los únicos, deja las tablas y `crudos/` vacíos para la corrida que se graba. Esto borra los datos, no las tablas. Si limpias antes de que se cierre la última ventana, esa ventana llega después y queda en `landsat_ventanas`; en ese caso espera dos minutos y vuelve a limpiar:

```bash
python3 limpiar.py --bucket $BUCKET --si
```

✅ **Debes ver:** una línea por tabla con los ítems borrados, y los archivos borrados de `crudos/`.

#### Paso 12: La corrida final, grabando

**Empieza a grabar la pantalla antes de este paso.** Esta corrida es la del video y no hace falta repetirla.

```bash
python3 productor.py
```

Mientras corre (6 minutos), muestra y explica:
- La línea de avance que imprime CloudShell cada 30 segundos.
- **Kinesis** → `landsat-escenas` → pestaña **Monitoring**. Las gráficas de *Incoming data* tardan uno o dos minutos en aparecer.
- **S3** → `landsat-trabajo2-531640467230` → `crudos/`, donde van apareciendo carpetas `AAAA/MM/DD/HH/` con archivos.
- **Lambda** → **Monitor** → **View CloudWatch logs**, un log con `"final": true` y el `estado` de la ventana.

✅ **Debes ver al final:** un resumen con `"publicados": 7200`, `"unicos": 6524`, `"repetidos": 328`, `"invalidos": 348` y `"no_publicados": 0`. Son los números de la corrida real, y se repiten porque el productor usa una semilla fija. Si `no_publicados` es mayor que 0, los números cambian un poco, pero igual cuadran.

#### Paso 13: Conteos y la consulta

Espera 5 minutos y:

```bash
python3 contar.py --bucket $BUCKET
```

✅ **Debes ver:**
- `-> CUADRA`.
- `Suma de conteos de las ventanas` igual a los únicos. Si es menor, espera un minuto y corre `contar.py` otra vez. **No** corras `limpiar.py` después de la corrida final.
- Los inválidos por motivo (descartados / enviados, iguales).
- Las ventanas de cada satélite.
- La respuesta a la pregunta.

La misma consulta en la consola, para el video:

1. **DynamoDB** → **Explore items** → `landsat_ventanas`.
2. Elige **Query**. **satelite (Partition key):** `9` → **Run**.
3. Se ven las ventanas de un minuto de Landsat 9, en orden, con `conteo` y `promedio_nubosidad`.
4. Repite con `8`.

Muestra también:
- `landsat_invalidos`, con **Scan** → Run: se ven las columnas `motivo` y `evento`.
- `landsat_repetidos`.

Si CloudShell se reinició y `$BUCKET` quedó vacío, vuelve a escribir `BUCKET=landsat-trabajo2-531640467230`.

---

## Resultados de la corrida final

Corrida del 5 de noviembre de 2026 en `us-east-1`, de 12:07:12 a 12:13:12 UTC.

Salida de `python3 contar.py --bucket landsat-trabajo2-531640467230`:

```text
CONTEOS
  Publicados por el productor ........   7200
  Únicos procesados (landsat_vistos) .   6524   (el productor envió 6524)
  Repetidos descartados ..............    328   (el productor envió 328)
  Inválidos descartados ..............    348   (el productor envió 348)
  Únicos + repetidos + inválidos .....   7200   -> CUADRA
  Eventos crudos en S3 ...............   7200   (7 archivos de Firehose)
  Suma de conteos de las ventanas ....   6524   (debe ser igual a los únicos)

INVÁLIDOS POR MOTIVO (descartados / enviados)
  falta_campo:land_cloud_cover        54 / 54
  falta_campo:scene_id                72 / 72
  nubosidad_fuera_de_rango            75 / 75
  satelite_invalido                   75 / 75
  wrs_path_invalido                   72 / 72

VENTANAS DE UN MINUTO (Query a landsat_ventanas por satélite)
  Landsat 8  2026-11-05T12:07:00Z   381 escenas  nubosidad promedio  41.21 %
  Landsat 8  2026-11-05T12:08:00Z   478 escenas  nubosidad promedio  39.75 %
  Landsat 8  2026-11-05T12:09:00Z   499 escenas  nubosidad promedio  41.20 %
  Landsat 8  2026-11-05T12:10:00Z   503 escenas  nubosidad promedio  41.11 %
  Landsat 8  2026-11-05T12:11:00Z   531 escenas  nubosidad promedio  41.91 %
  Landsat 8  2026-11-05T12:12:00Z   502 escenas  nubosidad promedio  40.80 %
  Landsat 8  2026-11-05T12:13:00Z   100 escenas  nubosidad promedio  37.65 %
  Landsat 9  2026-11-05T12:07:00Z   491 escenas  nubosidad promedio  36.83 %
  Landsat 9  2026-11-05T12:08:00Z   605 escenas  nubosidad promedio  39.11 %
  Landsat 9  2026-11-05T12:09:00Z   578 escenas  nubosidad promedio  41.28 %
  Landsat 9  2026-11-05T12:10:00Z   593 escenas  nubosidad promedio  39.99 %
  Landsat 9  2026-11-05T12:11:00Z   555 escenas  nubosidad promedio  42.62 %
  Landsat 9  2026-11-05T12:12:00Z   598 escenas  nubosidad promedio  37.58 %
  Landsat 9  2026-11-05T12:13:00Z   110 escenas  nubosidad promedio  42.07 %

PREGUNTA: ¿qué satélite entregó escenas más despejadas y en qué minuto?
  Landsat 8: 2994 escenas, nubosidad promedio 40.89 %; su minuto más despejado fue 2026-11-05T12:13:00Z (37.65 %)
  Landsat 9: 3530 escenas, nubosidad promedio 39.68 %; su minuto más despejado fue 2026-11-05T12:07:00Z (36.83 %)
```

**Respuesta:** Landsat 9 entregó las escenas más despejadas, con 39,68 % de nubosidad promedio frente a 40,89 % de Landsat 8. La primera y la última ventana están incompletas, porque la corrida empezó a las 12:07:12 y terminó a las 12:13:12, y la última solo tiene 12 segundos de eventos. El 37,65 % de Landsat 8 sale de esa última ventana, con 100 escenas.

Resumen del productor (`publicados.json`):

```json
{
  "stream": "landsat-escenas",
  "inicio_utc": "2026-11-05T12:07:12+00:00",
  "fin_utc": "2026-11-05T12:13:12+00:00",
  "segundos": 360.0,
  "publicados": 7200,
  "unicos": 6524,
  "repetidos": 328,
  "invalidos": 348,
  "invalidos_por_motivo": {
    "falta_campo:land_cloud_cover": 54,
    "falta_campo:scene_id": 72,
    "nubosidad_fuera_de_rango": 75,
    "satelite_invalido": 75,
    "wrs_path_invalido": 72
  },
  "no_publicados": 0
}
```

---

## Prueba local sin AWS

`pruebas/simulacion_local.py` simula DynamoDB y S3 con la librería `moto` y pasa 6 minutos de eventos del productor por la Lambda: ventana por ventana, en lotes de 100, con un lote que se procesa dos veces para imitar un reintento de Lambda. Comprueba que las ventanas guardadas coinciden con un cálculo directo sobre los eventos únicos, que `contar.py` da `CUADRA` y que `limpiar.py` deja todo vacío. No crea nada en AWS.

```bash
pip install boto3 "moto[dynamodb,s3]==5.1.4"
python3 parteB_streaming/pruebas/simulacion_local.py
```

✅ **Debes ver:** `Ventanas simuladas: 7; filas en landsat_ventanas: 14; coinciden con el cálculo directo`, la salida de `contar.py` con `7200`, `6524`, `328`, `348` y `-> CUADRA`, y al final `limpiar.py dejó las tablas y crudos/ vacíos`.

---

## Borrar todo

Apenas termines de grabar. **Primero el stream**, que es lo único que cobra por hora.

1. **Kinesis** → **Data streams** → marca `landsat-escenas` → **Actions** → **Delete** → escribe `delete` → **Delete**.
2. **Firehose** → marca `landsat-crudos` → **Delete** → confirma.
3. **Lambda** → **Functions** → marca `landsat-ventanas` → **Actions** → **Delete** → confirma.
4. **DynamoDB** → **Tables** → marca las cuatro tablas `landsat_*` → **Delete** → confirma.
5. **S3** → marca `landsat-trabajo2-531640467230` → **Empty** → escribe `permanently delete` → luego **Delete** → escribe el nombre del bucket.
6. **IAM** → **Roles** → borra `landsat-ventanas-role-…` y el rol que creó Firehose (`KinesisFirehoseServiceRole-landsat-cr…`).
7. **CloudWatch** → **Log groups** → borra `/aws/lambda/landsat-ventanas`.

Los pasos 1 a 5 son los que importan para el costo; el 6 y el 7 dejan la cuenta limpia.

✅ **Debes ver:** Kinesis → **Data streams** sin streams.

---

## Uso de IA

Esta parte se hizo con apoyo de un asistente de IA generativa. La IA generó el código (`productor.py`, `lambda_function.py`, `contar.py`, `limpiar.py`, la política IAM y `pruebas/simulacion_local.py`), la primera versión de esta guía y el guion de esta parte del video, a partir del enunciado y del código del trabajo 1, e hizo los cortes de la grabación. El equipo creó todos los recursos a mano en la consola, ejecutó el ensayo y la corrida final, grabó el video, revisó el código y la guía, y explica cada parte.
