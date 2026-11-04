# Trabajo 2: pipeline batch y streaming en AWS

Curso SI7006 Almacenamiento y Procesamiento de Grandes Datos, Universidad EAFIT, 2026-2. Profesor Edwin Montoya.

Integrantes:

- Tomás Guzmán Tapias (tguzmant@eafit.edu.co)
- Cristian Miguel Gómez Salazar (cmgomezs@eafit.edu.co)
- Alejandro Jimenez Lopera (ajimenezl3@eafit.edu.co)

Video (YouTube): _completar_

## Contenido

| Carpeta | Parte | Servicios | Cuenta y región |
|---|---|---|---|
| [`parteA_batch/`](parteA_batch/README.md) | A: pipeline batch | AWS Glue, Step Functions, EventBridge Scheduler, S3, Athena | cuenta de la parte A, `us-east-2` |
| [`parteB_streaming/`](parteB_streaming/README.md) | B: streaming | Kinesis Data Streams, Lambda, DynamoDB, Data Firehose, S3 | cuenta `531640467230`, `us-east-1` |

Cada carpeta tiene su README con el paso a paso para reproducirla, lo que se debe ver en cada paso y cómo borrar lo que se creó.

## Parte A: pipeline batch

Un horario de EventBridge Scheduler inicia la máquina de estados `sira-pipeline-batch`. Primero corre el trabajo de Glue inicial (`sira-limpieza-y-union`), que limpia los datos, los une con un segundo conjunto y los escribe en Parquet particionado. Cuando termina con éxito, corren en paralelo dos trabajos de Glue, `RamaEstresHidrico` y `RamaViabilidadSatelital`, cada uno con una agrupación que responde una pregunta del proyecto. Las agrupaciones se consultan con Athena.

El detalle está en [`parteA_batch/README.md`](parteA_batch/README.md).

## Parte B: streaming

Un productor publica escenas Landsat 8-9 en un stream de Kinesis durante 6 minutos, con repetidos e inválidos a propósito. Una función Lambda con ventanas tumbling de un minuto calcula, por satélite, el conteo de escenas y su nubosidad promedio, sin contar los repetidos. Aparta los inválidos con su motivo y guarda los resultados en DynamoDB. Firehose, un segundo consumidor del mismo stream, guarda en S3 todos los eventos tal como llegan, y un script comprueba que publicados = únicos + repetidos + inválidos.

El detalle está en [`parteB_streaming/README.md`](parteB_streaming/README.md).

## Recursos creados a mano

Todos los recursos de AWS se crearon manualmente en la consola, sin infraestructura como código. En el video se muestra la consola de AWS CloudFormation sin ningún stack en la región de trabajo de cada parte: `us-east-2` en la cuenta de la parte A y `us-east-1` en la cuenta de la parte B.

## Declaración de uso de IA

- **Parte A:** _completar: qué partes se hicieron con IA (código de los trabajos de Glue, definición de la máquina de estados, consultas, textos) y cuáles no._
- **Parte B:** se hizo con apoyo de un asistente de IA generativa. La IA generó el código (`productor.py`, `lambda_function.py`, `contar.py`, `limpiar.py`, la política IAM y la simulación local) y la primera versión de su guía, a partir del enunciado y del código del trabajo 1. El equipo creó todos los recursos a mano en la consola, ejecutó el ensayo y la corrida final, revisó el código y la guía, y explica cada parte.
- **Informe y video:** _completar: qué textos o partes del guion se hicieron con IA._

Cada integrante revisó y entiende lo que generó la IA, y responde por ello en la sustentación.
