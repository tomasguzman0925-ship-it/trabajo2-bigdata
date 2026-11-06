# Trabajo 2: pipeline batch y streaming en AWS

**Proyecto integrador:** SIRA, Sistema Inteligente de Riego para Arroz en el Alto Magdalena.

Curso SI7006/SI6003 Almacenamiento y Procesamiento de Grandes Datos, Universidad EAFIT, 2026-2. Profesor Edwin Montoya.

**Equipo 10:**

- Cristian Miguel Gómez Salazar (cmgomezs@eafit.edu.co)
- Tomás Guzmán Tapias (tguzmant@eafit.edu.co)
- Alejandro Jiménez Lopera (ajimenezl3@eafit.edu.co)
- Santiago Vélez Casallas (svelezc@eafit.edu.co)

**Video (YouTube):** _completar_

## Contenido

| Carpeta | Parte | Servicios | Cuenta y región |
|---|---|---|---|
| [`parteA_batch/`](parteA_batch/README.md) | A: pipeline batch | AWS Glue, Step Functions, EventBridge Scheduler, S3, Athena | `929570730700`, `us-east-2` |
| [`parteB_streaming/`](parteB_streaming/README.md) | B: streaming | Kinesis Data Streams, Lambda, DynamoDB, Data Firehose, S3 | `531640467230`, `us-east-1` |

Cada carpeta tiene su README con lo necesario para reproducirla, los resultados y, en la parte B, cómo borrar lo que se creó.

```text
.
├── README.md
├── parteA_batch/
│   ├── README.md
│   ├── scripts/generar_datos_sira.py        # Genera los 5.000.000 de registros y el maestro de parcelas
│   ├── glue_jobs/                           # Los tres trabajos de Glue en PySpark
│   ├── stepfunctions/                       # Definición de la máquina de estados
│   └── athena/                              # Tablas y consultas de las dos agrupaciones
└── parteB_streaming/
    ├── README.md
    ├── productor/                           # Productor y las 10.000 escenas que publica
    ├── lambda/                              # Función de las ventanas de un minuto
    ├── iam/                                 # Permisos de la Lambda sobre DynamoDB
    ├── scripts/                             # Conteos y limpieza después del ensayo
    └── pruebas/                             # Simulación local sin AWS
```

## Parte A: pipeline batch

Un horario de EventBridge Scheduler inicia la máquina de estados `sira-pipeline-batch`. Primero corre el trabajo de Glue `sira-limpieza-y-union`, que limpia 5.000.000 de observaciones, las une con el maestro de parcelas y las escribe en Parquet particionado por año. Cuando termina con éxito corren en paralelo `sira-agg-estres-hidrico` y `sira-agg-viabilidad-satelital`, cada uno con una agrupación que responde una pregunta distinta del proyecto, y se consultan con Athena.

El detalle está en [`parteA_batch/README.md`](parteA_batch/README.md).

## Parte B: streaming

Un productor publica escenas de Landsat 8 y 9 en un stream de Kinesis durante 6 minutos, con repetidos e inválidos a propósito. Una función Lambda con ventanas de un minuto calcula, por satélite, el conteo de escenas y su nubosidad promedio, sin contar los repetidos; aparta los inválidos con su motivo y guarda los resultados en DynamoDB. Firehose, un segundo consumidor del mismo stream, guarda en S3 todos los eventos tal como llegan.

En la corrida final los conteos cuadraron: **7.200 publicados = 6.524 únicos + 328 repetidos + 348 inválidos**, con 7.200 eventos crudos en S3. Landsat 9 entregó las escenas más despejadas, con 39,68 % de nubosidad promedio frente a 40,89 % de Landsat 8.

El detalle está en [`parteB_streaming/README.md`](parteB_streaming/README.md).

## Recursos creados a mano

Todos los recursos de AWS se crearon manualmente en la consola, sin infraestructura como código. En el video se muestra la consola de AWS CloudFormation sin ningún stack en la región de trabajo de cada parte: `us-east-2` en la cuenta de la parte A y `us-east-1` en la cuenta de la parte B.

## Declaración de uso de IA

- **Parte A:** se usó Antigravity AI Assistant (Google DeepMind) en el generador de datos sintéticos, la depuración de los scripts de PySpark y la estructura de la máquina de estados. El detalle está en la sección 8 de [`parteA_batch/README.md`](parteA_batch/README.md).
- **Parte B:** se hizo con apoyo de un asistente de IA generativa, que generó el código, la primera versión de la guía y el guion de esa parte del video, e hizo los cortes de la grabación. El detalle está en la sección *Uso de IA* de [`parteB_streaming/README.md`](parteB_streaming/README.md).
- **Este README:** lo redactó el asistente de IA generativa de la parte B, a partir de los informes de las dos partes.
- **Video de la parte A:** _completar: qué partes del guion o la edición se hicieron con IA, si aplica._

Cada integrante revisó y entiende lo que generó la IA, y responde por ello en la sustentación.
