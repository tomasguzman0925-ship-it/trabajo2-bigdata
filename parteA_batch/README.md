# Parte A: pipeline batch con Glue, Step Functions y EventBridge Scheduler

_Esta carpeta la completa quien hizo la parte A. Abajo está la estructura sugerida, igual a la de la parte B._

- [Resumen](#resumen)
- [Paso a paso](#paso-a-paso)
- [Resultados](#resultados)
- [Borrar todo](#borrar-todo)

---

## Resumen

_completar:_

- Datos de partida: qué son, de dónde vienen, cuántos registros tienen (al menos 5.000.000) y el segundo conjunto de datos con el que se unen.
- La máquina de estados `sira-pipeline-batch` (región `us-east-2`):
  - el trabajo de Glue inicial `sira-limpieza-y-union`, con la limpieza, el join y la columna de partición, y por qué se eligió esa columna;
  - las dos ramas en paralelo, `RamaEstresHidrico` y `RamaViabilidadSatelital`, con la pregunta de analítica que responde cada una.
- El horario de EventBridge Scheduler que la inicia.

Archivos sugeridos:

| Archivo | Contenido |
|---|---|
| `glue/sira_limpieza_y_union.py` | Script de PySpark del trabajo inicial |
| `glue/rama_estres_hidrico.py` | Script de la primera rama |
| `glue/rama_viabilidad_satelital.py` | Script de la segunda rama |
| `stepfunctions/definicion.json` | Definición de la máquina de estados (botón **Copiar** de la vista *Código* en la consola) |
| `queries/consultas.sql` | Consultas de Athena de las dos agrupaciones |

---

## Paso a paso

_completar: cada recurso creado en la consola, en orden (bucket, datos subidos, rol de Glue, los tres trabajos de Glue, la máquina de estados, el horario, las tablas de Athena), con lo que se debe ver después de cada paso._

---

## Resultados

_completar: cantidad de registros de partida y después de la limpieza, la ejecución iniciada por el horario y terminada con éxito, y la respuesta de cada consulta._

---

## Borrar todo

_completar: el horario, la máquina de estados, los trabajos de Glue, las tablas y la base de datos de Glue, el bucket (vaciarlo primero) y los roles de IAM._
