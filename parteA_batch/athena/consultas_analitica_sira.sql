-- ==============================================================================
-- consultas_analitica_sira.sql
-- Consultas analíticas en Amazon Athena para el Reto 2 (Parte A)
-- Proyecto Integrador SIRA: Sistema Inteligente de Riego para Arroz
-- ==============================================================================

-- 1. Crear base de datos en Athena / Glue Data Catalog
CREATE DATABASE IF NOT EXISTS sira_db;

-- 2. Tabla externa para la Rama 1: Resumen de Estrés Hídrico y Riego
-- (Apunta a la salida Parquet de sira-agg-estres-hidrico)
CREATE EXTERNAL TABLE IF NOT EXISTS sira_db.sira_oro_estres_hidrico (
    distrito_riego                  string,
    municipio                       string,
    tecnica_riego                   string,
    anio                            int,
    mes                             int,
    total_observaciones_aptas       bigint,
    ndmi_promedio                   double,
    ndwi_promedio                   double,
    ndvi_promedio                   double,
    temp_superficial_c_promedio     double,
    obs_en_estres_hidrico           bigint,
    pct_estres_hidrico              double
)
STORED AS PARQUET
LOCATION 's3://trabajo2-sira-bigdata-<ACCOUNT_ID>/gold/estres_hidrico_distrito_mes/';

-- 3. Tabla externa para la Rama 2: Resumen de Viabilidad Satelital y Nubosidad
-- (Apunta a la salida Parquet de sira-agg-viabilidad-satelital)
CREATE EXTERNAL TABLE IF NOT EXISTS sira_db.sira_oro_viabilidad_satelital (
    departamento                    string,
    municipio                       string,
    satelite                        string,
    mes                             int,
    total_tomas_programadas         bigint,
    nubosidad_promedio_pct          double,
    tomas_aptas_riego               bigint,
    tomas_descartadas_nubes         bigint,
    pct_viabilidad_optica           double
)
STORED AS PARQUET
LOCATION 's3://trabajo2-sira-bigdata-<ACCOUNT_ID>/gold/viabilidad_satelital_municipio/';


-- ==============================================================================
-- CONSULTAS DE NEGOCIO PARA MOSTRAR EN EL VIDEO Y EN EL INFORME
-- ==============================================================================

-- CONSULTA 1 (Responde a la Rama 1):
-- Comparación del nivel de hidratación (NDMI) y porcentaje de estrés hídrico
-- entre el manejo tecnificado (AWD+MIRI) y la cascada tradicional (CD)
-- en los distritos de UsoCoello y UsoSaldaña durante la temporada seca (Enero).
SELECT 
    distrito_riego,
    tecnica_riego,
    anio,
    mes,
    total_observaciones_aptas,
    ndmi_promedio,
    ndwi_promedio,
    temp_superficial_c_promedio,
    pct_estres_hidrico
FROM sira_db.sira_oro_estres_hidrico
WHERE mes = 1 AND anio = 2024
ORDER BY distrito_riego, ndmi_promedio DESC;

-- Hallazgo esperado:
-- Las parcelas con AWD+MIRI mantienen un NDMI balanceado y temperatura superficial
-- estable optimizando el consumo de agua, mientras que el riego tradicional en cascada (CD)
-- presenta fluctuaciones y mayor vulnerabilidad a estrés en periodos de bajo caudal.


-- CONSULTA 2 (Responde a la Rama 2):
-- Evaluación de viabilidad de Landsat 8 y 9 por mes en El Espinal (sede Nataima / UsoCoello).
-- ¿En qué meses la persistencia de nubes compromete el seguimiento satelital óptico?
SELECT 
    satelite,
    mes,
    total_tomas_programadas,
    nubosidad_promedio_pct,
    tomas_aptas_riego,
    pct_viabilidad_optica
FROM sira_db.sira_oro_viabilidad_satelital
WHERE municipio = 'El Espinal'
ORDER BY mes, satelite;

-- Hallazgo esperado:
-- En los meses secos (Diciembre a Febrero y Julio a Agosto) la viabilidad óptica supera el 75-80%.
-- En los meses de mayor pluviosidad (Abril a Mayo y Octubre a Noviembre) la viabilidad óptica
-- cae por debajo del 30%, justificando en SIRA la necesidad de integrar radar SAR (Sentinel-1)
-- o estaciones agrometeorológicas del IDEAM.
