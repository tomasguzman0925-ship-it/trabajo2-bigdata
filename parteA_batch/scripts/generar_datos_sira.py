#!/usr/bin/env python3
"""
generar_datos_sira.py
---------------------
Generador de datos sintéticos/aumentados para el Reto 2 (Big Data) en el marco
del Proyecto Integrador SIRA (Sistema Inteligente de Riego para Arroz en el Alto Magdalena).

Genera:
1. sira_maestro_parcelas_distritos.csv : Inventario maestro de parcelas en los distritos
   de riego de UsoCoello, UsoSaldaña, Campoalegre y el C.I. Nataima (AGROSAVIA).
2. sira_observaciones_satelitales_landsat.csv.gz : 5.000.000 de observaciones de monitoreo
   satelital Landsat 8/9 Collection 2 Level 2 sobre las zonas arroceras (2018-2026),
   con reflectancias, bandas térmicas, QA_PIXEL, nubosidad e imperfecciones realistas
   para el proceso de limpieza en Apache Spark.

Uso:
  python generar_datos_sira.py --filas 5000000 --output ../data/raw
  python generar_datos_sira.py --filas 50000   --output ../data/muestra
"""

import argparse
import csv
import gzip
import os
import random
from datetime import datetime, timedelta

# Semilla fija para reproducibilidad
random.seed(42)

DISTRICTS = [
    {
        "distrito": "UsoCoello",
        "municipio": "El Espinal",
        "depto": "Tolima",
        "path": 8,
        "row": 57,
        "lat_base": 4.15,
        "lon_base": -74.88,
        "suelo": "Franco-arcilloso",
        "fc": 0.34,
        "pmp": 0.18,
    },
    {
        "distrito": "UsoCoello",
        "municipio": "Guamo",
        "depto": "Tolima",
        "path": 8,
        "row": 57,
        "lat_base": 4.03,
        "lon_base": -74.97,
        "suelo": "Franco-limoso",
        "fc": 0.32,
        "pmp": 0.16,
    },
    {
        "distrito": "UsoSaldaña",
        "municipio": "Saldaña",
        "depto": "Tolima",
        "path": 8,
        "row": 58,
        "lat_base": 3.92,
        "lon_base": -74.98,
        "suelo": "Franco-arcilloso",
        "fc": 0.35,
        "pmp": 0.19,
    },
    {
        "distrito": "UsoSaldaña",
        "municipio": "Purificación",
        "depto": "Tolima",
        "path": 8,
        "row": 58,
        "lat_base": 3.86,
        "lon_base": -74.93,
        "suelo": "Franco-arcilloso",
        "fc": 0.33,
        "pmp": 0.17,
    },
    {
        "distrito": "C.I. Nataima (AGROSAVIA)",
        "municipio": "El Espinal",
        "depto": "Tolima",
        "path": 8,
        "row": 57,
        "lat_base": 4.191,
        "lon_base": -74.961,
        "suelo": "Franco-arcilloso",
        "fc": 0.36,
        "pmp": 0.18,
    },
    {
        "distrito": "Distrito Campoalegre",
        "municipio": "Campoalegre",
        "depto": "Huila",
        "path": 8,
        "row": 59,
        "lat_base": 2.68,
        "lon_base": -75.32,
        "suelo": "Franco-limoso",
        "fc": 0.31,
        "pmp": 0.15,
    },
]

TECNICAS_RIEGO = ["AWD+MIRI", "MIRI", "Cascada_CD"]
VARIEDADES = ["Fedearroz 67", "Fedearroz 2000", "Fedearroz 4"]


def generar_maestro_parcelas(num_parcelas=1000, output_dir="."):
    """Genera el dataset 2 (Maestro de Parcelas y Distritos de Riego)."""
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, "sira_maestro_parcelas_distritos.csv")

    headers = [
        "parcela_id",
        "distrito_riego",
        "municipio",
        "departamento",
        "wrs_path",
        "wrs_row",
        "latitud_centro",
        "longitud_centro",
        "tecnica_riego",
        "variedad_arroz",
        "tipo_suelo",
        "capacidad_campo_fc",
        "punto_marchitez_pmp",
        "area_hectareas",
    ]

    parcelas = []
    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for i in range(1, num_parcelas + 1):
            dist_info = random.choice(DISTRICTS)
            p_id = f"PAR-{dist_info['distrito'][:3].upper()}-{i:05d}"
            lat = round(dist_info["lat_base"] + random.uniform(-0.04, 0.04), 6)
            lon = round(dist_info["lon_base"] + random.uniform(-0.04, 0.04), 6)
            tecnica = random.choices(TECNICAS_RIEGO, weights=[0.45, 0.30, 0.25])[0]
            variedad = random.choice(VARIEDADES)
            area = round(random.uniform(2.5, 25.0), 2)

            row = [
                p_id,
                dist_info["distrito"],
                dist_info["municipio"],
                dist_info["depto"],
                dist_info["path"],
                dist_info["row"],
                lat,
                lon,
                tecnica,
                variedad,
                dist_info["suelo"],
                dist_info["fc"],
                dist_info["pmp"],
                area,
            ]
            writer.writerow(row)
            parcelas.append((p_id, dist_info["path"], dist_info["row"], tecnica))

    print(f"[OK] Generado maestro de {num_parcelas} parcelas en: {filename}")
    return parcelas


def generar_observaciones_landsat(parcelas, num_filas=5000000, output_dir="."):
    """Genera el dataset 1 (5M Observaciones Satelitales Landsat en CSV comprimido)."""
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, "sira_observaciones_satelitales_landsat.csv.gz")

    headers = [
        "observacion_id",
        "parcela_id",
        "escena_id",
        "fecha",
        "satelite",
        "wrs_path",
        "wrs_row",
        "sr_b3_verde",
        "sr_b4_rojo",
        "sr_b5_nir",
        "sr_b6_swir1",
        "st_b10_termica",
        "qa_pixel",
        "qa_radsat",
        "sr_qa_aerosol",
        "nubes_escena_pct",
        "calidad_imagen",
    ]

    fecha_inicio = datetime(2018, 1, 1)
    dias_totales = (datetime(2026, 9, 30) - fecha_inicio).days

    print(f"Generando {num_filas:,} observaciones satelitales en {filename}...")

    with gzip.open(filename, mode="wt", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        # Generar por lotes para velocidad y bajo consumo de memoria
        batch_size = 50000
        total_escritos = 0

        while total_escritos < num_filas:
            current_batch = min(batch_size, num_filas - total_escritos)
            rows = []

            for _ in range(current_batch):
                total_escritos += 1
                obs_id = f"OBS-LND-{total_escritos:08d}"

                # Parcela asignada
                parcela_info = random.choice(parcelas)
                p_id, path, row, tecnica = parcela_info

                # Fecha de pasada satelital (Landsat 8 o 9)
                d_offset = random.randint(0, dias_totales)
                f_date = fecha_inicio + timedelta(days=d_offset)
                fecha_str = f_date.strftime("%Y-%m-%d")

                # Landsat 8 (2013+) o Landsat 9 (lanzado finales 2021)
                if f_date.year >= 2022 and random.random() > 0.45:
                    satelite = "Landsat 9"
                    prefix = "LC09"
                else:
                    satelite = "Landsat 8"
                    prefix = "LC08"

                scene_id = f"{prefix}_L2SP_{path:03d}{row:03d}_{f_date.strftime('%Y%m%d')}_02_T1"

                # Nubosidad (en temporadas secas dic-feb y jul-ago hay menos nubes)
                mes = f_date.month
                if mes in [12, 1, 2, 7, 8]:
                    nubes = round(random.betavariate(1.5, 5.0) * 100, 2)
                else:
                    nubes = round(random.betavariate(2.5, 2.5) * 100, 2)

                # Generar reflectancias realistas de arroz según técnica de riego
                # B3 (Verde): 0.04 - 0.12
                # B4 (Rojo): 0.02 - 0.10
                # B5 (NIR): 0.25 - 0.55 (alto por dosel vegetal)
                # B6 (SWIR1): sensible a agua (más bajo si está bien regado)
                es_estres = (tecnica == "Cascada_CD" and random.random() < 0.25) or (random.random() < 0.08)

                if es_estres:
                    # En estrés hídrico: NIR baja, SWIR1 sube (NDMI cae)
                    b3 = round(random.uniform(0.06, 0.13), 4)
                    b4 = round(random.uniform(0.05, 0.12), 4)
                    b5 = round(random.uniform(0.20, 0.35), 4)
                    b6 = round(random.uniform(0.18, 0.30), 4)
                    lst_base = round(random.uniform(31.0, 38.0), 2)  # Mayor temperatura
                else:
                    # Buen estado hídrico: NIR alto, SWIR1 bajo
                    b3 = round(random.uniform(0.04, 0.09), 4)
                    b4 = round(random.uniform(0.03, 0.07), 4)
                    b5 = round(random.uniform(0.38, 0.52), 4)
                    b6 = round(random.uniform(0.10, 0.18), 4)
                    lst_base = round(random.uniform(24.0, 30.5), 2)

                # Convertir LST a DN entero térmico (según escala Landsat Level-2)
                # LST = DN * 0.00341802 + 149 - 273.15  => DN = (LST + 124.15) / 0.00341802
                st_b10_dn = int((lst_base + 124.15) / 0.00341802)

                # QA_PIXEL: Bitmask de calidad
                # 21824 = Clear land; 21826 = Clear water (lámina de arroz)
                # 22280 = Cloud / Cloud Shadow
                rand_qa = random.random()
                if rand_qa < 0.05 or nubes > 75:
                    qa_pixel = 22280  # Con nubes / sombra
                elif rand_qa < 0.20:
                    qa_pixel = 21826  # Agua superficial presente
                else:
                    qa_pixel = 21824  # Suelo / cultivo despejado

                qa_radsat = 0
                sr_qa_aerosol = 64  # Calidad media/buena
                calidad_img = 9

                # Inyección de pequeñas anomalías para evaluar el script de limpieza (Glue)
                anom_rand = random.random()
                if anom_rand < 0.005:
                    b5 = -0.05  # Reflectancia inválida (<0)
                elif anom_rand < 0.010:
                    b6 = 1.45   # Reflectancia inválida (>1)
                elif anom_rand < 0.015:
                    st_b10_dn = ""  # Valor nulo en temperatura
                elif anom_rand < 0.018:
                    calidad_img = 7 # Calidad menor a 9

                rows.append([
                    obs_id,
                    p_id,
                    scene_id,
                    fecha_str,
                    satelite,
                    path,
                    row,
                    b3,
                    b4,
                    b5,
                    b6,
                    st_b10_dn,
                    qa_pixel,
                    qa_radsat,
                    sr_qa_aerosol,
                    nubes,
                    calidad_img,
                ])

            writer.writerows(rows)
            if total_escritos % 500000 == 0 or total_escritos == num_filas:
                print(f"  -> Progreso: {total_escritos:,} / {num_filas:,} filas escritas...")

    print(f"[OK] Archivo de observaciones generado con éxito: {filename}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generador de datos para el Reto 2 - SIRA")
    parser.add_argument("--filas", type=int, default=5000000, help="Número de observaciones a generar (default: 5.000.000)")
    parser.add_argument("--output", type=str, default="../data/raw", help="Directorio de salida")
    parser.add_argument("--parcelas", type=int, default=1200, help="Número de parcelas en el catálogo maestro")
    args = parser.parse_args()

    # Rutas relativas al script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    out_dir = os.path.normpath(os.path.join(script_dir, args.output))

    print(f"=== SIRA DATASET GENERATOR ===")
    print(f"Destino: {out_dir}")
    print(f"Filas objetivo: {args.filas:,}")

    lista_parcelas = generar_maestro_parcelas(num_parcelas=args.parcelas, output_dir=out_dir)
    generar_observaciones_landsat(lista_parcelas, num_filas=args.filas, output_dir=out_dir)
    print("=== PROCESO COMPLETADO EXITOSAMENTE ===")
