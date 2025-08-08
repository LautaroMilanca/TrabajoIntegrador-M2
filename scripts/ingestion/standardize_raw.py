
import os
import re
import json
from pathlib import Path
from datetime import datetime
import pandas as pd

INPUT_PATH = Path("data/raw_data/listings_raw.csv")  
OUTPUT_DIR = Path("data/standardized")               
SOURCE_NAME = "listings"                              
MIN_ROWS = 1                                          


EXPECTED_COLUMNS = [
    "id","name","host_id","host_name","neighbourhood_group","neighbourhood",
    "latitude","longitude","room_type","price","minimum_nights",
    "number_of_reviews","last_review","reviews_per_month",
    "calculated_host_listings_count","availability_365"
]

def snake_case(s: str) -> str:
    s = s.strip()
    s = re.sub(r"[^0-9a-zA-Z]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_").lower()

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [snake_case(c) for c in df.columns]
    return df

def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    numeric_int = [
        "id","host_id","price","minimum_nights",
        "number_of_reviews","calculated_host_listings_count","availability_365"
    ]
    for col in numeric_int:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    numeric_float = ["latitude","longitude","reviews_per_month"]
    for col in numeric_float:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "last_review" in df.columns:
        df["last_review"] = pd.to_datetime(df["last_review"], errors="coerce")

    for col in ["name","host_name","neighbourhood_group","neighbourhood","room_type"]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    return df

def validate(df: pd.DataFrame):
    if df is None or len(df) < MIN_ROWS:
        raise ValueError("El DataFrame está vacío o por debajo del mínimo esperado.")
    # Validaciones simples
    required = ["id","neighbourhood_group","neighbourhood","room_type","price"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}")

def save_outputs(df: pd.DataFrame, outdir: Path, source: str):
    outdir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")  # UTC para consistencia
    base = f"{source}_{stamp}"

    # CSV (UTF-8, separador coma)
    csv_path = outdir / f"{base}.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8")
    
    # JSON lineal (records), UTF-8
    json_path = outdir / f"{base}.json"
    df.to_json(json_path, orient="records", force_ascii=False)

    manifest = {
        "source": source,
        "created_at_utc": stamp,
        "rows": int(len(df)),
        "columns": df.columns.tolist(),
        "csv_path": str(csv_path),
        "json_path": str(json_path),
        "encoding": "utf-8",
        "delimiter": ","
    }
    manifest_path = outdir / f"{base}.manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print(f"Archivos generados:\n  - {csv_path}\n  - {json_path}\n  - {manifest_path}")

def main():
    try:
        print(f"Leyendo archivo: {INPUT_PATH}")
        if not INPUT_PATH.exists():
            raise FileNotFoundError(f"No existe el archivo de entrada: {INPUT_PATH}")

        df = pd.read_csv(INPUT_PATH, encoding="utf-8")  # se asume utf-8
        df = normalize_headers(df)
        df = coerce_types(df)
        validate(df)

        save_outputs(df, OUTPUT_DIR, SOURCE_NAME)
        print("Estandarización completada correctamente.")
    except Exception as e:
        print(f"ERROR durante la estandarización: {e}")
        raise

if __name__ == "__main__":
    main()
