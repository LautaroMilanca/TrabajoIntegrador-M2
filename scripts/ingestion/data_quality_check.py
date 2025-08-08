
import os
import json
import glob
from pathlib import Path
from datetime import datetime, date
import pandas as pd

STANDARD_DIR = Path("data/standardized")
QUALITY_OUT_DIR = Path("data/quality")
SOURCE_NAME = "listings"

# Columnas esperadas y tipos
EXPECTED_COLUMNS = [
    "id","name","host_id","host_name","neighbourhood_group","neighbourhood",
    "latitude","longitude","room_type","price","minimum_nights",
    "number_of_reviews","last_review","reviews_per_month",
    "calculated_host_listings_count","availability_365"
]

# Reglas de calidad (umbral de nulos permitido por columna, en %)
NULL_THRESHOLDS = {
    # 0% nulos permitidos
    "id": 0, "host_id": 0, "neighbourhood_group": 0, "neighbourhood": 0,
    "latitude": 0, "longitude": 0, "room_type": 0, "price": 0, "minimum_nights": 0,
    # Campos con posible ausencia
    "name": 10, "host_name": 10, "number_of_reviews": 0,
    "last_review": 50, "reviews_per_month": 50, "calculated_host_listings_count": 0,
    "availability_365": 0
}

def latest_standard_file():
    pattern = str(STANDARD_DIR / f"{SOURCE_NAME}_*.csv")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No se encontraron archivos estandarizados con patrón {pattern}")
    files.sort(reverse=True)
    return Path(files[0])

def ensure_out_dirs():
    (QUALITY_OUT_DIR / "violations").mkdir(parents=True, exist_ok=True)

def pct(n, d):
    return 0 if d == 0 else round((n / d) * 100, 2)

def run_checks(df: pd.DataFrame):
    results = {"checks": [], "violations": {}}

    # --- Completitud: columnas esperadas ---
    missing_cols = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    results["checks"].append({
        "name": "completeness_expected_columns",
        "status": "FAIL" if missing_cols else "PASS",
        "missing_columns": missing_cols
    })

    # Si faltan columnas críticas, abortar otras reglas
    # (Igual seguimos, pero marcamos FAIL general luego)
    # --- Calidad: nulos por columna vs umbral ---
    nulls = df.isna().sum()
    total = len(df)
    nulls_pct = {col: pct(int(nulls.get(col, 0)), total) for col in EXPECTED_COLUMNS if col in df.columns}

    null_viol = {c: v for c, v in nulls_pct.items() if v > NULL_THRESHOLDS.get(c, 0)}
    results["checks"].append({
        "name": "quality_null_thresholds",
        "status": "FAIL" if len(null_viol) > 0 else "PASS",
        "nulls_pct": nulls_pct,
        "violations": null_viol
    })

    # --- Calidad: unicidad de clave primaria (id) ---
    if "id" in df.columns:
        dup_mask = df.duplicated(subset=["id"], keep=False)
        dup_count = int(dup_mask.sum())
        results["checks"].append({
            "name": "quality_primary_key_uniqueness",
            "status": "FAIL" if dup_count > 0 else "PASS",
            "duplicates_count": dup_count
        })
        if dup_count > 0:
            results["violations"]["duplicate_ids.csv"] = df.loc[dup_mask].sort_values("id")

    # --- Coherencia: valores dentro de rangos válidos ---
    range_viol = {}

    if "price" in df.columns:
        bad_price = df[(df["price"].isna()) | (df["price"] < 0)]
        if not bad_price.empty:
            range_viol["bad_price.csv"] = bad_price

    if "minimum_nights" in df.columns:
        bad_min_nights = df[(df["minimum_nights"].isna()) | (df["minimum_nights"] < 1)]
        if not bad_min_nights.empty:
            range_viol["bad_minimum_nights.csv"] = bad_min_nights

    if "latitude" in df.columns:
        bad_lat = df[(df["latitude"].isna()) | (df["latitude"] < -90) | (df["latitude"] > 90)]
        if not bad_lat.empty:
            range_viol["bad_latitude.csv"] = bad_lat

    if "longitude" in df.columns:
        bad_lon = df[(df["longitude"].isna()) | (df["longitude"] < -180) | (df["longitude"] > 180)]
        if not bad_lon.empty:
            range_viol["bad_longitude.csv"] = bad_lon

    if "availability_365" in df.columns:
        bad_av = df[(df["availability_365"].isna()) | (df["availability_365"] < 0) | (df["availability_365"] > 365)]
        if not bad_av.empty:
            range_viol["bad_availability_365.csv"] = bad_av

    if "last_review" in df.columns:
        # Fecha válida y no futura
        future_date = pd.to_datetime(date.today())
        bad_dates = df[(df["last_review"].notna()) & (pd.to_datetime(df["last_review"], errors="coerce") > future_date)]
        if not bad_dates.empty:
            range_viol["bad_last_review_future.csv"] = bad_dates

    if range_viol:
        results["checks"].append({
            "name": "coherence_value_ranges",
            "status": "FAIL",
            "violations_files": list(range_viol.keys())
        })
    else:
        results["checks"].append({
            "name": "coherence_value_ranges",
            "status": "PASS"
        })

    return results, range_viol

def write_report(results, violations, input_file: Path, started_at: str):
    QUALITY_OUT_DIR.mkdir(parents=True, exist_ok=True)
    ensure_out_dirs()

    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    base = f"quality_report_{stamp}"
    report_json = QUALITY_OUT_DIR / f"{base}.json"
    report_md = QUALITY_OUT_DIR / f"{base}.md"

    # Guardar violaciones detalladas como CSVs
    for fname, dfv in violations.items():
        out_path = QUALITY_OUT_DIR / "violations" / fname
        dfv.to_csv(out_path, index=False)

    # Estado general
    overall = "PASS"
    for chk in results["checks"]:
        if chk["status"] == "FAIL":
            overall = "FAIL"
            break

    results_out = {
        "source_file": str(input_file),
        "started_at_utc": started_at,
        "finished_at_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "overall_status": overall,
        "details": results["checks"],
        "violations_saved": list(violations.keys())
    }

    # JSON
    with open(report_json, "w", encoding="utf-8") as f:
        json.dump(results_out, f, ensure_ascii=False, indent=2)

    lines = []
    lines.append(f"# Reporte de Calidad de Datos – {base}")
    lines.append("")
    lines.append(f"- **Archivo de entrada:** `{input_file}`")
    lines.append(f"- **Estado general:** **{overall}**")
    lines.append("")
    lines.append("## Detalles de validación")
    for chk in results["checks"]:
        lines.append(f"### {chk['name']} → {chk['status']}")
        for k, v in chk.items():
            if k in ("name", "status"): 
                continue
            lines.append(f"- {k}: {v}")
        lines.append("")
    if violations:
        lines.append("## Archivos de violaciones generados")
        for v in violations.keys():
            lines.append(f"- `data/quality/violations/{v}`")
    report_md.write_text("\n".join(lines), encoding="utf-8")

    print(f"Reporte JSON: {report_json}")
    print(f"Reporte MD:   {report_md}")
    return report_json, report_md

def main():
    started_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    try:
        latest_csv = latest_standard_file()
        print(f"Usando archivo estandarizado más reciente: {latest_csv}")
        df = pd.read_csv(latest_csv, encoding="utf-8")

        results, violations = run_checks(df)
        report_json, report_md = write_report(results, violations, latest_csv, started_at)
        print("Validación de calidad completada.")
    except Exception as e:
        print(f"ERROR en la validación de calidad: {e}")
        raise

if __name__ == "__main__":
    main()
