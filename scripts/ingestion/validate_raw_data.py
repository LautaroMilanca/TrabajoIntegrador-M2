import os
import pandas as pd
from datetime import datetime

RAW_FILE_PATH = "data/raw_data/listings_raw.csv"
LOG_FILE_PATH = "data/raw_data/validation_log.txt"

def log(message):
    with open(LOG_FILE_PATH, "a") as f:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"[{timestamp}] {message}\n")
    print(message)

def validate_file(path):
    if not os.path.exists(path):
        log(f"ERROR: El archivo {path} no existe.")
        return False

    if os.path.getsize(path) < 100:  # tamaño mínimo en bytes
        log(f"ERROR: El archivo {path} está vacío o es demasiado pequeño.")
        return False

    try:
        df = pd.read_csv(path)

        if df.empty:
            log(f"ERROR: El archivo {path} fue leído pero está vacio.")
            return False

        if df.duplicated().sum() > 0:
            log(f"ADVERTENCIA: Se encontraron {df.duplicated().sum()} filas duplicadas en {path}.")
        else:
            log(f"VALIDACIÓN EXITOSA: El archivo {path} es válido, con {len(df)} registros únicos.")

        return True
    except Exception as e:
        log(f"ERROR: Falló la lectura del archivo {path} → {str(e)}")
        return False

if __name__ == "__main__":
    validate_file(RAW_FILE_PATH)
