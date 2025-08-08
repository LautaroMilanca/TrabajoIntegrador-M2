import pandas as pd
import os

def main():
    input_path = "data/AB_NYC.csv"
    output_path = "data/raw_data/listings_raw.csv"

    # Crear la carpeta de salida si no existe
    os.makedirs("data/raw_data", exist_ok=True)

    try:
        print(f"Leyendo archivo local desde {input_path}...")
        df = pd.read_csv(input_path)

        print(f"Guardando archivo estructurado en {output_path}...")
        df.to_csv(output_path, index=False)
        print("Extracción completada exitosamente.")
    except Exception as e:
        print(f"Error durante el proceso de extracción: {e}")

if __name__ == "__main__":
    main()
