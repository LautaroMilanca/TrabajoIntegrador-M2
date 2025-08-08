# ---------------------------
# Imagen base liviana de Python
# ---------------------------
FROM python:3.10-slim

# ---------------------------
# Establecemos el directorio de trabajo dentro del contenedor
# ---------------------------
WORKDIR /app

# ---------------------------
# Copiamos los archivos necesarios al contenedor
# ---------------------------
COPY extract_airbnb_data.py .
COPY requirements.txt .

# ---------------------------
# Instalamos las dependencias declaradas en requirements.txt
# ---------------------------
RUN pip install --no-cache-dir -r requirements.txt

# ---------------------------
# Definimos el ENTRYPOINT para que se ejecute el script
# ---------------------------
ENTRYPOINT ["python", "extract_airbnb_data.py"]
