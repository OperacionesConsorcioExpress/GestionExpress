# Usa una imagen oficial de Python
FROM python:3.12-slim

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia solo los archivos necesarios (evita venv y __pycache__)
COPY . /app

# Instala dependencias
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Expone el puerto en el contenedor
EXPOSE 80

# Comando para iniciar la aplicación
CMD ["gunicorn", "main:app", "--workers=1", "--worker-class=uvicorn.workers.UvicornWorker", "--bind=0.0.0.0:80", "--timeout=600"]
