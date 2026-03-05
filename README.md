# GestiónExpress

### Sistema de Gestión Administrativa y Operativa — Consorcio Express S.A.S

---

## 🔗 Accesos rápidos

| Recurso                     | URL                                                           |
| --------------------------- | ------------------------------------------------------------- |
| 🌐 Aplicación en producción | https://gestionconsorcioexpress.azurewebsites.net             |
| 📦 Repositorio GitHub       | https://github.com/OperacionesConsorcioExpress/GestionExpress |
| 🐳 Imagen Docker Hub        | https://hub.docker.com/r/cinf/gestionexpress/tags             |
| 📊 Azure Portal             | https://portal.azure.com                                      |

---

## 🛠️ Tecnologías

| Capa                 | Tecnología                                                |
| -------------------- | --------------------------------------------------------- |
| Backend              | Python 3.12, FastAPI, Uvicorn, Gunicorn                   |
| Frontend             | Jinja2, HTML, CSS, JavaScript vanilla, Bootstrap, Leaflet |
| Base de datos        | PostgreSQL (Azure Flexible Server — D2ds_v5)              |
| Almacenamiento       | Azure Blob Storage                                        |
| Despliegue           | Azure App Service vía Docker Hub                          |
| CI/CD                | GitHub Actions (workflows automáticos)                    |
| Control de versiones | GitHub                                                    |

---

## 📁 Estructura del proyecto

```
GestiónExpress/
├── main.py                    # Punto de entrada FastAPI — rutas, middleware, lifespan
├── Dockerfile                 # Imagen de producción
├── requirements.txt           # Dependencias Python
├── .env                       # Variables de entorno (NO subir a Git)
│
├── controller/                # Endpoints FastAPI (route_*.py)
├── model/                     # Lógica de negocio y acceso a datos (gestion_*.py)
├── database/
│   └── database_manager.py    # Pool centralizado, circuit breaker, resiliencia
├── jobs/                      # Scripts de ejecución programada (GitHub Actions)
├── lib/                       # Librerías auxiliares compartidas
├── view/                      # Plantillas HTML Jinja2
├── static/                    # Assets JS, CSS, imágenes
└── .github/
    └── workflows/             # GitHub Actions — jobs automáticos
```

---

## 🔐 Variables de entorno

Definir en `.env` para local y en **Azure App Service → Configuration** para producción.

| Variable                          | Descripción                                                                | Entorno    |
| --------------------------------- | -------------------------------------------------------------------------- | ---------- |
| `DATABASE_PATH`                   | URL PostgreSQL — puerto **6432** (PgBouncer, solo Azure interno)           | Producción |
| `DATABASE_PATH_DEDICATED`         | URL PostgreSQL — puerto **5432** (directo, LISTEN/NOTIFY y GitHub Actions) | Ambos      |
| `DB_POOL_MIN`                     | Conexiones mínimas del pool por worker (recomendado: `2`)                  | Ambos      |
| `DB_POOL_MAX`                     | Conexiones máximas del pool por worker (recomendado: `8`)                  | Ambos      |
| `AZURE_STORAGE_CONNECTION_STRING` | Cadena de conexión a Azure Blob Storage                                    | Ambos      |
| `SGI_EVIDENCIAS_CONTAINER`        | Contenedor Azure para evidencias SGI                                       | Ambos      |
| `SGI_EVIDENCIAS_PREFIX`           | Prefijo de ruta para evidencias SGI                                        | Ambos      |
| `SGI_EVIDENCIAS_SAS_MINUTES`      | Minutos de validez del token SAS                                           | Ambos      |
| `CLIENT_ID`                       | ID de aplicación Azure AD (OAuth2)                                         | Ambos      |
| `CLIENT_SECRET`                   | Secreto de aplicación Azure AD                                             | Ambos      |
| `TENANT_ID`                       | Tenant ID Azure AD                                                         | Ambos      |
| `SECRET_KEY`                      | Clave secreta para sesiones FastAPI                                        | Ambos      |
| `USUARIO_JURIDICO`                | Correo para notificaciones módulo jurídico                                 | Ambos      |
| `CLAVE_JURIDICO`                  | Contraseña del correo jurídico                                             | Ambos      |
| `DEEPSEEK_API_KEY`                | API Key DeepSeek IA                                                        | Ambos      |
| `GEMINI_API_KEY`                  | API Key Google Gemini                                                      | Ambos      |
| `HUGGINGFACE_API_KEY`             | API Key Hugging Face                                                       | Ambos      |

> **Nota local:** En `.env` local usar puerto `5432` en ambas variables `DATABASE_PATH` y `DATABASE_PATH_DEDICATED`.
> El puerto `6432` (PgBouncer) solo es accesible desde dentro de la red de Azure.

---

## 🖥️ Instalación y ejecución local

**1. Abrir terminal y navegar al proyecto:**

```powershell
cd "C:\Users\sergio.hincapie\OneDrive - Grupo Express\Gestión de la Operación\0 - Script Python\GestiónExpress"
```

**2. Crear entorno virtual:**

```powershell
"C:\Program Files\Python312\python.exe" -m venv venv
```

**3. Activar entorno virtual:**

```powershell
# PowerShell
.\venv\Scripts\Activate.ps1

# CMD
venv\Scripts\activate.bat
```

**4. Instalar dependencias:**

```powershell
pip install -r requirements.txt
```

**5. Correr el servidor local:**

```powershell
uvicorn main:app --reload
```

**6. Abrir en el navegador:**

```
http://127.0.0.1:8000
```

**7. Actualizar requirements.txt** (cuando se instalen nuevas librerías):

```powershell
pip freeze > requirements.txt
```

---

## 🐳 Publicar actualización en Azure App Service (vía Docker)

> Ejecutar estos pasos cada vez que se quiera desplegar una nueva versión a producción.

### 1. Abrir PowerShell y navegar al proyecto

```powershell
cd "C:\Users\sergio.hincapie\OneDrive - Grupo Express\Gestión de la Operación\0 - Script Python\GestiónExpress"
```

### 2. Verificar que existe el Dockerfile

```powershell
ls Dockerfile
```

### 3. Iniciar sesión en Docker Hub _(una vez por sesión)_

```powershell
docker login
# Usuario: cinf
# Password: Pru3b@*Jun-2024
# Resultado esperado: Login Succeeded
```

### 4. (Opcional) Limpiar contenedores locales anteriores

```powershell
docker ps -a
docker stop <id>
docker rm <id>
```

### 5. Construir la nueva imagen local

```powershell
docker build -t gestionexpress .
# Esperar: Successfully tagged gestionexpress:latest
```

### 6. Probar localmente antes de publicar _(obligatorio)_

```powershell
docker run -d --rm -p 8080:80 --name ge-test gestionexpress
# Abrir: http://localhost:8080
# Verificar que la app funciona correctamente

# Detener al terminar la prueba:
docker stop ge-test
```

### 7. Etiquetar la imagen

```powershell
# Tag versionado (para histórico y rollback)
$TAG_VER="2026-03-05"
docker tag gestionexpress cinf/gestionexpress:$TAG_VER

# Tag latest (el que consume Azure App Service)
docker tag gestionexpress cinf/gestionexpress:latest
```

### 8. Publicar a Docker Hub

```powershell
# (Opcional) subir versión histórica
docker push cinf/gestionexpress:$TAG_VER

# (Obligatorio) subir latest — es el que usa Azure
docker push cinf/gestionexpress:latest
```

### 9. Verificar publicación

- Ir a https://hub.docker.com/r/cinf/gestionexpress/tags
- Confirmar que `latest` muestra **"Last pushed: a few seconds/minutes ago"**

### 10. Azure App Service toma la imagen automáticamente

Azure App Service está configurado para usar `cinf/gestionexpress:latest`.
Una vez publicado en Docker Hub, reiniciar la instancia desde el portal si el cambio no se refleja inmediatamente:

```
Azure Portal → App Service → gestionconsorcioexpress → Overview → Restart
```

---

## ⚙️ GitHub Actions — Jobs automáticos

Los workflows se encuentran en `.github/workflows/` y se ejecutan automáticamente o de forma manual desde GitHub.

| Workflow               | Trigger                        | Descripción                                                   |
| ---------------------- | ------------------------------ | ------------------------------------------------------------- |
| `posicionamientos.yml` | Diario 5:00 AM Bogotá / Manual | Procesa archivos Parquet de GPS desde Azure Blob → PostgreSQL |

> Los jobs usan `DATABASE_PATH_DEDICATED` (puerto 5432 directo) porque GitHub Actions corre fuera de la red de Azure y no tiene acceso a PgBouncer (puerto 6432).

**Secrets requeridos en GitHub** (`Settings → Secrets → Actions`):

| Secret                            | Descripción                  |
| --------------------------------- | ---------------------------- |
| `DATABASE_PATH_DEDICATED`         | URL PostgreSQL puerto 5432   |
| `AZURE_STORAGE_CONNECTION_STRING` | Cadena conexión Blob Storage |

---

## 📡 Monitoreo

### Estado del sistema

```
GET https://gestionconsorcioexpress.azurewebsites.net/health
```

Respuesta esperada en operación normal:

```json
{
  "status": "🟢 Gestión Express activo",
  "database": {
    "status": "ok",
    "circuit_breaker": "CLOSED",
    "cb_fallas": 0,
    "db_ping": "ok",
    "db_idle_tx": 0,
    "db_total": 9,
    "db_tiempo_ms": 12.4
  }
}
```

| Campo             | Valor normal | Alerta                    |
| ----------------- | ------------ | ------------------------- |
| `circuit_breaker` | `CLOSED`     | `OPEN` → resetear         |
| `db_idle_tx`      | `0`          | `> 0` → bug en código     |
| `db_total`        | `< 30`       | `> 45` → riesgo de límite |
| `db_tiempo_ms`    | `< 200`      | `> 500` → latencia alta   |

### Resetear circuit breaker

Si el `circuit_breaker` aparece en `OPEN` después de un redeploy pero la DB está ok:

```
GET https://gestionconsorcioexpress.azurewebsites.net/reset-circuit-breaker
```

**Flujo recomendado:**

1. `GET /health` → confirmar `db_ping: "ok"`
2. `GET /reset-circuit-breaker` → resetear
3. `GET /health` → confirmar `circuit_breaker: "CLOSED"`

---

## 🗄️ Base de datos — Límite de conexiones

| Servidor                       | Plan    | Conexiones máx |
| ------------------------------ | ------- | -------------- |
| `serverdbceinfop` (producción) | D2ds_v5 | 50             |

**Distribución en operación normal:**

| Fuente                                     | Conexiones  |
| ------------------------------------------ | ----------- |
| Azure App Service (2 workers × pool_min=2) | 4           |
| Pico máximo app (2 workers × pool_max=8)   | 16          |
| Conexión dedicada LISTEN/NOTIFY            | 1           |
| Overhead Azure interno                     | 3           |
| Reservadas PostgreSQL superusuarios        | 3           |
| **Total máximo**                           | **27 / 50** |

> ⚠️ No conectar entornos locales de desarrollo contra la DB de producción simultáneamente con la app corriendo. Usar una DB de desarrollo separada.

---

## 🔄 Sistema de resiliencia de conexiones

`database/database_manager.py` implementa 3 capas de protección:

| Capa                    | Mecanismo                            | Comportamiento                                 |
| ----------------------- | ------------------------------------ | ---------------------------------------------- |
| **1 — Colchón**         | Semáforo con 1 slot reservado        | Siempre hay 1 conexión libre para health check |
| **2 — Cola**            | Timeout de 3 segundos                | Espera antes de responder "intente más tarde"  |
| **3 — Circuit Breaker** | 10 fallos → OPEN, 15s → recuperación | Bloquea nuevas conexiones si la DB está caída  |

---

## 🏗️ Comando de inicio en Azure App Service

```bash
gunicorn main:app --workers 2 --worker-class=uvicorn.workers.UvicornWorker --bind=0.0.0.0:80 --timeout 600
```
