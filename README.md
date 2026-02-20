# GestiónExpress - by Centro de Información

Proyecto de Gestión Administrativa y Operativa para **Consorcio Express S.A.S**  
Desarrollado con **FastAPI**, **Jinja2**, **PostgreSQL** y desplegado en **Render**.

---

## 🔗 Accesos rápidos

- [Panel de Render](https://dashboard.render.com/)
- [Repositorio Github](https://github.com/OperacionesConsorcioExpress/GestionExpress)
- [Aplicación en Producción](https://gestionconsorcioexpress.onrender.com/)

---

## 📊 Tecnologías utilizadas

- Backend: **Python**, **FastAPI**
- Frontend: **Jinja2**, **HTML**, **CSS**, **JavaScript** (vanilla)
- Base de datos: **PostgreSQL**
- Almacenamiento de archivos: **Azure Blob Storage**
- Despliegue: **Render**
- Control de versiones: **GitHub**

---

## 📁 Estructura del proyecto (Ejemplo)

```text
GestionExpress/
├── main.py                 # Archivo principal de FastAPI
├── controller/             # Lógica de negocio
├── model/                  # Acceso a base de datos
├── view/                   # Plantillas HTML con Jinja2
├── static/                 # JS, CSS, imágenes
├── cargues/                # Scripts de carga de datos
├── lib/                    # Librerías auxiliares
├── requirements.txt        # Dependencias del proyecto
└── .env                    # Variables de entorno
```

---

## 🛠️ Instrucciones de instalación local

1. **Abrir terminal** y posicionarse en el proyecto:

```
cd "/c/Users/sergio.hincapie/OneDrive - Grupo Express/Gestión de la Operación/0 - Script Python/GestiónExpress"
```

**2:** Crear un entorno virtual:

```
"C:\Program Files\Python312\python.exe" -m venv venv
```

**3:** Ingresar en el entorno virtual:

```
.\venv\Scripts\Activate.ps1 # para PowerShell
venv\Scripts\activate.bat # Para CMD.exe

```

**4:** Descargar las dependencias del archivo 'requirements.txt' con el comando:

```
pip install -r requirements.txt
```

**5:** Correr el servidor web de uvicorn para visualizar la aplicación:
uvicorn <archivo>:<instancia> --reload

```
uvicorn main:app --reload
```

**6:** En el navegador ir al localhost:8000.

```
(http://127.0.0.1:8000/)
```

**7:** Construir Archivo Requirements.txt #####
Genera la lista de todas las libreris utilizadas con su versión

```
pip freeze > requirements.txt
```

---

## 🔐 Variables de entorno requeridas

Para ejecución local y despliegue, asegúrate de definir las siguientes variables de entorno en un archivo `.env` en la raíz del proyecto:

| Variable                          | Descripción                                        |
| --------------------------------- | -------------------------------------------------- |
| `AZURE_STORAGE_CONNECTION_STRING` | Cadena de conexión a Azure Blob Storage            |
| `DATABASE_PATH`                   | URL de conexión a la base de datos PostgreSQL      |
| `CLIENT_ID` / `CLIENT_SECRET`     | Autenticación para Microsoft OAuth2                |
| `TENANT_ID`                       | Tenant para autenticación Azure                    |
| `SECRET_KEY`                      | Clave secreta para seguridad interna de la app     |
| `USUARIO_CORREO_JURIDICO`         | Usuario de correo para módulo jurídico             |
| `CLAVE_CORREO_JURIDICO`           | Contraseña correspondiente                         |
| `HUGGINGFACEHUB_API_TOKEN`        | Token para conexión con modelos IA de Hugging Face |

---

## 💡 Módulos funcionales desarrollados

### 1. Asignaciones de técnicos a controles

- Asignación visual con filtros
- Cargue masivo desde plantilla Excel
- Gestión de técnicos por control y usuario

### 2. Checklist vehicular

- Registro diario de estado de documentos y componentes
- Consolidado de fallas, historial, y ajustes por usuario
- Visualización modal por ítem con control de solución
- Generación de reportes CSV, Excel y JSON

### 3. Explorador de Azure Blob Storage

- Navegación por contenedores y archivos
- Carga, descarga, eliminación de archivos
- Interfaz moderna con barra de progreso y filtrado

### 4. Dashboard Power BI embebido

- Visualización de indicadores operativos
- Integración segura por iframe
- Enlaces y actualizaciones dinámicas

### 5. Gestión de cláusulas jurídicas

- Registro masivo y seguimiento por procesos/subprocesos
- Notificaciones automáticas por correo
- Carga desde archivos Excel o entrada manual

### 6. Gestión de usuarios y roles

- Creación y modificación de usuarios
- Asignación de roles por módulo
- Validación de acceso con seguridad por sesión

### 7. Gestión SGI Planes de Acción

Este paquete contiene todo lo necesario para que el administrador
integre el modulo SGI dentro de GestionExpress actualizado.

Incluye:

- Guia de integracion
- Plantilla de variables de entorno
- Dependencias minimas del modulo
- Checklist de validacion

Resumen tecnico

- El modulo SGI se integra en el backend (router FastAPI) y en el frontend
  (plantillas y assets en /static).
- La limpieza recursiva en ADLS Gen2 ya esta implementada en
  model/gestion_sgi.py y requiere azure-storage-file-datalake.

1. Archivos del modulo (SGI_module)
   Copiar estos archivos al proyecto actualizado, respetando rutas:

- controller/route_sgi.py
- model/gestion_sgi.py
- model/auditoria_sgi.py
- model/gestion_notificaciones.py
- model/cache_ultra_sgi.py
- model/gestionar_db.py
- view/sgi.html
- view/components/layout.html
- view/components/encabezados.html
- static/js/optimizacion_grilla_ultra.js
- static/Consorcio.png
- static/images/ventana.png
- sgi_enterprise_integration.py (opcional)
- sgi_enterprise_data_manager.py (opcional)

2. Dependencias Python
   Instalar en el mismo venv donde corre la app:

- azure-storage-blob==12.28.0
- azure-storage-file-datalake==12.15.0
- psycopg2-binary
- fastapi, starlette, jinja2, pandas, python-dotenv, python-multipart

Se recomienda instalar requirements.txt completo del proyecto.

3. Variables de entorno (ver .env.sgi.template)
   Obligatorias para SGI:

- DATABASE_PATH
- DB_POOL_MAX
- AZURE_STORAGE_CONNECTION_STRING
- SGI_EVIDENCIAS_CONTAINER
- SGI_EVIDENCIAS_PREFIX
- SGI_EVIDENCIAS_SAS_MINUTES
- SGI_ANALISIS_CAUSAS_CONTAINER
- SGI_ANALISIS_CAUSAS_PREFIX
- SGI_CIERRE_ROLES

4. Configuracion de storage en BD
   En sgi_storage_config deben existir (o coincidir con el .env):

- sgi_evidencias_container
- sgi_evidencias_prefix
- sgi_evidencias_container_PA / \_GC / \_RVD (opcional)
- sgi_evidencias_prefix_PA / \_GC / \_RVD (opcional)
- sgi_analisis_causas_container
- sgi_analisis_causas_prefix

5. BD y funciones requeridas (schema sgi)
   Tablas:

- planes_accion, gestion_cambio, revision_direccion
- sgi_actividades, sgi_evidencias, sgi_analisis_causas_adjuntos
- sgi_cierres, sgi_cierres_historial, sgi_notificaciones
- auditoria_log
- categorias_procesos, procesos, subprocesos, sgi_storage_config

Funciones/vistas:

- generar_codigo_pa, generar_codigo_gc, generar_codigo_rvd
- refresh_sgi_ultra_performance()
- busqueda_sgi_ultra(...)
- mv_sgi_procesos_ultra
- sgi.registrar_auditoria(...)

Nota: sgi_actividades debe incluir la columna tipo_actividad.

6. Integracion en main.py

- Incluir el router:
  from controller.route_sgi import router_sgi
  app.include_router(router_sgi)
- Evitar duplicar /sgi
- Mantener Jinja2Templates(directory="view")
- Mantener StaticFiles montado en /static

7. Permisos ADLS Gen2
   La identidad usada por AZURE_STORAGE_CONNECTION_STRING debe tener permisos
   para listar y borrar archivos y directorios en el contenedor.

8. Checklist rapido
   Ver CHECKLIST_ADMIN_SGI.txt

---
