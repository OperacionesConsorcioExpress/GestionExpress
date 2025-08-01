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

---
