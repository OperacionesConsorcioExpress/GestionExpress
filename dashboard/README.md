# Dashboard — Módulo de Business Intelligence

Este submódulo es independiente del resto de la aplicación GestiónExpress. Su único propósito es mostrar gráficas y datos de negocio usando [Apache ECharts](https://echarts.apache.org/). Tiene su propia conexión a base de datos, su propio sistema de permisos y su propio menú — no depende del resto del software para funcionar.

**Regla de oro de este módulo:** hay un "armazón" (los archivos que arman el menú, las rutas, la conexión a base de datos y el estilo visual) y hay "lienzos" (los archivos donde se construye cada visualización). El trabajo del día a día — propio o con ayuda de una IA — ocurre casi siempre en los lienzos, nunca en el armazón. Más abajo se explica exactamente cuáles son unos y otros, y al final hay un texto listo para copiar y pegar a cualquier asistente de IA para que respete esta regla.

---

## Tabla de contenido

1. [¿Qué hace este módulo?](#1-qué-hace-este-módulo)
2. [El armazón vs. los lienzos](#2-el-armazón-vs-los-lienzos)
3. [Estructura de carpetas](#3-estructura-de-carpetas)
4. [Cómo funciona por dentro](#4-cómo-funciona-por-dentro)
5. [Conexión a base de datos: capas Plata y Oro](#5-conexión-a-base-de-datos-capas-plata-y-oro)
5bis. [Rendimiento: vistas materializadas y refresco](#5bis-rendimiento-vistas-materializadas-y-refresco)
6. [Cómo agregar o desarrollar una visualización](#6-cómo-agregar-o-desarrollar-una-visualización)
7. [Prompt estándar para trabajar con IA](#7-prompt-estándar-para-trabajar-con-ia)
8. [Sistema de roles y permisos](#8-sistema-de-roles-y-permisos)
9. [Estilos y componentes visuales](#9-estilos-y-componentes-visuales)
10. [Preguntas frecuentes](#10-preguntas-frecuentes)

---

## 1. ¿Qué hace este módulo?

El usuario entra a `/business_intelligence` y ve una pantalla con un menú lateral, organizado por áreas de negocio (EMIC, Gerenciales, Ingresos, Kilómetros, Operacionales, Operadores, Pasajeros). Desde ese menú selecciona una visualización, que se carga sin recargar la página. Cada visualización pide sus propios datos al servidor y los pinta con ECharts.

**Flujo resumido:**

```
Usuario abre el menú lateral
       ↓
Selecciona una visualización (ej. "SIGMA")
       ↓
El navegador pide al servidor el HTML de esa visualización
       ↓
El HTML llega y ejecuta su script, que pide los datos al servidor
       ↓
Los datos llegan como JSON (consultados en Plata u Oro) y ECharts los dibuja
```

Todo esto ocurre sin recargar la página. Si el usuario selecciona otra visualización, la anterior se destruye limpiamente y la nueva ocupa su lugar.

---

## 2. El armazón vs. los lienzos

Para que cualquier persona (o cualquier IA) sepa dónde puede trabajar sin romper nada, el módulo se divide en dos tipos de archivos:

### 🔒 El armazón — se toca solo para cambios estructurales, no para desarrollar una gráfica

| Archivo | Qué hace | Cuándo se toca |
|---|---|---|
| `config.py` | Define qué áreas y visualizaciones existen, sus íconos y sus rutas | Solo al **registrar** una visualización nueva (una vez, al crearla) |
| `router.py` | Recibe las peticiones HTTP y conecta cada URL con su función de datos | Solo al **registrar** una visualización nueva (una vez, al crearla) |
| `database/database_manager.py` | Administra las conexiones a las bases de datos Plata y Oro | Nunca, salvo un cambio de infraestructura de base de datos |
| `static/css/bi_dashboard.css` | Estilos visuales compartidos por todo el módulo (tarjetas, chips, menú) | Solo si se necesita un componente visual nuevo que usarán varias gráficas |
| `templates/_echarts_theme.html` | Paleta de colores y configuración base de ECharts (`GE_BI`) | Solo si cambia la identidad visual corporativa |
| `templates/_bi_menu.html` / `templates/business_intelligence.html` | El menú lateral y la página principal | Casi nunca |

### 🎨 Los lienzos — aquí se desarrolla cada visualización

| Carpeta | Qué contiene |
|---|---|
| `model/{área}/bi_{nombre}.py` | La consulta de datos de una visualización específica |
| `templates/charts/{área}/{nombre}.html` | El HTML + JS de esa visualización específica |

**En resumen:** para construir o modificar una gráfica, solo se edita su archivo en `model/` y su archivo en `templates/charts/`. El armazón ya está armado y **no debería cambiar** por el trabajo normal de desarrollar dashboards.

---

## 3. Estructura de carpetas

```
dashboard/
│
├── __init__.py                     ← Punto de entrada del módulo
├── config.py                       ← Registro del menú: áreas, visualizaciones e íconos
├── router.py                       ← Rutas HTTP y proveedores de datos
├── README.md                       ← Este documento
│
├── database/                       ← Conexión propia del Dashboard (independiente del software)
│   ├── __init__.py
│   ├── database_manager.py         ← get_plata_connection() / get_oro_connection()
│   ├── infisical_client.py         ← Descarga de credenciales desde Infisical (cacheada)
│   └── refresco_bi.py              ← Mantiene al día las vistas materializadas (ver §5bis)
│
├── model/                          ← Una carpeta por área de negocio ("lienzo" de datos)
│   ├── __init__.py                 ← Re-exporta todos los modelos para el router
│   ├── emic/            (DPV, ICO, ICS, IRI, ISV)
│   ├── gerenciales/     (Gestión COP, Gestión Operacional)
│   ├── ingresos/        (Benchmarking SITP, Ingresos CEXP)
│   ├── kilometros/      (Kilómetros Comercial, Kilómetros Eliminados y No Ejecutados, Objeción SNE)
│   ├── operacionales/   (Ejecución Operacional)
│   ├── operadores/      (Conductas Operacionales, Disponibilidad de Operadores, SIGMA)
│   └── pasajeros/       (Demanda de Pasajeros CEXP, Validaciones SITP)
│
├── templates/                      ← Archivos HTML que ve el navegador
│   ├── business_intelligence.html  ← Página principal (hub)
│   ├── _bi_menu.html                ← Panel lateral del menú
│   ├── _echarts_theme.html          ← Configuración visual global de ECharts (GE_BI)
│   └── charts/                     ← Un archivo por visualización ("lienzo" visual), mismo mapa que model/
│       ├── emic/
│       ├── gerenciales/
│       ├── ingresos/
│       ├── kilometros/
│       ├── operacionales/
│       ├── operadores/
│       └── pasajeros/
│
└── static/
    └── css/
        └── bi_dashboard.css         ← Estilos exclusivos del módulo BI (clases `.bi-*`)
```

`model/{área}/` y `templates/charts/{área}/` son siempre un espejo uno del otro: la misma área, el mismo nombre de archivo (el modelo usa el prefijo `bi_`, la plantilla no).

---

## 4. Cómo funciona por dentro

### 4.1 El menú se construye desde `config.py`

Todo lo que aparece en el menú lateral está declarado en `config.py`, en cuatro diccionarios:

- **`ICONOS_AREA`** — qué ícono (Bootstrap Icons) muestra cada área.
- **`ICONOS_GRAFICA`** — qué ícono muestra cada visualización en el menú.
- **`RUTAS_GRAFICA`** — la ruta interna de cada visualización, con el formato `área/identificador` (ej. `"operadores/sigma"`).
- **`AREAS_GRAFICAS`** — qué visualizaciones pertenecen a cada área, en el orden en que aparecen en el menú.

La función `construir_menu()` junta todo eso en un único objeto que consume el menú HTML. Áreas y visualizaciones se mantienen **ordenadas alfabéticamente**.

### 4.2 El router conecta peticiones con datos

`router.py` expone tres rutas:

| Ruta | Qué hace |
|---|---|
| `GET /business_intelligence` | Devuelve la página principal con el menú |
| `GET /bi/partial/{área}/{identificador}` | Devuelve el HTML de una visualización específica |
| `GET /api/bi/{área}/{identificador}` | Devuelve los datos en JSON para que la visualización los dibuje |

El diccionario `PROVEEDORES_DATOS` conecta cada URL de datos con su función Python:

```python
PROVEEDORES_DATOS = {
    "operadores/sigma":                bi_sigma.consultar,
    "kilometros/kilometros_comercial": bi_kilometros_comercial.consultar,
    ...
}
```

### 4.3 Cada modelo es una función `consultar()`

Todos los archivos en `model/` siguen la misma regla: exponen una función llamada `consultar()` que devuelve un diccionario con los datos, con esta forma estándar:

```python
def consultar() -> dict:
    return {
        "ok": True,
        "fuente": "sigma",
        "datos": [],   # aquí va el resultado real de la consulta
    }
```

El router llama a esta función cuando el navegador pide `/api/bi/{área}/{identificador}`, y devuelve lo que sea que retorne, tal cual, como JSON.

### 4.4 Cada visualización es un archivo HTML autónomo

Los archivos en `templates/charts/` son fragmentos HTML (no páginas completas). Cada uno tiene:

1. **Un contenedor visual** (`.bi-tarjeta-grafica`) con el título y los chips de estado.
2. **Un lienzo** (`<div id="lienzo-...">`) donde ECharts dibuja la gráfica.
3. **Un script** que inicializa ECharts, pide los datos con `fetch('/api/bi/...')` y llama a `GE_BI.base({...})` para pintar.

---

## 5. Conexión a base de datos: capas Plata y Oro

El Dashboard **no usa** la conexión del software principal (`DATABASE_PATH` / `DATABASE_PATH_DEDICATED`, definida en `database/database_manager.py` en la raíz del proyecto). Tiene su propia conexión, independiente, definida en `dashboard/database/database_manager.py`.

Esto mantiene el análisis de negocio separado del software operativo: un problema de carga en el Dashboard no afecta la operación diaria, y viceversa.

### 5.1 De dónde salen las credenciales

La variable **`BI_DATABASE_CONNECTION_MODE`** decide el origen de las credenciales:

| Modo | Origen | Comportamiento ante fallo |
|---|---|---|
| `infisical` | Secretos `CAPA_{CAPA}_*` descargados de Infisical | Falla explícita: no cae a credenciales locales |
| `auto` | Infisical → entorno local → DSN heredado | Degrada al siguiente origen disponible |
| `legacy` | DSN completo `DATABASE_{CAPA}_CEINF` | No consulta Infisical |

Los secretos que cada capa espera son `CAPA_PLATA_HOST`, `CAPA_PLATA_DATA_BASE`, `CAPA_PLATA_USER`, `CAPA_PLATA_PASSWORD` y, opcionalmente, `CAPA_PLATA_PORT` (por defecto 5432). Lo mismo para `CAPA_ORO_*`.

La descarga desde Infisical la resuelve `dashboard/database/infisical_client.py` mediante *Universal Auth* (identidad de máquina). Se hace **una sola vez por proceso** y se reutiliza durante `INFISICAL_CACHE_TTL_SEG` segundos (15 min por defecto), de modo que ninguna consulta del Dashboard genera tráfico HTTP adicional. Para forzar la recarga tras rotar credenciales, sin reiniciar la aplicación, llame a `recargar_secretos()` desde `dashboard.database.database_manager`.

Los pools se crean **bajo demanda**: una capa que ningún modelo consulta nunca abre conexiones.

Para usarlas desde cualquier modelo (`model/{área}/bi_{nombre}.py`):

```python
from dashboard.database.database_manager import get_plata_connection, get_oro_connection

def consultar() -> dict:
    with get_plata_connection() as conexion:
        with conexion.cursor() as cursor:
            cursor.execute("SELECT ... FROM ...")
            filas = cursor.fetchall()

    return {
        "ok": True,
        "fuente": "sigma",
        "datos": filas,
    }
```

Ambas conexiones (`get_plata_connection()` y `get_oro_connection()`) funcionan igual: administran su propio grupo de conexiones (pool) y su propio "cortacircuito" (si una base de datos falla varias veces seguidas, deja de intentar por unos segundos en vez de colgar la aplicación). Un fallo en Plata no afecta a Oro, ni al revés.

---

## 5bis. Rendimiento: vistas materializadas y refresco

Consultar directamente una vista de la capa Plata puede ser muy lento cuando esa vista resuelve dimensiones al vuelo. El caso de *Kilómetros no realizados* sirve de referencia: la vista original unía cada hecho con la dimensional de rutas mediante un `LEFT JOIN LATERAL` que **se ejecutaba una vez por fila**. El tablero tardaba **31,5 segundos** en abrir.

Tras el rediseño abre en **8,3 segundos**. Estas son las decisiones que lo lograron y el criterio para repetirlas.

### 5bis.1 Qué materializar y qué no

No todo se materializa. La regla es **el costo de la unión**:

| Tipo de unión | Ejemplo | Decisión | Por qué |
|---|---|---|---|
| `LATERAL` con `ORDER BY … LIMIT 1` | `dim_rutas` (vigencias solapadas) | **Materializar** | Se ejecuta por fila. Costaba 8× en cada consulta |
| `JOIN` contra una dimensional pequeña | `dim_calendario` (4.025 filas) | **Al vuelo** | Es un hash join. Medido: un 8 % *más rápido* que duplicar sus columnas |

Duplicar el calendario en el millón de filas de la materializada la hacía más ancha y **más lenta**. Dejarlo en su dimensional permite `Parallel Index Only Scan`.

De ahí que la capa de datos sean **dos objetos**:

```
pl_k01_kilometros.bi_km_eliminados      (materializada)  hechos + ruta resuelta · 18 columnas
pl_k01_kilometros.vw_bi_km_eliminados   (vista)          la anterior ⋈ dim_calendario · sin disco
```

El modelo consulta **siempre la vista**, nunca la materializada directamente.

### 5bis.2 Qué columnas incluir

Solo las que el tablero usa: filtros, dimensiones de agrupación, métricas y detalle. La vista de origen tenía 85 columnas; la materializada tiene 18. Antes de añadir una columna, compruebe que aparece en el modelo o en el HTML.

**Cuidado con las columnas de alta cardinalidad que solo alimentan un filtro.** `servicio` tenía 41.989 valores distintos: su desplegable era inservible y el catálogo viajaba entero al navegador en cada carga. Retirarla —de la materializada, de `FILTER_SPECS`, de `OPTION_COLUMNS` y del `FILTER_GROUPS` de la plantilla— redujo el payload de **866 KB a 196 KB**. Antes de conservar un filtro, pregúntese cuántas opciones tendrá su lista.

**El grano se mantiene a nivel de registro.** Se evaluó pre-agregar por las 16 dimensiones: reducía apenas un 12 % y alteraba los percentiles del boxplot, que se calculan sobre valores individuales. Pre-agregar solo compensa si la reducción es grande y ninguna gráfica necesita la distribución.

### 5bis.3 Dimensionales con vigencia

`dim_rutas` maneja `fecha_inicial` / `fecha_fin` y conserva vigencias caducadas que nunca se extendieron. Cruzar en estricto por rango dejaba 7.706 hechos sin ruta. El `ORDER BY` del `LATERAL` aplica un respaldo de dos niveles:

```sql
ORDER BY
    (ke.fecha >= d.fecha_inicial AND ke.fecha <= d.fecha_fin) DESC,  -- 1º la vigencia que cubre
    d.fecha_fin DESC, d.fecha_inicial DESC                            -- 2º la más reciente conocida
```

Verificado: solo **añade** valores, nunca altera los que ya se resolvían bien.

### 5bis.4 El refresco es obligatorio

Una materializada no se actualiza sola. Sin refresco el tablero responde rápido **con datos congelados**, que es la peor forma de fallar porque no avisa.

Los ETL que cargan las tablas de origen corren fuera de este repositorio y el servidor **no tiene `pg_cron`**, así que lo gobierna la aplicación: `dashboard/database/refresco_bi.py` compara el sello de carga del origen con el de la materializada y refresca solo si hay datos nuevos, con `REFRESH … CONCURRENTLY` para no bloquear lecturas.

**Cada modelo registra la suya**, al final del archivo:

```python
from dashboard.database.refresco_bi import registrar_materializada

registrar_materializada(
    nombre="bi_mi_indicador",
    objeto="pl_x01_esquema.bi_mi_indicador",   # la materializada
    origen="pl_x01_esquema.tabla_origen",      # tabla que llena el ETL
    columna_sello="fecha_hora_carga",          # marca de tiempo de la carga
    al_refrescar=limpiar_cache,                # opcional: invalidar cachés
)
```

El arranque de `main.py` dispara el vigilante una sola vez para todas las registradas. Variables de entorno: `BI_REFRESCO_AUTOMATICO` (por defecto `true`) y `BI_REFRESCO_INTERVALO_SEG` (por defecto `900`).

> **Requisito 1:** la materializada necesita un índice **único** para admitir `CONCURRENTLY`.
> `CREATE UNIQUE INDEX <nombre>_pk ON <objeto> (<clave>);`
>
> **Requisito 2 — permisos:** `REFRESH MATERIALIZED VIEW` exige ser **dueño** del objeto. No basta con `INSERT`/`UPDATE`: no existe un privilegio de refresco por separado.

### 5bis.4.1 Dos roles: uno lee, otro mantiene

El tablero **consulta con un rol de solo lectura** (`cinf`, miembro de `cexp_read`, con `default_transaction_read_only = on`). Ese rol no puede refrescar: no es dueño de las materializadas.

Por eso hay un segundo juego de credenciales, solo para mantenimiento:

| Uso | Secreto | Rol |
|---|---|---|
| Consultas del tablero | `CAPA_PLATA_USER` / `CAPA_PLATA_PASSWORD` | `cinf` (solo lectura, con pool) |
| Refresco de materializadas | `CAPA_PLATA_USER_RW` / `CAPA_PLATA_PASSWORD_RW` | `admincinf` (dueño, sin pool) |

La conexión de escritura se obtiene con `get_plata_connection_rw()` y **queda deliberadamente fuera del pool**: se abre y se cierra en cada refresco, que es una operación esporádica. Así no hay riesgo de que una consulta del tablero acabe usando por accidente una conexión con permisos de escritura.

`hay_credenciales_escritura()` permite comprobar si están publicadas antes de intentar nada.

> Si esas credenciales faltan o el rol no es dueño del objeto, el refresco avisa una sola vez con el motivo y deja de intentarlo: el tablero sigue funcionando con los datos de la última carga. Consulte `estado_refresco()` para ver la situación.

> **Alternativa sin credenciales de escritura:** que el proceso que carga la tabla de origen ejecute `REFRESH MATERIALIZED VIEW CONCURRENTLY <objeto>;` al terminar, y desactivar el vigilante con `BI_REFRESCO_AUTOMATICO=false`. Es incluso preferible, porque refresca justo cuando hay datos nuevos en lugar de sondear.

### 5bis.5 El DDL vive junto al modelo

No hay archivos `.sql` sueltos. Cada modelo lleva su definición en la constante `DDL_CAPA_DATOS` y la función `crear_capa_datos()`, que crea o recrea la materializada, sus índices y su vista de consumo:

```python
from dashboard.model.kilometros.bi_kilometros_eliminados_no_ejecutados import crear_capa_datos
crear_capa_datos()   # ~30 s sobre un millón de filas
```

Es una operación **de despliegue**, no de uso diario: se ejecuta al instalar por primera vez o si cambia la estructura.

### 5bis.6 Consultas agrupadas, no una por gráfica

Contra la intuición, **una consulta por gráfica es más lenta**. Cada consulta paga el escaneo del período completo, y contra un servidor remoto cada viaje añade ~120 ms de red. Las 16 tarjetas de Kilómetros se resuelven en **4 consultas** (una por sección), reutilizando el mismo `WHERE`. Dividirlas en 16 costaría unas 4 veces más.

### 5bis.7 Cachear lo que es estable entre cargas

Los catálogos que llenan las listas de filtro son el bloque más caro (~11 s para 24 dimensiones) y ese costo es **irreducible** mientras las listas deban reflejar el período y los filtros activos. Se probaron `GROUPING SETS` (2× más lento), eliminar el CTE, `NOT MATERIALIZED`, subir `work_mem` y forzar paralelismo: ninguna mejoró.

La solución no es una consulta más lista, sino no repetirla:

- El caché usa **el sello de carga como parte de la clave**: cuando el ETL publica datos nuevos, se invalida solo. El TTL queda como red de seguridad, no como mecanismo principal.
- El arranque de la aplicación **precalienta** el caché en un hilo aparte, para que ningún usuario pague el cálculo en frío.

### 5bis.8 Diagnóstico

```python
from dashboard.database.refresco_bi import estado_refresco   # sellos, si está al día, duración
from dashboard.database.database_manager import get_pool_status  # pools y gestor de secretos
from dashboard.model.kilometros.bi_kilometros_eliminados_no_ejecutados import estado_cache
```

---

## 6. Cómo agregar o desarrollar una visualización

### Si la visualización ya existe en el menú (caso más común)

Ya está registrada en `config.py` y en `router.py`. Solo hay que trabajar dos archivos:

1. **`dashboard/model/{área}/bi_{nombre}.py`** — reemplazar el `return` de `consultar()` por la consulta real contra `get_plata_connection()` o `get_oro_connection()`.
2. **`dashboard/templates/charts/{área}/{nombre}.html`** — reemplazar el lienzo vacío por la gráfica real (`GE_BI.base({...})`), usando los `datos` que llegan del `fetch('/api/bi/{área}/{nombre}')`.

No hace falta tocar `config.py`, `router.py` ni ningún otro archivo del armazón.

### Si es una visualización totalmente nueva (no está en el menú) Se soliciata al Administrador del Proyecto que realice:

1. **Registrarla en `config.py`**: agregar su ícono en `ICONOS_GRAFICA`, su ruta en `RUTAS_GRAFICA` y su nombre en la lista del área correspondiente dentro de `AREAS_GRAFICAS` (respetando el orden alfabético).
2. **Crear el modelo** `dashboard/model/{área}/bi_{nombre}.py` con la función `consultar()`.
3. **Registrar el modelo** en `dashboard/model/__init__.py` (importarlo) y en `PROVEEDORES_DATOS` dentro de `dashboard/router.py`.
4. **Crear la plantilla** `dashboard/templates/charts/{área}/{nombre}.html`.
5. **Asignarla a un rol BI** en la pantalla de administración de roles (`/roles_business_intelligence`) para que algún usuario pueda verla.

### Si la visualización consulta muchos datos

Antes de dar por terminada una visualización, mida cuánto tarda. Si supera unos pocos segundos, revise la [sección 5bis](#5bis-rendimiento-vistas-materializadas-y-refresco) y aplique la lista de comprobación:

- [ ] ¿La vista de origen resuelve alguna dimensión con `LATERAL` o subconsulta correlacionada? → materialícela.
- [ ] ¿La materializada lleva solo las columnas que el tablero usa?
- [ ] ¿Tiene índice **único** (para `REFRESH CONCURRENTLY`) y un índice por `fecha`?
- [ ] ¿Está registrada con `registrar_materializada()` para que el vigilante la mantenga al día?
- [ ] ¿Las tarjetas se resuelven en consultas agrupadas por sección, en vez de una por gráfica?
- [ ] ¿Los catálogos de filtro se cachean usando el sello de carga en la clave?

---

## 7. Prompt estándar para trabajar con IA

Cuando se le pida a un asistente de IA (Claude, ChatGPT, Copilot, etc.) que desarrolle o edite una visualización del Dashboard, se recomienda darle primero este mensaje para que respete el armazón del módulo y no reinvente lo que ya existe:

```
Vas a trabajar en el módulo "dashboard" de este proyecto (Business Intelligence).
Este módulo separa "armazón" (ya construido, no se toca) de "lienzos" (donde se
desarrolla cada visualización). Reglas obligatorias:

1. NO modifiques dashboard/config.py, dashboard/router.py,
   dashboard/database/database_manager.py, dashboard/static/css/bi_dashboard.css
   ni dashboard/templates/_echarts_theme.html, salvo que te lo pida explícitamente.
   Esos archivos ya arman el menú, las rutas, la conexión a base de datos y el
   estilo visual compartido — no deben duplicarse ni reescribirse.

2. Trabaja únicamente en el par de archivos de la visualización que te indique:
   - Backend: dashboard/model/{área}/bi_{nombre}.py
   - Frontend: dashboard/templates/charts/{área}/{nombre}.html

3. En el backend, la función pública siempre se llama consultar() y devuelve
   un diccionario con la forma {"ok": True, "fuente": "...", "datos": [...]}.
   Usa get_plata_connection() o get_oro_connection() de
   dashboard.database.database_manager para consultar datos reales. No inventes
   datos de ejemplo (dummy) ni dejes números aleatorios en el resultado final.

4. En el frontend, usa siempre el tema visual compartido: inicializa la gráfica
   con echarts.init(...) y pinta las opciones con GE_BI.base({...}) (definido en
   _echarts_theme.html). Usa las clases CSS existentes con prefijo "bi-"
   (bi-tarjeta-grafica, bi-titulo-grafica, bi-fila-kpi, bi-chip-kpi, bi-lienzo,
   bi-lienzo-grande, bi-lienzo-pequeno) en vez de crear estilos nuevos, salvo que
   sea estrictamente necesario para esa gráfica puntual.

5. El frontend obtiene sus datos con fetch('/api/bi/{área}/{nombre}') — ese
   endpoint ya existe y llama automáticamente a consultar() en el backend. No
   crees rutas ni endpoints nuevos.

6. Si la visualización aún no existe en el menú, dime explícitamente que hace
   falta registrarla en config.py, router.py y model/__init__.py antes de
   continuar — no lo hagas por tu cuenta sin avisar.

Con esas reglas, concéntrate solo en construir la consulta real de datos y la
gráfica de la visualización que te pida.
```

Copiar y pegar este bloque (ajustando el nombre del área y la visualización) evita que la IA modifique por error el menú, las rutas o el estilo global, y mantiene todo el trabajo dentro de los dos archivos que le corresponden a cada gráfica.

---

## 8. Sistema de roles y permisos "Genrados por el Administrador del Proyecto"

El módulo tiene su propio sistema de permisos independiente del rol general de la aplicación. Cada usuario tiene un campo `rol_bi` en su perfil.

- **`rol_bi = 0`** → sin acceso al módulo, no ve el menú.
- **`rol_bi > 0`** → accede solo a las visualizaciones que el administrador le asignó, siempre que el rol esté activo.

Los roles se administran en `/roles_business_intelligence`. Cada rol tiene una lista de rutas permitidas (por ejemplo `["operadores/sigma", "kilometros/kilometros_objecion_sne"]`), que deben coincidir exactamente con los valores de `RUTAS_GRAFICA` en `config.py`.

Si un usuario intenta acceder directamente a la URL de un partial o de datos que no tiene asignado, el servidor devuelve `403 Forbidden`. La validación ocurre en el servidor, no solo en el menú.

---

## 9. Estilos y componentes visuales

Todos los componentes visuales están en `static/css/bi_dashboard.css` y se usan con clases prefijadas `.bi-`.

### Tarjeta de visualización

```html
<div class="bi-tarjeta-grafica">
    <div class="bi-encabezado-grafica">
        <div class="bi-titulo-grafica">
            <i class="bi bi-diagram-3-fill"></i> Título de la visualización
        </div>
        <div class="bi-fila-kpi">
            <!-- chips aquí -->
        </div>
    </div>
    <div id="lienzo-mi-grafica" class="bi-lienzo-grande"></div>
</div>
```

### Chips de estado / KPI

```html
<span class="bi-chip-kpi azul">   Total: 1.260 </span>
<span class="bi-chip-kpi verde">  Cumplimiento: 94,8% </span>
<span class="bi-chip-kpi naranja">Período: Jul 2026 </span>
<span class="bi-chip-kpi rojo">   En desarrollo </span>
```

### Tamaños de lienzo

```html
<div class="bi-lienzo">          <!-- altura estándar: 420px --></div>
<div class="bi-lienzo-grande">   <!-- altura grande: 520px   --></div>
<div class="bi-lienzo-pequeno">  <!-- altura pequeña: 300px  --></div>
```

### Cuadrícula de dos columnas (para poner dos visualizaciones lado a lado)

```html
<div class="bi-cuadricula-2">
    <div class="bi-tarjeta-grafica">...</div>
    <div class="bi-tarjeta-grafica">...</div>
</div>
```

En pantallas pequeñas (`< 860px`) las columnas colapsan automáticamente a una sola.

### Mensaje de error en el lienzo

```html
<div class="bi-error-grafica">
    <i class="bi bi-exclamation-triangle"></i>
    No fue posible cargar los datos.
</div>
```

### El objeto `GE_BI` — paleta y configuración compartida

`_echarts_theme.html` define un objeto JavaScript global llamado `GE_BI` que **todas las visualizaciones usan**:

- **`GE_BI.paleta`** — los colores corporativos en orden.
- **`GE_BI.colores`** — colores con nombre (`primario`, `acento`, `exito`, `advertencia`, `peligro`, `texto`...).
- **`GE_BI.cargando`** — configuración del spinner de carga (`grafica.showLoading(GE_BI.cargando)`).
- **`GE_BI.base(opciones)`** — combina las opciones propias de la gráfica con los valores por defecto (tooltip, leyenda, grilla, botón de descarga). **Siempre se usa esto** en vez de pasar opciones directas a ECharts.

```javascript
grafica.setOption(GE_BI.base({
    xAxis: { type: 'category', data: datos.meses },
    yAxis: { type: 'value' },
    series: [{ name: 'Ejecutados', type: 'bar', data: datos.ejecutados }],
}));
```

---

## 10. Preguntas frecuentes

**¿Puedo agregar una visualización sin tocar `router.py` ni `config.py`?**
No, pero solo se tocan **una vez**, al registrarla por primera vez. Después de eso, todo el desarrollo posterior de esa gráfica ocurre solo en su archivo de `model/` y su archivo de `templates/charts/`.

**¿Por qué hay dos rutas — `/bi/partial/` y `/api/bi/`?**
Son dos pasos separados. La primera devuelve el HTML (estructura + script). La segunda devuelve solo los datos en JSON. Así el HTML se puede cachear y los datos se actualizan de forma independiente.

**¿Puedo poner SQL directamente en el archivo de la visualización (el `.html`)?**
No. El SQL va siempre en el modelo (`model/{área}/bi_{nombre}.py`). El archivo `.html` solo llama a `fetch('/api/bi/...')` y recibe JSON. Esa separación permite cambiar la fuente de datos sin tocar el HTML.

**¿Cómo conecto una visualización a datos reales?**
Modifica solo el archivo del modelo (`model/{área}/bi_{nombre}.py`). Reemplaza el `return` de `consultar()` con tu consulta real usando `get_plata_connection()` o `get_oro_connection()` (ver sección 5). El HTML y el router no necesitan ningún cambio.

**¿Uso la misma conexión de base de datos que el resto del software?**
No. El Dashboard usa su propia conexión (`dashboard/database/database_manager.py`, capas Plata y Oro). El software principal usa otra completamente distinta (`database/database_manager.py`, en la raíz del proyecto). No se deben mezclar.

**¿Dónde va el CSS nuevo que solo usa una visualización?**
Si es un estilo muy específico, puede ir en un `<style>` dentro del propio archivo `{nombre}.html`. Si es un componente que usarán varias visualizaciones, va en `static/css/bi_dashboard.css`.

**¿Qué pasa si el nombre del área tiene tildes (ej. "Kilómetros")?**
Es solo la etiqueta visual que se ve en el menú. Las rutas internas (`RUTAS_GRAFICA`, carpetas de `model/` y `templates/charts/`) siempre usan minúsculas sin tildes (ej. `kilometros`). No hay conflicto.
