import io
import pandas as pd
from fastapi import HTTPException, UploadFile

# Columnas requeridas por hoja
_COLUMNAS_REQUERIDAS = {
    "TCZ":         ["cedula", "nombre"],
    "Supervisores":["cedula", "nombre"],
    "Turnos":      ["turno", "hora_inicio", "hora_fin", "detalles"],
    "Controles":   ["concesion", "puestos", "control", "ruta",
                    "linea", "admin", "cop", "tablas"],
}

def _formatear_hora(valor) -> str:
    """
    Normaliza un valor de hora a string 'HH:MM:SS'.
    Acepta datetime.time, timedelta, strings o NaN/None.
    """
    if pd.isna(valor) if not isinstance(valor, str) else False:
        return "00:00:00"
    if hasattr(valor, "strftime"):          # datetime.time
        return valor.strftime("%H:%M:%S")
    if hasattr(valor, "seconds"):           # timedelta
        total = int(valor.total_seconds())
        h, rem = divmod(total, 3600)
        m, s   = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"
    return str(valor).strip() or "00:00:00"

class ProcesarCargueControles:
    def __init__(self, file: UploadFile):
        self.file = file
        self.planta_data = None
        self.supervisores_data = None
        self.turnos_data = None
        self.controles_data = None
        self.procesamiento_preliminar = None

    # ─────────────────────────────────────────────────────────────────────
    # VALIDACIÓN DE ENCABEZADOS  (ahora realmente se usa)
    # ─────────────────────────────────────────────────────────────────────
    def validar_encabezados(self, df: pd.DataFrame, hoja: str):
        """
        Verifica que el DataFrame tenga todas las columnas requeridas para
        la hoja indicada. Lanza HTTP 400 con el nombre exacto de la columna
        faltante.
        """
        expected = _COLUMNAS_REQUERIDAS.get(hoja, [])
        columnas_actuales = [c.strip().lower() for c in df.columns.tolist()]
        for col in expected:
            if col.lower() not in columnas_actuales:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"La hoja '{hoja}' no tiene la columna requerida: '{col}'. "
                        f"Columnas encontradas: {df.columns.tolist()}"
                    ),
                )

    # ─────────────────────────────────────────────────────────────────────
    # LECTURA PRINCIPAL
    # ─────────────────────────────────────────────────────────────────────
    def leer_archivo(self) -> dict:
        """
        Lee el archivo Excel UNA sola vez en memoria y extrae las 4 hojas.
        Retorna el dict procesamiento_preliminar compatible con la versión
        anterior.
        """
        try:
            if not self.file.filename.endswith(".xlsx"):
                raise HTTPException(
                    status_code=400,
                    detail="Tipo de archivo inválido. Solo se permiten archivos .xlsx.",
                )

            # ── 1. Leer el stream completo en memoria (UNA sola vez) ──────
            contenido = self.file.file.read()
            buffer    = io.BytesIO(contenido)

            # ── 2. Abrir con ExcelFile para acceder a múltiples hojas ─────
            with pd.ExcelFile(buffer) as xls:
                hojas_disponibles = xls.sheet_names

                for hoja in _COLUMNAS_REQUERIDAS:
                    if hoja not in hojas_disponibles:
                        raise HTTPException(
                            status_code=400,
                            detail=(
                                f"La hoja '{hoja}' no existe en el archivo. "
                                f"Hojas disponibles: {hojas_disponibles}"
                            ),
                        )

                df_tcz         = pd.read_excel(xls, sheet_name="TCZ")
                df_supervisores= pd.read_excel(xls, sheet_name="Supervisores")
                df_turnos      = pd.read_excel(xls, sheet_name="Turnos")
                df_controles   = pd.read_excel(xls, sheet_name="Controles")

            # ── 3. Validar encabezados (método reutilizable) ──────────────
            self.validar_encabezados(df_tcz,          "TCZ")
            self.validar_encabezados(df_supervisores,  "Supervisores")
            self.validar_encabezados(df_turnos,        "Turnos")
            self.validar_encabezados(df_controles,     "Controles")

            # ── 4. Limpieza y casteo de tipos ─────────────────────────────

            # TCZ — cédula puede venir como int64 (ej: 12345678 → "12345678")
            df_tcz = df_tcz[["cedula", "nombre"]].dropna(subset=["cedula", "nombre"])
            df_tcz["cedula"] = df_tcz["cedula"].astype(str).str.strip()
            df_tcz["nombre"] = df_tcz["nombre"].astype(str).str.strip().str.upper()

            # Supervisores — igual que TCZ
            df_supervisores = df_supervisores[["cedula", "nombre"]].dropna(
                subset=["cedula", "nombre"]
            )
            df_supervisores["cedula"] = df_supervisores["cedula"].astype(str).str.strip()
            df_supervisores["nombre"] = (
                df_supervisores["nombre"].astype(str).str.strip().str.upper()
            )

            # Turnos — horas pueden venir como datetime.time, timedelta o str
            df_turnos = df_turnos[
                ["turno", "hora_inicio", "hora_fin", "detalles"]
            ].dropna(subset=["turno"])
            df_turnos["turno"]      = df_turnos["turno"].astype(str).str.strip()
            df_turnos["hora_inicio"]= df_turnos["hora_inicio"].apply(_formatear_hora)
            df_turnos["hora_fin"]   = df_turnos["hora_fin"].apply(_formatear_hora)
            df_turnos["detalles"]   = df_turnos["detalles"].fillna("").astype(str)

            # Controles — puestos puede venir como int
            cols_controles = ["concesion", "puestos", "control", "ruta",
                              "linea", "admin", "cop", "tablas"]
            df_controles = df_controles[cols_controles].dropna(
                subset=["concesion", "puestos", "control"]
            )
            df_controles["puestos"] = df_controles["puestos"].astype(str).str.strip()
            for col in ["concesion", "control", "ruta", "linea",
                        "admin", "cop", "tablas"]:
                df_controles[col] = (
                    df_controles[col].fillna("").astype(str).str.strip()
                )

            # ── 5. Convertir a listas de dicts ────────────────────────────
            self.planta_data        = df_tcz.to_dict(orient="records")
            self.supervisores_data  = df_supervisores.to_dict(orient="records")
            self.turnos_data        = df_turnos.to_dict(orient="records")
            self.controles_data     = df_controles.to_dict(orient="records")

            self.procesamiento_preliminar = {
                "planta":       self.planta_data,
                "supervisores": self.supervisores_data,
                "turnos":       self.turnos_data,
                "controles":    self.controles_data,
            }

            return self.procesamiento_preliminar

        except HTTPException:
            raise   # Re-lanzar errores HTTP ya formateados
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error al procesar el archivo: {str(e)}",
            )