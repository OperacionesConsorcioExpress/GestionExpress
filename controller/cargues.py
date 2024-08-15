import pandas as pd
from fastapi import HTTPException, UploadFile

class ProcesarCargueControles:
    def __init__(self, file: UploadFile):
        self.file = file
        self.planta_data = None
        self.controles_data = None
        self.procesamiento_preliminar = None

    def validar_encabezados(self, df, expected_columns):
        actual_columns = df.columns.tolist()
        for col in expected_columns:
            if col not in actual_columns:
                raise HTTPException(status_code=400, detail=f"El encabezado '{col}' falta en la hoja del archivo Excel.")

    def leer_archivo(self):
        try:
            if not self.file.filename.endswith('.xlsx'):
                raise HTTPException(status_code=400, detail="Tipo de archivo inválido. Solo se permiten archivos .xlsx.")

            # Leer datos del archivo Excel
            df_tcz = pd.read_excel(self.file.file, sheet_name='TCZ')
            df_controles = pd.read_excel(self.file.file, sheet_name='Controles')

            # Verificar columnas necesarias en cada hoja
            if 'cedula' not in df_tcz.columns or 'nombre' not in df_tcz.columns:
                raise HTTPException(status_code=400, detail="El archivo TCZ no tiene las columnas esperadas (cedula, nombre).")

            if 'concesion' not in df_controles.columns or 'puestos' not in df_controles.columns or 'control' not in df_controles.columns or 'ruta' not in df_controles.columns or 'linea' not in df_controles.columns or 'admin' not in df_controles.columns or 'cop' not in df_controles.columns or 'tablas' not in df_controles.columns:
                raise HTTPException(status_code=400, detail="El archivo Controles no tiene las columnas esperadas (concesion, puestos, control, ruta, linea, admin, cop, tablas).")

            # Convertir datos a diccionarios
            self.planta_data = df_tcz[['cedula', 'nombre']].to_dict(orient='records')
            self.controles_data = df_controles[['concesion', 'puestos', 'control', 'ruta', 'linea', 'admin', 'cop', 'tablas']].to_dict(orient='records')

            # Preparar estructura preliminar
            self.procesamiento_preliminar = {
                "planta": self.planta_data,
                "controles": self.controles_data
            }
            
            return self.procesamiento_preliminar

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error al procesar el archivo: {str(e)}")
