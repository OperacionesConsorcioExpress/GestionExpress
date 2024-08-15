"""
Planeación - CONSORCIO EXPRESS S.A.S
Título: [Procesamiento Automático tiempos de ciclo]
Autor: Maximiliano Andres Bernal Giraldo
"""
import sys
import os
import pandas as pd
# from PyQt6 import QtWidgets, QtCore, QtGui
# from PyQt6.QtCore import Qt
# from PyQt6.QtGui import QPixmap, QFont, QIcon
# from PyQt6.QtWidgets import QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, QPushButton, QMessageBox, QMainWindow, QInputDialog, QFileDialog, QComboBox, QRadioButton
from azure.storage.blob import BlobServiceClient, BlobClient
import tempfile
import io
from io import BytesIO
import shutil
import time
import numpy as np
import openpyxl

# ---Funciones---
def obtener_lista_contenedores(conexion_str: str) -> list:
    # Obtener la cadena de conexión
    blob_service_client = BlobServiceClient.from_connection_string(
        conexion_str)

    # Se obtiene la lista de contenedores
    lista_contenedores = blob_service_client.list_containers()
    return lista_contenedores


def verificar_ubicacion(ruta: str):
    # el if identifica si la carpeta existe, si no es así la crea automáticamente
    if not os.path.exists(ruta):
        os.makedirs(ruta)
    # en caso contrario la borra y la vuelve a crear
    else:
        shutil.rmtree(ruta)
        time.sleep(10)
        os.makedirs(ruta)


def extraer_archivos_ActividadBus(conexion_str: str, fechas: list, zonas: list, Carpeta_Local: str):
    # Establecer la conexión al Blob
    blob_service_client = BlobServiceClient.from_connection_string(
        conexion_str)
    # Recorrer todas las fechas deseadas y en cada una pedir el archivo de cada zona (for s anidados)

    for fecha in fechas:
        for zona in zonas.keys():
            # los archivos pueden estar en dos carpetas, se buscara entonces en orden, si no se encuentra en la primera ubicación se ira a la segunda
            primera_ubicacion = f"{fecha}/ZN/Otras_Descargas/actividad_bus/{zonas[zona]}/Informe_Diario_ActividadBus_{fecha}_{zona}.csv"
            segunda_ubicacion = f"{fecha[6:]}/{fecha[3:5]}/{fecha}/ZN/Otras_Descargas/actividad_bus/{zonas[zona]}/Informe_Diario_ActividadBus_{fecha}_{zona}.csv"
            try:
                # Descargar el archivo desde blob en Azure a la ruta de destino por Zona y Fecha
                blob_client = blob_service_client.get_blob_client(
                    container="robotmsacexp", blob=primera_ubicacion)
                with open(f"{Carpeta_Local}/Informe_Diario_ActividadBus_{fecha}_{zona}.csv", "wb") as file:
                    blob_stream = blob_client.download_blob()
                    file.write(blob_stream.readall())
            except Exception as e:
                print(f'el archivo no se encuentra en la ubicación: {primera_ubicacion}: {str(e)}')
                print(f'Intentando en la siguiente ubicación: {segunda_ubicacion}')
                # Descargar el archivo desde blob en Azure a la ruta de destino por Zona y Fecha
                blob_client = blob_service_client.get_blob_client(
                    container="robotmsacexp", blob=segunda_ubicacion)
                with open(f"{Carpeta_Local}/Informe_Diario_ActividadBus_{fecha}_{zona}.csv", "wb") as file:
                    blob_stream = blob_client.download_blob()
                    file.write(blob_stream.readall())
            except Exception as ex:
                print(f'el archivo no se encuentra en ninguna de las dos ubicaciones esperadas, por favor revisar el blob en azure o consultar con centro de información: {str(ex)}')

def extraer_archivos_ViajesDesglosado(conexion_str: str, fechas: list, zonas: list, Carpeta_Local: str):
    # Establecer la conexión al Blob
    blob_service_client = BlobServiceClient.from_connection_string(
        conexion_str)
    # Recorrer todas las fechas deseadas y en cada una pedir el archivo de cada zona (for s anidados)

    for fecha in fechas:
        for zona in zonas.keys():
            # los archivos pueden estar en dos carpetas, se buscara entonces en orden, si no se encuentra en la primera ubicación se ira a la segunda
            primera_ubicacion = f"{fecha}/ZN/Otras_Descargas/viajes_desglosado/{zonas[zona]}/VD_{fecha}_Zonal_{zona}.csv"
            segunda_ubicacion = f"{fecha[6:]}/{fecha[3:5]}/{fecha}/ZN/Otras_Descargas/viajes_desglosado/{zonas[zona]}/VD_{fecha}_Zonal_{zona}.csv"
            try:
                # Descargar el archivo desde blob en Azure a la ruta de destino por Zona y Fecha
                blob_client = blob_service_client.get_blob_client(
                    container="robotmsacexp", blob=primera_ubicacion)
                with open(f"{Carpeta_Local}/VD_{fecha}_Zonal_{zona}.csv", "wb") as file:
                    blob_stream = blob_client.download_blob()
                    file.write(blob_stream.readall())
            except Exception as e:
                print(f'el archivo no se encuentra en la ubicación: {primera_ubicacion}: {str(e)}')
                print(f'Intentando en la siguiente ubicación: {segunda_ubicacion}')
                # Descargar el archivo desde blob en Azure a la ruta de destino por Zona y Fecha
                blob_client = blob_service_client.get_blob_client(
                    container="robotmsacexp", blob=segunda_ubicacion)
                with open(f"{Carpeta_Local}/VD_{fecha}_Zonal_{zona}.csv", "wb") as file:
                    blob_stream = blob_client.download_blob()
                    file.write(blob_stream.readall())
            except Exception as ex:
                print(f'el archivo no se encuentra en ninguna de las dos ubicaciones esperadas, por favor revisar el blob en azure o consultar con centro de información: {str(ex)}')

def Estadisticos(datos:list,extension:float, franja:int)->dict:
    # Calcular rango intercuartil
    q1 = np.percentile(datos, 25)
    q3 = np.percentile(datos, 75)
    ric = q3 -q1
    # Calcular limites, Bigotes
    limite_inferior = q1 - (extension * ric)
    limite_superior = q3 + (extension * ric)
    #Calcular percentiles 80, 85, 90)
    per80 = np.percentile(datos, 80)
    per85 = np.percentile(datos, 85)
    per90 = np.percentile(datos, 90)
    # calcular media y mediana
    mediana = np.percentile(datos, 50)
    rango_limpio = [i for i in datos if ((i > limite_inferior) & (i < limite_superior))]
    media = np.mean(rango_limpio)
    #Calcular Desviación Estandar
    desvStan = np.std(datos)
    #Contar con cuandos datos se calculan estos valores
    cuenta = len(datos)

    dic_estadisticos = {"Franja":franja,"LimInf":limite_inferior, "cuartil1":q1, "mediana":mediana,"Promedio":media, "cuartil3":q3,"per80":per80,"per85":per85,"per90":per90, "LimSup":limite_superior, "Desviasion_Standar":desvStan, "cuenta":cuenta}

    return dic_estadisticos

# ---Parámetros---
# Parametros de conexión, no mover
Parametros_conexion = {"DefaultEndpointsProtocol": "https",
                       "AccountName": "datacentroinformacion",
                       "AccountKey": "TCvIM//bTsungQ7zughV6fAYK6aZg2ENVpJC54QyF2eNIL4d3gP7xJpIJd851idwuQbB9MTZRzw1oQuSx3lptA==",
                       "BlobEndpoint": "https://datacentroinformacion.blob.core.windows.net/",
                       "QueueEndpoint": "https://datacentroinformacion.queue.core.windows.net/",
                       "TableEndpoint": "https://datacentroinformacion.table.core.windows.net/",
                       "FileEndpoint": "https://datacentroinformacion.file.core.windows.net/"}

# Cadema de texto de los parámetros de conexión
conexion_str = "DefaultEndpointsProtocol={};AccountName={};AccountKey={};BlobEndpoint={};QueueEndpoint={};TableEndpoint={};FileEndpoint={};".format(Parametros_conexion["DefaultEndpointsProtocol"], Parametros_conexion["AccountName"], Parametros_conexion["AccountKey"],
                                                                                                                                                    Parametros_conexion["BlobEndpoint"], Parametros_conexion["QueueEndpoint"], Parametros_conexion["TableEndpoint"],
                                                                                                                                                    Parametros_conexion["FileEndpoint"])

# Carpeta en local donde se guardarán los archivos que se descarguen
Carpeta_Temporal = r"C:\0001_Planeacion\__Pruebas_Descarga_Azure"

# Las fechas que se desean descargar
fechas = ["29_04_2024", "30_04_2024", "01_03_2024","02_05_2024", "03_05_2024","06_05_2024", "07_05_2024", "08_03_2024","09_05_2024", "10_05_2024","13_05_2024", "14_05_2024", "15_03_2024","16_05_2024", "17_05_2024"]

# Las zonas que se desean descargar
zonas = {"US": "usaquen", "SC": "san_cristobal"}

#Franjas Horarias
Franjas_Horarias = {0:("00:00:00","00:59:59"),1:("01:00:00","01:59:59"),2:("02:00:00","02:59:59"),
                    3:("03:00:00","03:59:59"),4:("04:00:00","04:59:59"),5:("05:00:00","05:59:59"),
                    6:("06:00:00","06:59:59"),7:("07:00:00","07:59:59"),8:("08:00:00","08:59:59"),
                    9:("09:00:00","09:59:59"),10:("10:00:00","10:59:59"),11:("11:00:00","11:59:59"),
                    12:("12:00:00","12:59:59"),13:("13:00:00","13:59:59"),14:("14:00:00","14:59:59"),
                    15:("15:00:00","15:59:59"),16:("16:00:00","16:59:59"),17:("17:00:00","17:59:59"),
                    18:("18:00:00","18:59:59"),19:("19:00:00","19:59:59"),20:("20:00:00","20:59:59"),
                    21:("21:00:00","21:59:59"),22:("22:00:00","22:59:59"),23:("23:00:00","23:59:59")} 

#Parámetro para estimar la extensión de la distribución desde la cual todo lo que quede fuera del rango se considerará atipico
Extension_Bigotes = 1

#Carpeta local en la que se exportarán los exceles de resultados
Carpeta_Resultados = r"C:\0001_Planeacion\__Pruebas_Descarga_Azure"


# ---Flujo---
# Extraer datos desde Azure a Local
#extraer_archivos_ActividadBus(conexion_str, fechas, zonas, Carpeta_Temporal)
extraer_archivos_ViajesDesglosado(conexion_str, fechas, zonas, Carpeta_Temporal)

# Cargar los datos a dos Dataframes, uno por cada zona
for zona in zonas.keys():
    dfs = []
    for fecha in fechas:
        #esta línea de código se puede modificar para en caso de requerirse cargar la actividad bus
        df =  pd.read_csv(os.path.join(Carpeta_Temporal,f'VD_{fecha}_Zonal_{zona}.csv'),sep=",") #f"Informe_Diario_ActividadBus_{fecha}_{zona}.csv"),sep=",")
        dfs.append(df)
    if zona == "US":
        df_US = pd.concat(dfs,ignore_index=True)
        df_US.reset_index(drop=True, inplace= True)
        # Converir la columna Fecha a formato datetime
        df_US['Fecha'] = pd.to_datetime(df_US['Fecha'])
    elif zona == "SC":
        df_SC = pd.concat(dfs,ignore_index=True)
        df_SC.reset_index(drop=True, inplace= True)
        # Converir la columna Fecha a formato datetime
        df_SC['Fecha'] = pd.to_datetime(df_SC['Fecha'])
    
#en caso de querer ver una muestra de la tabla para conocer la estructura de los datos se puede usar el siguiente código en la ventana Interactiva:
# pd.set_option('display.max_columns', None)
# df_US.head()

rutas_US =  df_US['Linea'].unique()
rutas_SC =  df_SC['Linea'].unique()

Tiempos_ciclo_US ={}
for ruta in rutas_US:
    #Filtrar por la ruta especificada
    df_ruta = df_US.loc[df_US['Linea'] == ruta]
    #Descartar los datos vacios en Hora real de despacho y en la duración real
    df_ruta.dropna(subset = 'DurReal',inplace=True)
    df_ruta.dropna(subset = 'DHoraReal',inplace=True)
    #Convertir las columnas desde su formato de texto a horas con el fin de poder filtrar
    colAModificarHora = ['DHoraReal','DHoraRealFin']
    for col in colAModificarHora:
        df_ruta[col] = pd.to_datetime(df_ruta[col], format='%H:%M:%S')
    #Convertir las columnas desde su formato a valores en minutos en nuevas columnas
    colAModificarMins =['DurRef','DurReal']
    for col in colAModificarMins:
        df_ruta[col] = pd.to_datetime(df_ruta[col],format='%H:%M:%S')
        df_ruta[col+'Mins'] = df_ruta[col].dt.hour * 60 + df_ruta[col].dt.minute+ df_ruta[col].dt.second / 60

    #Filtrar por la franja horaria y calcular estadisticos
    lista_estadisticos_por_franja = [] 
    for franja in Franjas_Horarias.keys():
        iniFranja = pd.to_datetime("1900-01-01 " + Franjas_Horarias[franja][0])
        finFranja = pd.to_datetime("1900-01-01 "+ Franjas_Horarias[franja][1])
        val_tablas_franja = df_ruta.loc[(df_ruta['DHoraReal'] >= iniFranja) & (df_ruta['DHoraReal'] <= finFranja),'DurRealMins'].values
        if val_tablas_franja.size == 0:
            df_franja = {"Franja":franja,"LimInf":0, "cuartil1":0, "mediana":0,"Promedio":0, "cuartil3":0, "per80":0,"per85":0,"per90":0, "LimSup":0, "Desviasion_Standar":0, "cuenta":0}
        else:
            df_franja = Estadisticos(val_tablas_franja,Extension_Bigotes,franja)
        
        #guardar diccionarios de estadisticos por franja en una lista
        lista_estadisticos_por_franja.append(df_franja)
    #Convertir la lista en un dataframe, alistando la exportación a excel
    df_estadisticos_Ruta_Franja = pd.DataFrame(lista_estadisticos_por_franja)
    Tiempos_ciclo_US.update({ruta:df_estadisticos_Ruta_Franja})
#Exportar a Excel
with pd.ExcelWriter(f'{Carpeta_Resultados}\Tiempos_ciclo_US.xlsx')as writer:
    for ruta in Tiempos_ciclo_US.keys():
        nombre = ruta[ruta.find(" ")+1:]
        codigo_SAE = ruta[ruta.find("[")+1:ruta.find("]")]
        Tiempos_ciclo_US[ruta].to_excel(writer,sheet_name=str(nombre+"_"+codigo_SAE),index=False)



print("finalizado US")
print(str(df_US.shape))


Tiempos_ciclo_SC ={}
for ruta in rutas_SC:
    #Filtrar por la ruta especificada
    df_ruta = df_SC.loc[df_SC['Linea'] == ruta]
    #Descartar los datos vacios en Hora real de despacho y en la duración real
    df_ruta.dropna(subset = 'DurReal',inplace=True)
    df_ruta.dropna(subset = 'DHoraReal',inplace=True)
    #Convertir las columnas desde su formato de texto a horas con el fin de poder filtrar
    colAModificarHora = ['DHoraReal','DHoraRealFin']
    for col in colAModificarHora:
        df_ruta[col] = pd.to_datetime(df_ruta[col], format='%H:%M:%S')
    #Convertir las columnas desde su formato a valores en minutos en nuevas columnas
    colAModificarMins =['DurRef','DurReal']
    for col in colAModificarMins:
        df_ruta[col] = pd.to_datetime(df_ruta[col],format='%H:%M:%S')
        df_ruta[col+'Mins'] = df_ruta[col].dt.hour * 60 + df_ruta[col].dt.minute+ df_ruta[col].dt.second / 60

    #Filtrar por la franja horaria y calcular estadisticos
    lista_estadisticos_por_franja = [] 
    for franja in Franjas_Horarias.keys():
        iniFranja = pd.to_datetime("1900-01-01 " + Franjas_Horarias[franja][0])
        finFranja = pd.to_datetime("1900-01-01 "+ Franjas_Horarias[franja][1])
        val_tablas_franja = df_ruta.loc[(df_ruta['DHoraReal'] >= iniFranja) & (df_ruta['DHoraReal'] <= finFranja),'DurRealMins'].values
        if val_tablas_franja.size == 0:
            df_franja = {"Franja":franja,"LimInf":0, "cuartil1":0, "mediana":0,"Promedio":0, "cuartil3":0, "per80":0,"per85":0,"per90":0, "LimSup":0,"Desviasion_Standar":0, "cuenta":0}
        else:
            df_franja = Estadisticos(val_tablas_franja,Extension_Bigotes,franja)
        
        #guardar diccionarios de estadisticos por franja en una lista
        lista_estadisticos_por_franja.append(df_franja)
    #Convertir la lista en un dataframe, alistando la exportación a excel
    df_estadisticos_Ruta_Franja = pd.DataFrame(lista_estadisticos_por_franja)
    Tiempos_ciclo_SC.update({ruta:df_estadisticos_Ruta_Franja})

with pd.ExcelWriter(f'{Carpeta_Resultados}\Tiempos_ciclo_SC.xlsx')as writer:
    for ruta in Tiempos_ciclo_SC.keys():
        nombre = ruta[ruta.find(" ")+1:]
        codigo_SAE = ruta[ruta.find("[")+1:ruta.find("]")]
        Tiempos_ciclo_SC[ruta].to_excel(writer,sheet_name=str(nombre+"_"+codigo_SAE),index=False)


print("finalizado SC")
print(str(df_SC.shape))


#ideas:
#me gustaria contar cuantos viajes estuvieron por encima y cuantos por abajo del tiempo de referencia dado que tengo ese valor y el tiempo real, no solamente 
#es ver que los tiempos cuadren sino los viajes que cumplen ese tiempo o menos, saldría una buena estadistica de Ahí