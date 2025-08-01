'''
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from fastapi import Body
from pydantic import BaseModel
from typing import List
import os, io
import traceback
from lib.NPL_chatbot import CargaPDF , Chunks, Embeddings, FAISSIndex

npl_router = APIRouter()

@npl_router.get("/listar_pdfs", response_class=JSONResponse)
async def listar_archivos_pdf(request: Request):
    loader = CargaPDF()
    lista_pdf = loader.listar_pdfs()
    resultado = []

    for ruta_pdf in lista_pdf:
        nombre_archivo = ruta_pdf.split("/")[-1].replace(".pdf", ".json")
        ruta_chunk = f"chunks/{nombre_archivo}"
        existe_chunk = loader.existe_blob(ruta_chunk)
        resultado.append({
            "ruta_pdf": ruta_pdf,
            "procesado": existe_chunk
        })

    return {"pdfs": resultado}

class RutasPDF(BaseModel):
    rutas_pdf: List[str]
    
@npl_router.get("/listar_chunks", response_class=JSONResponse)
async def listar_chunks():
    loader = CargaPDF()
    blobs = loader.contenedor.list_blobs(name_starts_with="chunks/")
    archivos = [b.name for b in blobs if b.name.endswith(".json")]
    return {"chunks": archivos}
    
@npl_router.post("/procesar_chunks", response_class=JSONResponse)
async def procesar_chunks(datos: RutasPDF):
    rutas = datos.rutas_pdf
    if not rutas:
        return JSONResponse(content={"error": "No se enviaron rutas"}, status_code=400)

    loader = CargaPDF()
    divisor = Chunks(loader.blob_service, loader.contenedor)

    resultado = []
    for ruta in rutas:
        try:
            texto = loader.extraer_texto(ruta)
            fragmentos = divisor.dividir_texto(texto)
            divisor.guardar_chunks(ruta, fragmentos)
            resultado.append({"ruta": ruta, "chunks": len(fragmentos), "estado": "ok"})
        except Exception as e:
            resultado.append({"ruta": ruta, "error": str(e), "estado": "error"})

    return {"resultado": resultado}

@npl_router.get("/listar_embeddings", response_class=JSONResponse)
async def listar_embeddings(request: Request):
    loader = CargaPDF()
    blobs = loader.contenedor.list_blobs(name_starts_with="embeddings/")
    archivos = [b.name for b in blobs if b.name.endswith(".npy")]
    return {"embeddings": archivos}

@npl_router.post("/procesar_embeddings", response_class=JSONResponse)
async def procesar_embeddings(datos: RutasPDF):
    rutas = datos.rutas_pdf
    loader = CargaPDF()
    encoder = Embeddings(loader.blob_service, loader.contenedor)

    resultado = []
    for ruta in rutas:
        try:
            nombre_chunk = os.path.basename(ruta).replace(".pdf", ".json")
            ruta_chunk = f"chunks/{nombre_chunk}"

            print(f"🔍 Verificando existencia del chunk: {ruta_chunk}")

            if not loader.existe_blob(ruta_chunk):
                raise Exception("❌ No existe el archivo de chunks")

            cantidad = encoder.procesar_desde_chunks(ruta_chunk)
            resultado.append({"ruta": ruta, "embeddings": cantidad, "estado": "ok"})
        except Exception as e:
            resultado.append({"ruta": ruta, "error": str(e), "estado": "error"})

    return {"resultado": resultado}

@npl_router.get("/listar_faiss_index", response_class=JSONResponse)
async def listar_faiss_index(request: Request):
    loader = CargaPDF()
    blobs = loader.contenedor.list_blobs(name_starts_with="faiss_index/")
    lista = [b.name for b in blobs if b.name.endswith(".index")]
    return {"indices": lista}

@npl_router.post("/procesar_faiss_index", response_class=JSONResponse)
async def procesar_faiss_index(datos: RutasPDF):
    rutas = datos.rutas_pdf
    if not rutas:
        return JSONResponse(content={"error": "No se enviaron rutas"}, status_code=400)

    loader = CargaPDF()
    indexador = FAISSIndex(loader.blob_service, loader.contenedor)

    resultado = []
    for ruta in rutas:
        try:
            cantidad = indexador.procesar_desde_embeddings(ruta)

            # 🔎 Verifica tipo del valor retornado
            print(f"🧪 Resultado de FAISS para {ruta}: {cantidad} ({type(cantidad)})")

            resultado.append({
                "ruta": ruta,
                "vectores": int(cantidad),
                "estado": "ok"
            })
        except Exception as e:
            tb = traceback.format_exc()
            print(f"🛑 Error procesando FAISS index para {ruta}:\n{tb}")
            error_msg = f"{type(e).__name__}: {str(e)}"
            resultado.append({"ruta": ruta, "error": error_msg, "estado": "error"})

    return {"resultado": resultado}
'''