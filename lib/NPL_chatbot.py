'''
from azure.storage.blob import BlobServiceClient
from PyPDF2 import PdfReader
from sentence_transformers import SentenceTransformer
from huggingface_hub import login
import numpy as np
import faiss
from dotenv import load_dotenv
import json, os, io
from typing import List

# Cargar variables de entorno
load_dotenv()
CONEXION_BLOB = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
NOMBRE_CONTENEDOR = "1041-operaciones-cinf-conocimiento-chatbot"

class CargaPDF:
    def __init__(self):
        self.blob_service = BlobServiceClient.from_connection_string(CONEXION_BLOB)
        self.contenedor = self.blob_service.get_container_client(NOMBRE_CONTENEDOR)

    def listar_pdfs(self):
        """
        Lista los archivos PDF dentro de la carpeta 'pdf/', excluyendo 'pdf/temp/'.
        """
        blobs = self.contenedor.list_blobs(name_starts_with="pdf/")
        lista_pdfs = [b.name for b in blobs if b.name.endswith(".pdf") and not b.name.startswith("pdf/temp/")]
        return lista_pdfs
    
    def existe_blob(self, ruta_blob):
        try:
            self.contenedor.get_blob_client(ruta_blob).get_blob_properties()
            return True
        except Exception:
            return False

    def extraer_texto(self, ruta_blob):
        """
        Extrae texto del PDF almacenado en Azure Blob Storage.
        """
        print(f"📄 Leyendo: {ruta_blob}")
        blob_cliente = self.contenedor.get_blob_client(ruta_blob)
        contenido = blob_cliente.download_blob().readall()

        texto_extraido = ""
        with io.BytesIO(contenido) as archivo_pdf:
            lector = PdfReader(archivo_pdf)
            for pagina in lector.pages:
                texto_extraido += pagina.extract_text() or ""

        return texto_extraido
    
class Chunks:
    def __init__(self, blob_service, contenedor, chunk_size=800, overlap=100):
        self.blob_service = blob_service
        self.contenedor = contenedor 
        self.chunk_size = chunk_size # Cantidad máxima de caracteres por fragmento
        self.overlap = overlap # Cantidad de caracteres que se superponen entre fragmentos

    def dividir_texto(self, texto: str) -> List[str]: # Divide un texto largo en fragmentos solapados de tamaño limitado.
        
        if not texto or len(texto.strip()) < 100:
            print("⚠️ Texto insuficiente para generar chunks.")
            return []
        
        chunks = []
        start = 0
        while start < len(texto):
            end = start + self.chunk_size
            fragmento = texto[start:end].strip()
            if len(fragmento) > 100:  # mínimo para guardar
                chunks.append(fragmento)
            start += self.chunk_size - self.overlap
        print(f"📦 Fragmentos generados: {len(chunks)}")
        return chunks

    def guardar_chunks(self, ruta_pdf: str, chunks: List[str]): # Guarda los fragmentos en Azure Blob Storage.
        
        if not chunks:
            raise ValueError("❌ No se pueden guardar chunks vacíos.")
        
        nombre_base = os.path.basename(ruta_pdf).replace(".pdf", ".json")
        ruta_destino = f"chunks/{nombre_base}"

        contenido_json = {
            "ruta_pdf": ruta_pdf,
            "chunks": chunks
        }

        contenido_bytes = json.dumps(contenido_json, ensure_ascii=False).encode("utf-8")
        blob_cliente = self.contenedor.get_blob_client(ruta_destino)
        blob_cliente.upload_blob(contenido_bytes, overwrite=True)
        print(f"✅ Chunks guardados en: {ruta_destino} ({len(chunks)} fragmentos)")
        
class Embeddings:
    def __init__(self, blob_service, contenedor, modelo="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"):
        self.blob_service = blob_service
        self.contenedor = contenedor
        self.modelo_nombre = modelo  # Modelo a usar
        self._modelo = None  # Lazy loading: el modelo se cargará solo si se necesita

        # Autenticación en Hugging Face
        hf_token = os.getenv("HUGGINGFACE_API_KEY")
        if not hf_token:
            raise ValueError("❌ No se encontró la variable HUGGINGFACE_API_KEY en el entorno.")
        login(hf_token)

    def _get_modelo(self):
        """Carga el modelo de embeddings solo cuando se requiere."""
        if self._modelo is None:
            print(f"🔁 Cargando modelo de embeddings: {self.modelo_nombre}")
            from sentence_transformers import SentenceTransformer
            self._modelo = SentenceTransformer(self.modelo_nombre)
        return self._modelo

    def procesar_desde_chunks(self, ruta_chunk: str):
        blob_chunk = self.contenedor.get_blob_client(ruta_chunk)
        datos = json.loads(blob_chunk.download_blob().readall())

        textos = datos["chunks"]
        nombre_base = os.path.basename(ruta_chunk).replace(".json", "")
        ruta_npy = f"embeddings/{nombre_base}.npy"
        ruta_meta = f"embeddings/{nombre_base}_metadata.json"

        # Usar modelo con carga diferida
        vectores = self._get_modelo().encode(textos, show_progress_bar=False)

        # Guardar embeddings .npy
        buffer = io.BytesIO()
        np.save(buffer, vectores)
        buffer.seek(0)
        self.contenedor.get_blob_client(ruta_npy).upload_blob(buffer, overwrite=True)

        # Guardar metadata
        contenido_meta = json.dumps({"texts": textos}, ensure_ascii=False).encode("utf-8")
        self.contenedor.get_blob_client(ruta_meta).upload_blob(contenido_meta, overwrite=True)

        print(f"✅ Embeddings guardados: {ruta_npy} / {ruta_meta}")
        return len(vectores)
    
class FAISSIndex: #Facebook AI Similarity Search
    def __init__(self, blob_service, contenedor, dim=384):
        self.blob_service = blob_service
        self.contenedor = contenedor
        self.dim = dim  # Dimensión del embedding del modelo utilizado

    def procesar_desde_embeddings(self, ruta_pdf):
        nombre_base = os.path.basename(ruta_pdf).replace(".pdf", "")
        ruta_embeddings = f"embeddings/{nombre_base}.npy"
        ruta_metadata = f"embeddings/{nombre_base}_metadata.json"

        print(f"🔍 Verificando embeddings: {ruta_embeddings}")
        try:
            self.contenedor.get_blob_client(ruta_embeddings).get_blob_properties()
        except Exception:
            raise FileNotFoundError(f"❌ Embeddings no encontrados: {ruta_embeddings}")

        # Descargar embeddings y metadatos
        vec = self.contenedor.get_blob_client(ruta_embeddings).download_blob().readall()
        metadatos = self.contenedor.get_blob_client(ruta_metadata).download_blob().readall()

        # Convertir a arreglo numpy
        try:
            arr = np.load(io.BytesIO(vec))
        except Exception as e:
            raise Exception(f"❌ Error al cargar los embeddings: {e}")

        print("ℹ️ Tipo de objeto cargado:", type(arr))
        print("ℹ️ Dimensiones del arreglo:", getattr(arr, 'shape', 'No disponible'))
        print("ℹ️ dtype:", getattr(arr, 'dtype', 'No disponible'))

        if not isinstance(arr, np.ndarray):
            raise TypeError("❌ El objeto cargado no es un arreglo numpy")
        
        if arr.size == 0:
            raise ValueError("❌ El arreglo de embeddings está vacío")

        if arr.ndim != 2:
            raise ValueError(f"❌ Se esperaba un arreglo 2D, pero se recibió uno de {arr.ndim} dimensiones.")

        if arr.shape[1] != self.dim:
            raise ValueError(f"❌ Dimensiones incorrectas: se esperaban vectores de dimensión {self.dim}, pero se recibió shape {arr.shape}")

        if arr.dtype != np.float32:
            print(f"⚠️ Convirtiendo de {arr.dtype} a float32 (puede perder precisión)")
            arr_orig = arr.copy()
            arr = arr.astype(np.float32)
            diff = np.max(np.abs(arr - arr_orig.astype(np.float32)))
            print(f"ℹ️ Diferencia máxima tras conversión: {diff:.6f}")

        # Cargar los textos
        textos_json = json.loads(metadatos)
        textos = textos_json.get("texts")
        if textos is None:
            textos = textos_json.get("chunks")

        if not isinstance(textos, list):
            raise TypeError("❌ Los metadatos no contienen una lista válida de textos")

        if len(textos) != arr.shape[0]:
            raise ValueError(f"❌ El número de textos ({len(textos)}) no coincide con la cantidad de vectores ({arr.shape[0]})")

        if arr.shape[0] > 0:
            primer_vector = arr[0]
            print("▶️ Primer vector:", [round(v, 4) for v in primer_vector[:10]])

        print(f"📊 Generando índice FAISS para {arr.shape[0]} vectores de dimensión {arr.shape[1]}")

        # Validación adicional: norma cero
        normas = np.linalg.norm(arr, axis=1)
        if np.any(normas == 0):
            indices_invalidos = np.where(normas == 0)[0]
            textos_invalidos = [textos[i] for i in indices_invalidos]
            print("⚠️ Textos con norma cero:")
            for i, texto in enumerate(textos_invalidos[:5]):
                print(f"• #{indices_invalidos[i]} → '{texto[:100].strip()}'...")
            if len(textos_invalidos) > 5:
                print(f"... y {len(textos_invalidos) - 5} más con norma cero.")
            raise ValueError("❌ Algunos vectores tienen norma cero (probablemente texto vacío o redundante)")

        if not np.isfinite(arr).all():
            raise ValueError("❌ Los embeddings contienen valores inválidos (NaN o Inf)")

        print("🔎 FAISS INPUT SUMMARY:")
        print("→ dtype:", arr.dtype)
        print("→ shape:", arr.shape)
        print("→ min:", np.min(arr), "| max:", np.max(arr))
        print("→ contiene NaN:", np.isnan(arr).any())
        print("→ contiene Inf:", np.isinf(arr).any())

        # Crear índice FAISS
        index = faiss.IndexFlatL2(self.dim)
        arr = np.array(arr, dtype=np.float32, copy=True)
        index.add(arr)

        # Serializar y guardar en Blob Storage
        try:
            index_bytes = faiss.serialize_index(index)
            if index_bytes is None:
                raise Exception("FAISS devolvió None")
            buffer = io.BytesIO(index_bytes)
        except Exception as e:
            raise Exception(f"❌ Error al serializar el índice FAISS: {e}")

        # Guardar índice en Blob Storage
        ruta_index = f"faiss_index/{nombre_base}.index"
        
        print(f"📝 Index bytes longitud: {len(index_bytes)}")
        print(f"📝 Primeros 20 bytes: {index_bytes[:20]}")

        try:
            self.contenedor.get_blob_client(ruta_index).upload_blob(buffer, overwrite=True)
            print(f"✅ FAISS Index subido: {ruta_index} ({len(index_bytes)} bytes)")
        except Exception as e:
            raise Exception(f"❌ Error al subir el índice FAISS a Azure Blob: {e}")

        # Guardar los textos como metadatos
        self.contenedor.get_blob_client(f"faiss_index/{nombre_base}_metadata.json").upload_blob(
            json.dumps(textos, ensure_ascii=False).encode("utf-8"), overwrite=True)

        print(f"✅ FAISS Index guardado: {ruta_index}")

        return int(arr.shape[0])
'''
