from openai import OpenAI
import google.generativeai as genai
from langchain.prompts import PromptTemplate
#from sentence_transformers import SentenceTransformer
import numpy as np
import faiss, json, os, re, io
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Carga variables desde .env
load_dotenv()  

# Variables de entorno
deepseek_key = os.getenv("DEEPSEEK_API_KEY") #Principal API Key para DeepSeek https://openrouter.ai/settings/keys
gemini_key = os.getenv("GEMINI_API_KEY")
CONEXION_BLOB = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
NOMBRE_CONTENEDOR = "1041-operaciones-cinf-conocimiento-chatbot"

if not deepseek_key or not gemini_key or not CONEXION_BLOB:
    raise ValueError("❌ Faltan variables de entorno requeridas")

# Configurar Gemini
genai.configure(api_key=gemini_key)

class AgenteIA:
    def __init__(self, nombre_usuario: str):
        self.nombre_usuario = nombre_usuario.strip().title()
        
        # Modelos
        self.deepseek_client = OpenAI(api_key=deepseek_key, base_url="https://openrouter.ai/api/v1") # Cliente DeepSeek vía OpenRouter
        self.deepseek_model = "deepseek/deepseek-r1:free"
        self.gemini_model = genai.GenerativeModel("gemini-1.5-flash") # Cliente Gemini (fallback)
        self.gemini_chat = self.gemini_model.start_chat()
        #self.modelo_emb = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2") # Modelo de embeddings
        self._modelo_emb = None # Lazy loading del modelo de embeddings
        
        # Prompt con contexto RAG
        self.prompt = PromptTemplate(
            input_variables=["input", "nombre", "contexto"],
            template="""Eres Bot-CEXP, un asistente experto en transporte del sistema SITP.

            Tu tarea es responder de forma clara, precisa y profesional la siguiente consulta del usuario, usando únicamente la información técnica que se te provee.

            Pregunta del usuario:
            {input}
            
            Nombre del usuario: {nombre}

            Información técnica relevante extraída de documentos oficiales:
            {contexto}

            Si el espacio es limitado, prioriza los puntos clave y cierra la respuesta de manera clara y comprensible. Evita dejar frases sin terminar.
            """
        )
        
        # Cliente de Azure Blob Storage
        self.blob_service = BlobServiceClient.from_connection_string(CONEXION_BLOB)
        self.contenedor = self.blob_service.get_container_client(NOMBRE_CONTENEDOR)

    def _get_modelo_emb(self):
        """Carga perezosa del modelo de embeddings solo cuando se requiere."""
        if self._modelo_emb is None:
            from sentence_transformers import SentenceTransformer
            self._modelo_emb = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
        return self._modelo_emb

    def responder(self, pregunta: str) -> str:
        """Genera respuesta a partir de contexto RAG o IA libre."""
        contexto = self._buscar_contexto(pregunta)

        if contexto.strip():
            prompt_texto = self.prompt.format(input=pregunta, nombre=self.nombre_usuario, contexto=contexto[:2000])
            #print("🧠 Usando RAG")
        else:
            prompt_texto = (
                f"Eres Bot-CEXP, un asistente experto en transporte del sistema SITP.\n\n"
                f"Tu tarea es responder de forma clara, precisa y profesional.\n\n"
                f"Nombre del usuario: {self.nombre_usuario}\n\n"
                f"Pregunta del usuario:\n{pregunta}\n\n"
                f"Responde de manera profesional en español."
            )
            #print("💬 Usando IA sin contexto")

        prompt_texto = prompt_texto[:3000]

        # 1 Intentar con DeepSeek
        try:
            respuesta = self.deepseek_client.chat.completions.create(
                model=self.deepseek_model,
                messages=[{"role": "user", "content": prompt_texto}],
                temperature=0.9,
                max_tokens=512,
                top_p=0.9
            )
            contenido = respuesta.choices[0].message.content.strip()
            return contenido if contenido else "⚠️ La IA no devolvió ninguna respuesta."
        except Exception as e:
            print(f"⚠️ DeepSeek falló: {str(e)}")

        # 2 Fallback con Gemini
        try:
            respuesta = self.gemini_chat.send_message(
                prompt_texto,
                generation_config={"max_output_tokens": 512, "temperature": 0.9, "top_p": 0.9}
            )
            texto = respuesta.text.strip()
            limpio = re.split(r"\n(Pregunta del usuario|Respuesta del asistente):", texto)[0].strip()
            return f"(⚙️ Gemini Fallback)\n{limpio if limpio else '⚠️ La IA no devolvió ninguna respuesta.'}"
        except Exception as e:
            msg = str(e)
            if any(x in msg.lower() for x in ["429", "quota", "rate"]):
                return f"😓 {self.nombre_usuario}, estoy fuera de servicio por límite de uso. Intenta más tarde."
            return f"⚠️ Error al generar respuesta: {msg}"
        
    def _buscar_contexto(self, pregunta: str, top_k: int = 3, max_fuentes: int = 3) -> str:
        """Busca fragmentos relevantes desde FAISS + metadatos JSON."""
        def limpiar_texto(texto):
            texto = re.sub(r"\s+", " ", texto)
            texto = re.sub(r"\s([.,;:])", r"\1", texto)
            return texto.strip()

        modelo = self._get_modelo_emb()
        embedding_pregunta = modelo.encode([pregunta]).astype("float32")
        #embedding_pregunta = self.modelo_emb.encode([pregunta]).astype("float32")
        contexto_total = []

        # Listar todos los archivos .index
        blobs_index = self.contenedor.list_blobs(name_starts_with="faiss_index/")
        indices = [b.name for b in blobs_index if b.name.endswith(".index")]

        for ruta_index in indices[:max_fuentes]:
            try:
                nombre_base = os.path.basename(ruta_index).replace(".index", "")
                ruta_metadata = f"faiss_index/{nombre_base}_metadata.json"

                index_bytes = self.contenedor.get_blob_client(ruta_index).download_blob().readall()
                index_stream = io.BytesIO(index_bytes)
                reader = faiss.PyCallbackIOReader(lambda sz: index_stream.read(sz))
                index = faiss.read_index(reader)

                if index.d != embedding_pregunta.shape[1]:
                    print(f"⚠️ Dimensión incompatible: FAISS {index.d} vs Embedding {embedding_pregunta.shape[1]}")
                    continue

                # Cargar textos
                meta_bytes = self.contenedor.get_blob_client(ruta_metadata).download_blob().readall()
                textos_json = json.loads(meta_bytes)
                textos = textos_json if isinstance(textos_json, list) else textos_json.get("texts") or textos_json.get("chunks")

                if not textos or not isinstance(textos, list):
                    print(f"⚠️ Metadatos inválidos o vacíos en {ruta_metadata}")
                    continue

                D, I = index.search(embedding_pregunta, top_k)
                #print(f"🔎 Resultados FAISS para {nombre_base}: {I[0]}")

                fragmentos = [f"📄 {nombre_base}\n{limpiar_texto(textos[i])}" for i in I[0] if 0 <= i < len(textos)]
                contexto_total.extend(fragmentos)

            except Exception as e:
                print(f"⚠️ Error al procesar {ruta_index}: {e}")

        #print("📚 Fragmentos seleccionados:\n", contexto_total[:3])
        return "\n\n".join(contexto_total[:3])