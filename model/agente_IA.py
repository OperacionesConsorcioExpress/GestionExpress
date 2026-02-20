'''
from openai import OpenAI
import google.generativeai as genai
from langchain.prompts import PromptTemplate
import os, re, io, time, json
import psutil
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# ===========================
# Carga variables de entorno
# ===========================
load_dotenv()

DEEPSEEK_KEY = os.getenv("DEEPSEEK_API_KEY")
GEMINI_KEY = os.getenv("GEMINI_API_KEY")
CONEXION_BLOB = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
NOMBRE_CONTENEDOR = "1041-operaciones-cinf-conocimiento-chatbot"

if not DEEPSEEK_KEY or not GEMINI_KEY or not CONEXION_BLOB:
    raise ValueError("❌ Faltan variables de entorno requeridas")

# Bandera de RAG: auto|true|false (por defecto auto)
RAG_ENABLED_ENV = (os.getenv("CHATBOT_RAG_ENABLED", "auto") or "auto").lower()
# Límite de índices FAISS a cargar por consulta (memoria)
MAX_FAISS_INDICES = int(os.getenv("CHATBOT_MAX_FAISS_INDICES", "2"))
# Memoria supuesta del contenedor (MB) y umbral de carga para embeddings
APP_ENV = os.getenv("APP_ENV", "local").lower()  # local|cloud
MEM_TOTAL_MB = float(os.getenv("CHATBOT_MEM_TOTAL_MB", "4096" if APP_ENV == "local" else "512"))
UMBRAL_MB_CARGA_EMB = float(os.getenv("CHATBOT_EMB_MIN_MB", "220"))

# Configurar Gemini
genai.configure(api_key=GEMINI_KEY)

# ==============
# Utilidades
# ==============
def _mem_libre_mb() -> float:
    p = psutil.Process(os.getpid())
    usada = p.memory_info().rss / (1024**2)
    return MEM_TOTAL_MB - usada

def _deps_faiss_disponibles() -> bool:
    try:
        import numpy  # noqa: F401
        import faiss  # noqa: F401
        return True
    except Exception:
        return False

def _puedo_usar_rag() -> bool:
    if RAG_ENABLED_ENV == "false":
        return False
    if RAG_ENABLED_ENV == "true":
        # Forzar intento (si faltan deps/memoria, luego caerá a LLM)
        return True
    # auto → requiere deps + memoria
    return _deps_faiss_disponibles() and (_mem_libre_mb() >= UMBRAL_MB_CARGA_EMB)

# Singleton para embeddings (no cargarlos más de una vez)
_EMB = {"inst": None, "name": "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2", "dim": 384}

def _cargar_embeddings_lazy():
    """Carga SentenceTransformer solo si RAG aplica y hay memoria. Devuelve None si no procede."""
    if _EMB["inst"] is not None:
        return _EMB["inst"]
    if not _puedo_usar_rag():
        return None
    # chequeo memoria
    libre = _mem_libre_mb()
    print(f"🧠 Memoria libre aprox: {libre:.1f} MB (total sup.: {MEM_TOTAL_MB} MB)")
    if libre < UMBRAL_MB_CARGA_EMB:
        print("⚠️ No se cargan embeddings por memoria insuficiente.")
        return None
    try:
        from sentence_transformers import SentenceTransformer
        print("🔁 Cargando modelo de embeddings…")
        _EMB["inst"] = SentenceTransformer(_EMB["name"])
        return _EMB["inst"]
    except Exception as e:
        print(f"⚠️ No se pudo cargar embeddings: {e}")
        return None


class AgenteIA:
    def __init__(self, nombre_usuario: str):
        self.nombre_usuario = (nombre_usuario or "").strip().title()

        # Clientes LLM
        self.deepseek_client = OpenAI(api_key=DEEPSEEK_KEY, base_url="https://openrouter.ai/api/v1")
        self.deepseek_model = "deepseek/deepseek-r1:free"

        self.gemini_model = genai.GenerativeModel("gemini-1.5-flash")
        self.gemini_chat = self.gemini_model.start_chat()

        # Prompt
        self.prompt = PromptTemplate(
            input_variables=["input", "nombre", "contexto"],
            template=(
                "Eres Bot-CEXP, un asistente experto en transporte del SITP.\n\n"
                "Responde de forma clara, precisa y profesional usando la información técnica provista.\n\n"
                "Pregunta del usuario:\n{input}\n\n"
                "Nombre del usuario: {nombre}\n\n"
                "Información técnica relevante:\n{contexto}\n\n"
                "Prioriza puntos clave y cierra la respuesta claramente."
            )
        )

        # Blob Storage
        self.blob_service = BlobServiceClient.from_connection_string(CONEXION_BLOB)
        self.contenedor = self.blob_service.get_container_client(NOMBRE_CONTENEDOR)

    # =============================
    # Flujo principal de respuesta
    # =============================
    def responder(self, pregunta: str) -> str:
        # 1) Construir contexto (si aplica RAG)
        contexto = ""
        if _puedo_usar_rag():
            try:
                contexto = self._buscar_contexto(pregunta)
            except Exception as e:
                print(f"🛑 Error buscando contexto RAG: {e}")
                contexto = ""

        # 2) Construir prompt
        if contexto.strip():
            prompt_texto = self.prompt.format(input=pregunta, nombre=self.nombre_usuario, contexto=contexto[:2000])
        else:
            prompt_texto = (
                "Eres Bot-CEXP, un asistente experto en transporte del SITP.\n\n"
                "Responde de forma clara, precisa y profesional en español.\n\n"
                f"Nombre del usuario: {self.nombre_usuario}\n\n"
                f"Pregunta del usuario:\n{pregunta}\n"
            )
        prompt_texto = prompt_texto[:3000]

        # 3) LLMs: DeepSeek → Gemini (fallback)
        txt = self._invocar_deepseek(prompt_texto)
        if txt:
            return txt

        txt = self._invocar_gemini(prompt_texto)
        if txt:
            return txt

        # 4) Falla general
        return (
            f"😓 {self.nombre_usuario}, en este momento los proveedores de IA están saturados. "
            "Intenta nuevamente en unos minutos."
        )

    # ======================
    # Proveedores de IA
    # ======================
    def _invocar_deepseek(self, prompt: str, reintentos: int = 0, pausa: float = 0.6) -> str | None:
        for i in range(reintentos + 1):
            try:
                r = self.deepseek_client.chat.completions.create(
                    model=self.deepseek_model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.9, max_tokens=512, top_p=0.9
                )
                out = (r.choices[0].message.content or "").strip()
                if out:
                    print("✔️ DeepSeek respondió.")
                    return out
                print("⚠️ DeepSeek respondió vacío.")
                return None
            except Exception as e:
                msg = str(e)
                low = msg.lower()
                print(f"⚠️ DeepSeek falló: {msg}")
                # Si es 429 / rate limit → no reintentar, pasar a Gemini
                if "429" in low or "rate" in low or "quota" in low or "temporarily rate-limited" in low:
                    return None
                # Otros errores: no insistir para no bloquear UX
                return None
        return None

    def _invocar_gemini(self, prompt: str) -> str | None:
        try:
            print("↪️ Probando Gemini fallback…")
            r = self.gemini_chat.send_message(
                prompt,
                generation_config={"max_output_tokens": 512, "temperature": 0.9, "top_p": 0.9}
            )
            # A veces .text viene vacío pero hay candidates
            texto = (getattr(r, "text", "") or "").strip()
            if not texto and hasattr(r, "candidates") and r.candidates:
                try:
                    texto = r.candidates[0].content.parts[0].text.strip()
                except Exception:
                    pass
            if texto:
                print("✔️ Gemini respondió (fallback).")
                return f"(⚙️ Gemini Fallback)\n{texto}"
            print("⚠️ Gemini respondió vacío.")
            return None
        except Exception as e:
            print(f"❌ Gemini falló: {e}")
            return None

    # ======================
    # Búsqueda de contexto
    # ======================
    def _buscar_contexto(self, pregunta: str, top_k: int = 3) -> str:
        """
        Busca fragmentos relevantes desde FAISS + metadatos JSON.
        Devuelve "" si no hay deps, memoria, índices o errores.
        """
        # Validar deps y memoria
        if not _deps_faiss_disponibles():
            return ""
        # Cargar embeddings (lazy)
        modelo = _cargar_embeddings_lazy()
        if modelo is None:
            return ""

        # Importar deps pesadas dentro (lazy)
        import numpy as np
        import faiss

        # Embedding de la pregunta
        emb_p = modelo.encode([pregunta]).astype("float32")

        # Listar índices
        try:
            blobs_index = self.contenedor.list_blobs(name_starts_with="faiss_index/")
            indices = [b.name for b in blobs_index if b.name.endswith(".index")]
            if not indices:
                return ""
        except Exception as e:
            print(f"⚠️ No se pudieron listar índices: {e}")
            return ""

        contexto_total = []
        # Limitar cantidad de índices por consulta
        for ruta_index in indices[:MAX_FAISS_INDICES]:
            try:
                nombre_base = os.path.basename(ruta_index).replace(".index", "")
                ruta_metadata = f"faiss_index/{nombre_base}_metadata.json"

                # Cargar índice FAISS desde bytes
                idx_bytes = self.contenedor.get_blob_client(ruta_index).download_blob().readall()
                stream = io.BytesIO(idx_bytes)
                reader = faiss.PyCallbackIOReader(lambda sz: stream.read(sz))
                index = faiss.read_index(reader)

                if index.d != emb_p.shape[1]:
                    print(f"⚠️ Dimensión incompatible: FAISS {index.d} vs Embedding {emb_p.shape[1]}")
                    continue

                # Cargar metadatos (textos)
                meta_bytes = self.contenedor.get_blob_client(ruta_metadata).download_blob().readall()
                try:
                    textos_json = json.loads(meta_bytes)
                except Exception:
                    # A veces guardaste lista “plana” en faiss_index; cubrir ambos casos
                    try:
                        textos_json = json.loads(meta_bytes.decode("utf-8"))
                    except Exception:
                        textos_json = None

                textos = textos_json if isinstance(textos_json, list) else (textos_json.get("texts") if isinstance(textos_json, dict) else None)
                if not textos or not isinstance(textos, list):
                    print(f"⚠️ Metadatos inválidos o vacíos en {ruta_metadata}")
                    continue

                # Buscar
                D, I = index.search(emb_p, top_k)

                def limpiar_texto(texto: str) -> str:
                    t = re.sub(r"\s+", " ", texto or "")
                    t = re.sub(r"\s([.,;:])", r"\1", t)
                    return t.strip()

                fragmentos = [
                    f"📄 {nombre_base}\n{limpiar_texto(textos[i])}"
                    for i in I[0]
                    if isinstance(i, (int, np.integer)) and 0 <= i < len(textos)
                ]
                contexto_total.extend(fragmentos)

            except Exception as e:
                print(f"⚠️ Error al procesar {ruta_index}: {e}")

        return "\n\n".join(contexto_total[:top_k])
'''