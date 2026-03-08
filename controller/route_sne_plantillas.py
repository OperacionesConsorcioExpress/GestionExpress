from fastapi import APIRouter, Request, HTTPException, Depends, Query
from starlette.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
from pydantic import BaseModel, Field, validator
from model.gestion_sne_plantillas import GestionSnePlantillas

router_sne_plantillas = APIRouter()
templates = Jinja2Templates(directory="./view")

def _sesion(req: Request):
    return req.session.get("user")

# ══════════════════════════════════════════════════════════════════════════════
# VISTA HTML
# ══════════════════════════════════════════════════════════════════════════════
@router_sne_plantillas.get("/sne_plantillas", response_class=HTMLResponse)
def vista_sne_plantillas(
    req: Request,
    user_session: dict = Depends(_sesion),
):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(
        "sne_plantillas.html",
        {"request": req, "user_session": user_session},
    )

# ══════════════════════════════════════════════════════════════════════════════
# SCHEMAS Pydantic
# ══════════════════════════════════════════════════════════════════════════════

class TokenIn(BaseModel):
    token:          str  = Field(..., min_length=3, max_length=60)
    descripcion:    str  = Field(..., min_length=3, max_length=150)
    campo_ics:      Optional[str]  = None
    sql_query:      Optional[str]  = None
    requiere_input: bool           = False
    input_label:    Optional[str]  = None
    activo:         bool           = True
    orden:          int            = Field(99, ge=0, le=999)

    @validator("token")
    def token_entre_llaves(cls, v):
        v = v.strip()
        if not v.startswith("{") or not v.endswith("}"):
            raise ValueError("El token debe estar entre llaves: {mi_token}")
        return v

class TokenUpdateIn(TokenIn):
    pass

class PlantillaIn(BaseModel):
    id_motivo: int  = Field(..., ge=1)
    nombre:    str  = Field("Plantilla estándar", max_length=120)
    plantilla: str  = Field(..., min_length=5)

class ProbarSqlIn(BaseModel):
    sql_query: str = Field(..., min_length=5)
    id_ics:    int = Field(..., ge=1)

# ══════════════════════════════════════════════════════════════════════════════
# TOKENS — CRUD
# ══════════════════════════════════════════════════════════════════════════════
@router_sne_plantillas.get("/api/sne/plantillas/tokens")
def listar_tokens(solo_activos: bool = Query(False)):
    with GestionSnePlantillas() as db:
        try:
            return {"ok": True, "data": db.listar_tokens(solo_activos=solo_activos)}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@router_sne_plantillas.get("/api/sne/plantillas/tokens/{id_token}")
def obtener_token(id_token: int):
    with GestionSnePlantillas() as db:
        try:
            return {"ok": True, "data": db.obtener_token(id_token)}
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))

@router_sne_plantillas.post("/api/sne/plantillas/tokens", status_code=201)
def crear_token(payload: TokenIn):
    with GestionSnePlantillas() as db:
        try:
            row = db.crear_token(
                token=payload.token,
                descripcion=payload.descripcion,
                campo_ics=payload.campo_ics,
                sql_query=payload.sql_query,
                requiere_input=payload.requiere_input,
                input_label=payload.input_label,
                orden=payload.orden,
            )
            return {"ok": True, "data": row}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@router_sne_plantillas.put("/api/sne/plantillas/tokens/{id_token}")
def actualizar_token(id_token: int, payload: TokenUpdateIn):
    with GestionSnePlantillas() as db:
        try:
            row = db.actualizar_token(
                id_token=id_token,
                descripcion=payload.descripcion,
                campo_ics=payload.campo_ics,
                sql_query=payload.sql_query,
                requiere_input=payload.requiere_input,
                input_label=payload.input_label,
                activo=payload.activo,
                orden=payload.orden,
            )
            return {"ok": True, "data": row}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@router_sne_plantillas.post("/api/sne/plantillas/tokens/probar-sql")
def probar_sql_token(payload: ProbarSqlIn):
    """Ejecuta el sql_query de un token con un ICS real para validarlo."""
    with GestionSnePlantillas() as db:
        resultado = db.probar_sql_token(
            sql_query=payload.sql_query,
            id_ics=payload.id_ics,
        )
    return {"ok": resultado["ok"], "data": resultado}

# ══════════════════════════════════════════════════════════════════════════════
# PLANTILLAS — CRUD
# ══════════════════════════════════════════════════════════════════════════════
@router_sne_plantillas.get("/api/sne/plantillas")
def listar_plantillas(id_motivo: Optional[int] = Query(None)):
    with GestionSnePlantillas() as db:
        try:
            return {"ok": True, "data": db.listar_plantillas(id_motivo=id_motivo)}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@router_sne_plantillas.get("/api/sne/plantillas/motivos-estado")
def motivos_con_estado():
    """Lista todos los motivos con indicador de plantilla activa."""
    with GestionSnePlantillas() as db:
        try:
            return {"ok": True, "data": db.listar_motivos_con_estado_plantilla()}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@router_sne_plantillas.get("/api/sne/plantillas/{id_plantilla}")
def obtener_plantilla(id_plantilla: int):
    with GestionSnePlantillas() as db:
        try:
            return {"ok": True, "data": db.obtener_plantilla(id_plantilla)}
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))

@router_sne_plantillas.post("/api/sne/plantillas", status_code=201)
def guardar_plantilla(payload: PlantillaIn, req: Request):
    user_session = req.session.get("user")
    usuario_id   = user_session["id"] if user_session else None
    with GestionSnePlantillas() as db:
        try:
            row = db.guardar_plantilla(
                id_motivo=payload.id_motivo,
                plantilla=payload.plantilla,
                nombre=payload.nombre,
                usuario_id=usuario_id,
            )
            return {"ok": True, "data": row}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@router_sne_plantillas.delete("/api/sne/plantillas/{id_plantilla}")
def eliminar_plantilla(id_plantilla: int):
    with GestionSnePlantillas() as db:
        try:
            return {"ok": True, "data": db.eliminar_plantilla(id_plantilla)}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

# ══════════════════════════════════════════════════════════════════════════════
# RESOLUCIÓN DE TOKENS — consumido desde sne_objecion.html
# ══════════════════════════════════════════════════════════════════════════════
@router_sne_plantillas.get("/api/sne/plantillas/resolver/{id_motivo}")
def resolver_plantilla(
    id_motivo: int,
    id_ics: int = Query(..., ge=1),
):
    """
    Endpoint principal consumido por el modal de objeción.
    Retorna el texto con tokens automáticos ya resueltos y lista
    de tokens que el revisor debe completar manualmente.
    """
    with GestionSnePlantillas() as db:
        try:
            resultado = db.resolver_plantilla(id_motivo=id_motivo, id_ics=id_ics)
            return {"ok": True, "data": resultado}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@router_sne_plantillas.post("/api/sne/plantillas/analizar-tokens")
def analizar_tokens_en_texto(req_body: dict):
    """
    Analiza un texto y retorna los tokens que contiene con su metadata.
    Usado para el panel de vista previa en tiempo real de la pantalla de gestión.
    """
    texto = req_body.get("texto", "")
    with GestionSnePlantillas() as db:
        try:
            return {"ok": True, "data": db.tokens_en_plantilla(texto)}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@router_sne_plantillas.get("/api/sne/plantillas/ics-prueba")
def ics_de_prueba():
    """Retorna ICS recientes con datos completos para la pestaña de prueba."""
    with GestionSnePlantillas() as db:
        try:
            return {"ok": True, "data": db.obtener_ics_de_prueba()}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

# ══════════════════════════════════════════════════════════════════════════════
# SEED DE TOKENS — pobla sne.nota_tokens con los 13 tokens canónicos del sistema
# POST /api/sne/tokens/seed   (solo administradores)
# ══════════════════════════════════════════════════════════════════════════════
_TOKENS_CANONICOS = [
    # (token, descripcion, campo_ics, requiere_input, input_label, orden)
    ("{id_ics}",          "Identificador único del ICS",                     "id_ics",           False, None, 1),
    ("{fecha}",           "Fecha del servicio (YYYY-MM-DD)",                 "fecha",            False, None, 2),
    ("{servicio}",        "Código de servicio del ICS",                      "servicio",         False, None, 3),
    ("{tabla}",           "Tabla origen del ICS",                            "tabla",            False, None, 4),
    ("{viaje}",           "Identificador del viaje (id_viaje)",               "id_viaje",         False, None, 5),
    ("{sentido}",         "Sentido del recorrido",                           "sentido",          False, None, 6),
    ("{hora_inicio}",     "Hora inicial teórica del servicio (HH:MM)",       "hora_ini_teorica", False, None, 7),
    ("{km_revision}",     "Kilómetros en revisión",                          "km_revision",      False, None, 8),
    ("{vehiculo}",        "Vehículo real que operó el servicio",             "vehiculo_real",    False, None, 9),
    ("{conductor}",       "Conductor asignado al servicio",                  "conductor",        False, None, 10),
    ("{ruta}",            "Ruta comercial (JOIN config.rutas por id_linea)",  "ruta_comercial",   False, None, 11),
    # requiere_input=TRUE para satisfacer chk_token_source; el resolver los ignora via _TOKENS_FRONTEND
    ("{km_objetado}",     "Km objetados — inyectado desde el frontend",      None,               True, "km_objetado",    12),
    ("{km_no_objetado}",  "Km no objetados — inyectado desde el frontend",   None,               True, "km_no_objetado", 13),
]

@router_sne_plantillas.post("/api/sne/tokens/seed")
def seed_tokens(req: Request, user_session: dict = Depends(_sesion)):
    """
    Trunca sne.nota_tokens y la vuelve a poblar con los 13 tokens canónicos.
    Solo ejecutar una vez al configurar o al resetear el sistema de plantillas.
    """
    with GestionSnePlantillas() as db:
        try:
            db.cursor.execute("TRUNCATE TABLE sne.nota_tokens RESTART IDENTITY CASCADE;")
            db.cursor.executemany(
                """
                INSERT INTO sne.nota_tokens
                    (token, descripcion, campo_ics, sql_query,
                        requiere_input, input_label, activo, orden)
                VALUES (%s, %s, %s, NULL, %s, %s, TRUE, %s)
                """,
                [(t[0], t[1], t[2], t[3], t[4], t[5]) for t in _TOKENS_CANONICOS],
            )
            db.connection.commit()
            tokens = db.listar_tokens()
            return {"ok": True, "msg": f"{len(tokens)} tokens sembrados correctamente", "data": [dict(r) for r in tokens]}
        except Exception as e:
            db.connection.rollback()
            raise HTTPException(status_code=500, detail=str(e))
