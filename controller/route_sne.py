from fastapi import APIRouter, Request, HTTPException, UploadFile, Depends, Body, Query,  File, Form
from starlette.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from model.gestion_checklist import GestionChecklist
from pydantic import BaseModel
from datetime import datetime, time
from typing import List, Optional, Dict, Any
import io, json
import pandas as pd
from model.gestion_sne_asignacion import RegistroSNE
from model.gestion_sne_objecion import RegistroSNE

# Crear router
router_sne = APIRouter()

# Configurar plantillas Jinja2
templates = Jinja2Templates(directory="./view")

# Función localmente para validar usuario
def get_user_session(req: Request):
    return req.session.get('user')

# --- RUTA PRINCIPAL SISTEMA ESTACIÓN DE SERVICIO ---
# ----------------------------------------------------------------------
@router_sne.get("/sne", response_class=HTMLResponse)
def eds_config(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
