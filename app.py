"""
Painel de acompanhamento das negativacoes (Mailgun -> Snov.io e
SendGrid -> Snov.io). Mailgun: le o historico gravado localmente pelo
negativacao_mailgun.py. SendGrid: pipeline irmao roda fora deste host
(maquina local do usuario) e entrega cada execucao concluida via
POST /api/sendgrid/ingest.

Uso:
    uvicorn app:app --host 0.0.0.0 --port 8080
"""
import os

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import db

app = FastAPI(title="Painel de Negativacao Mailgun / SendGrid -> Snov.io")
db.init_db()

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

SENDGRID_INGEST_API_KEY = os.getenv("SENDGRID_INGEST_API_KEY")


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/runs")
def api_list_runs(limit: int = 50):
    limit = max(1, min(limit, 500))
    return db.list_runs(limit=limit)


@app.get("/api/runs/{run_id}")
def api_get_run(run_id: int):
    run = db.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run nao encontrado")
    return run


@app.get("/api/sendgrid/runs")
def api_list_sendgrid_runs(limit: int = 50):
    limit = max(1, min(limit, 500))
    return db.list_sendgrid_runs(limit=limit)


@app.get("/api/sendgrid/runs/{run_id}")
def api_get_sendgrid_run(run_id: int):
    run = db.get_sendgrid_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run nao encontrado")
    return run


class SendgridDateBreakdown(BaseModel):
    date: str
    count: int


class SendgridAccountResult(BaseModel):
    account_label: str
    list_id: str | None = None
    added: int = 0
    duplicates: int = 0
    failed: int = 0
    error: str | None = None


class SendgridRunIngest(BaseModel):
    started_at: str
    finished_at: str | None = None
    status: str = "completed"
    dry_run: bool = False
    mode: str | None = None
    desde: str | None = None
    ate: str | None = None
    total_emails: int | None = None
    error: str | None = None
    date_breakdown: list[SendgridDateBreakdown] = []
    account_results: list[SendgridAccountResult] = []


@app.post("/api/sendgrid/ingest")
def api_ingest_sendgrid_run(payload: SendgridRunIngest, x_api_key: str | None = Header(default=None)):
    if not SENDGRID_INGEST_API_KEY:
        raise HTTPException(status_code=503, detail="SENDGRID_INGEST_API_KEY nao configurada no painel")
    if x_api_key != SENDGRID_INGEST_API_KEY:
        raise HTTPException(status_code=401, detail="X-API-Key invalida ou ausente")

    run_id = db.ingest_sendgrid_run(payload.model_dump())
    return {"id": run_id, "status": "ok"}
