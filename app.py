"""
Painel de acompanhamento das negativacoes (Mailgun -> Snov.io, SendGrid ->
Snov.io e Postal -> Snov.io). Le o historico gravado por
negativacao_mailgun.py, negativacao_sendgrid.py e negativacao_postal.py no
SQLite (db.py), e pode disparar os tres via POST /api/runs/trigger,
/api/sendgrid/runs/trigger e /api/postal/runs/trigger (mesmo processo/imagem,
subprocess.Popen - o botao "Rodar agora" do painel usa isso). Postal e
diferente dos outros dois: nao ha API de pull, o dado so chega via webhook
(POST /api/postal/webhook), que grava eventos brutos consumidos depois em
lote por negativacao_postal.py.

Uso:
    uvicorn app:app --host 0.0.0.0 --port 8080
"""
import hmac
import os
import subprocess
import sys

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import db

app = FastAPI(title="Painel de Negativacao Mailgun / SendGrid / Postal -> Snov.io")
db.init_db()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

SENDGRID_INGEST_API_KEY = os.getenv("SENDGRID_INGEST_API_KEY")
POSTAL_WEBHOOK_SECRET = os.getenv("POSTAL_WEBHOOK_SECRET")

# Status events (payload com "message"+"status") que indicam e-mail ruim.
# Chave = valor curto do campo "status" no body (ex: "status":"DeliveryFailed"),
# igual ao nome do evento (header X-Postal-Event) sem o prefixo "Message".
POSTAL_STATUS_KIND_MAP = {
    "DeliveryFailed": "delivery_failed",
    "Held": "held",
}


def _has_running(runs):
    return any(r["status"] == "running" for r in runs)


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


@app.post("/api/runs/trigger")
def api_trigger_run():
    if _has_running(db.list_runs(limit=5)):
        raise HTTPException(status_code=409, detail="Ja ha uma execucao Mailgun em andamento")
    subprocess.Popen([sys.executable, "negativacao_mailgun.py"], cwd=BASE_DIR)
    return {"status": "started"}


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


class SendgridTrigger(BaseModel):
    todos: bool = False


@app.post("/api/sendgrid/runs/trigger")
def api_trigger_sendgrid_run(payload: SendgridTrigger = SendgridTrigger()):
    if _has_running(db.list_sendgrid_runs(limit=5)):
        raise HTTPException(status_code=409, detail="Ja ha uma execucao SendGrid em andamento")
    cmd = [sys.executable, "negativacao_sendgrid.py"]
    if payload.todos:
        cmd.append("--todos")
    subprocess.Popen(cmd, cwd=BASE_DIR)
    return {"status": "started"}


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


@app.get("/api/postal/runs")
def api_list_postal_runs(limit: int = 50):
    limit = max(1, min(limit, 500))
    return db.list_postal_runs(limit=limit)


@app.get("/api/postal/runs/{run_id}")
def api_get_postal_run(run_id: int):
    run = db.get_postal_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run nao encontrado")
    return run


class PostalTrigger(BaseModel):
    todos: bool = False


@app.post("/api/postal/runs/trigger")
def api_trigger_postal_run(payload: PostalTrigger = PostalTrigger()):
    if _has_running(db.list_postal_runs(limit=5)):
        raise HTTPException(status_code=409, detail="Ja ha uma execucao Postal em andamento")
    cmd = [sys.executable, "negativacao_postal.py"]
    if payload.todos:
        cmd.append("--todos")
    subprocess.Popen(cmd, cwd=BASE_DIR)
    return {"status": "started"}


@app.post("/api/postal/webhook")
async def api_postal_webhook(request: Request, key: str | None = None):
    if not POSTAL_WEBHOOK_SECRET:
        raise HTTPException(status_code=503, detail="POSTAL_WEBHOOK_SECRET nao configurada no painel")
    if not key or not hmac.compare_digest(key, POSTAL_WEBHOOK_SECRET):
        raise HTTPException(status_code=401, detail="key invalida ou ausente")

    payload = await request.json()
    event_name = request.headers.get("X-Postal-Event")
    short_name = event_name[len("Message"):] if event_name and event_name.startswith("Message") else event_name

    is_bounce = short_name == "Bounced" if short_name else "bounce" in payload
    if is_bounce:
        recipient = (payload.get("original_message") or {}).get("to")
        token = (payload.get("bounce") or {}).get("token")
        if recipient:
            db.record_postal_event("bounced", recipient, status="Bounced", message_token=token)
        return {"status": "ok"}

    # Status event: usa o header (curto, sem "Message") se veio, senao o
    # proprio campo "status" do body (ex: {"status": "DeliveryFailed", ...}).
    status_value = short_name or payload.get("status")
    kind = POSTAL_STATUS_KIND_MAP.get(status_value)
    if kind:
        message = payload.get("message") or {}
        recipient = message.get("to")
        if recipient:
            db.record_postal_event(kind, recipient, status=payload.get("status"), message_token=message.get("token"))

    # Eventos irrelevantes (MessageSent, MessageDelayed, MessageLoaded,
    # MessageLinkClicked, DomainDNSError, ...) sao so confirmados, sem gravar nada.
    return {"status": "ok"}
