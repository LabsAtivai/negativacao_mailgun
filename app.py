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

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
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
# Chave = valor real do campo "status" no body. O Postal NAO manda header
# X-Postal-Event, e o enum real do campo "status" e Sent/SoftFail/HardFail/
# Held (SoftFail = falha temporaria, ainda tentando retry - nao negativa;
# HardFail = desistiu de vez apos esgotar as tentativas - negativa).
POSTAL_STATUS_KIND_MAP = {
    "HardFail": "delivery_failed",
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

    envelope = await request.json()

    # Postal 3.x envelopa o body real: {"event": "...", "timestamp": ...,
    # "payload": {...formato documentado (message/status ou
    # original_message/bounce)...}, "uuid": ...}. A doc original (e o campo
    # "Payload" exibido no admin do Postal) so mostra o conteudo de dentro
    # de "payload" - sem desembrulhar isso aqui, todo evento real cai no
    # branch de "irrelevante" e nada e gravado, mesmo respondendo 200.
    inner = envelope.get("payload")
    payload = inner if isinstance(inner, dict) else envelope

    # Bounce event: payload proprio, com "original_message" + "bounce" (sem
    # campo "status").
    if "bounce" in payload:
        recipient = (payload.get("original_message") or {}).get("to")
        token = (payload.get("bounce") or {}).get("token")
        if recipient:
            db.record_postal_event("bounced", recipient, status="Bounced", message_token=token)
        return {"status": "ok"}

    # Status event: {"message": {...}, "status": "Sent"|"SoftFail"|"HardFail"|"Held", ...}.
    # SoftFail = falha temporaria (ainda em retry) - nao negativa.
    kind = POSTAL_STATUS_KIND_MAP.get(payload.get("status"))
    if kind:
        message = payload.get("message") or {}
        recipient = message.get("to")
        if recipient:
            db.record_postal_event(kind, recipient, status=payload.get("status"), message_token=message.get("token"))

    # Eventos irrelevantes (MessageSent, MessageDelayed, MessageLoaded,
    # MessageLinkClicked, DomainDNSError, ...) sao so confirmados, sem gravar nada.
    return {"status": "ok"}


# ==============================================================================
# Agendamento diario (01h por padrao) dos tres pipelines, direto no processo
# do painel - nao depende de cron externo no host. Mesma logica de
# "bloqueia se ja ha run em andamento" + subprocess.Popen dos botoes
# "Rodar agora" manuais.
# ==============================================================================

SCHEDULE_TIMEZONE = os.getenv("SCHEDULE_TIMEZONE", "America/Sao_Paulo")
SCHEDULE_HOUR = int(os.getenv("SCHEDULE_HOUR", "1"))
SCHEDULE_MINUTE = int(os.getenv("SCHEDULE_MINUTE", "0"))


def _run_scheduled_pipeline(cmd, list_runs_fn, label):
    if _has_running(list_runs_fn(limit=5)):
        print(f"[scheduler] {label}: ja ha uma execucao em andamento, pulando a execucao agendada.")
        return
    print(f"[scheduler] {label}: iniciando execucao agendada ({SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} {SCHEDULE_TIMEZONE}).")
    subprocess.Popen(cmd, cwd=BASE_DIR)


scheduler = BackgroundScheduler(timezone=SCHEDULE_TIMEZONE)
_cron = CronTrigger(hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE, timezone=SCHEDULE_TIMEZONE)
scheduler.add_job(
    lambda: _run_scheduled_pipeline([sys.executable, "negativacao_mailgun.py"], db.list_runs, "Mailgun"),
    _cron, id="mailgun_nightly", replace_existing=True,
)
scheduler.add_job(
    lambda: _run_scheduled_pipeline([sys.executable, "negativacao_sendgrid.py"], db.list_sendgrid_runs, "SendGrid"),
    _cron, id="sendgrid_nightly", replace_existing=True,
)
scheduler.add_job(
    lambda: _run_scheduled_pipeline([sys.executable, "negativacao_postal.py"], db.list_postal_runs, "Postal"),
    _cron, id="postal_nightly", replace_existing=True,
)
scheduler.start()
