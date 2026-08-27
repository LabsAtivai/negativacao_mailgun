"""
Painel de acompanhamento das negativacoes (Mailgun -> Snov.io).
So leitura: mostra o historico de execucoes gravado pelo negativacao_mailgun.py
no SQLite (db.py). Nao dispara execucoes.

Uso:
    uvicorn app:app --host 0.0.0.0 --port 8080
"""
import os

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

import db

app = FastAPI(title="Painel de Negativacao Mailgun -> Snov.io")

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


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
