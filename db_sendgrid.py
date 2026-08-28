"""
Leitura (somente leitura) do historico de execucoes do pipeline de
negativacao SendGrid -> Snov.io, gravado por outro projeto
(C:\\Users\\HP\\relatorios\\negativação\\negativacao.py) num SQLite proprio.

Este modulo nao grava nada - so serve o painel (app.py). Schema (definido
no outro projeto):

    runs(id, started_at, finished_at, status, dry_run, mode, desde, ate,
         total_emails, error)
    date_breakdown(id, run_id, date, count)   -- equivalente a domain_suppressions,
                                                  mas agrupado por data (sem conceito
                                                  de dominio no SendGrid)
    account_results(id, run_id, account_label, list_id, added, duplicates,
                     failed, error)
"""
import contextlib
import os
import sqlite3

SENDGRID_DB_PATH = os.getenv(
    "SENDGRID_DB_PATH",
    r"C:\Users\HP\relatorios\negativação\data\negativacao.db",
)


def _connect():
    conn = sqlite3.connect(f"file:{SENDGRID_DB_PATH}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def list_runs(limit=50):
    try:
        with contextlib.closing(_connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM runs ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(row) for row in rows]
    except sqlite3.Error:
        # Banco do SendGrid ainda nao existe/nao rodou nenhuma vez neste host.
        return []


def get_run(run_id):
    try:
        with contextlib.closing(_connect()) as conn:
            run = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
            if not run:
                return None
            dates = conn.execute(
                "SELECT date, count FROM date_breakdown WHERE run_id = ? ORDER BY date",
                (run_id,),
            ).fetchall()
            accounts = conn.execute(
                "SELECT account_label, list_id, added, duplicates, failed, error FROM account_results "
                "WHERE run_id = ? ORDER BY account_label, list_id",
                (run_id,),
            ).fetchall()
            result = dict(run)
            result["date_breakdown"] = [dict(r) for r in dates]
            result["account_results"] = [dict(r) for r in accounts]
            return result
    except sqlite3.Error:
        return None
