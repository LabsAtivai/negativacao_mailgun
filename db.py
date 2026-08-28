"""
Persistencia SQLite do historico de execucoes da negativacao Mailgun -> Snov.io.
Usado tanto pelo negativacao_mailgun.py (grava) quanto pelo app.py (le, painel).
"""
import contextlib
import json
import os
import sqlite3

DB_PATH = os.getenv("DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "negativacao.db"))

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL DEFAULT 'running',   -- running | completed | failed
    dry_run INTEGER NOT NULL DEFAULT 0,
    domains_count INTEGER,
    total_emails INTEGER,
    error TEXT
);

CREATE TABLE IF NOT EXISTS domain_suppressions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(id),
    domain TEXT NOT NULL,
    kind TEXT NOT NULL,       -- bounces | complaints | unsubscribes
    count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS account_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(id),
    account_label TEXT NOT NULL,
    list_id TEXT,
    added INTEGER NOT NULL DEFAULT 0,
    duplicates INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_domain_suppressions_run ON domain_suppressions(run_id);
CREATE INDEX IF NOT EXISTS idx_account_results_run ON account_results(run_id);

-- Historico do pipeline irmao (negativacao SendGrid -> Snov.io, projeto
-- separado "negativacao"). Roda fora deste host (maquina local do
-- usuario, via Task Scheduler) e entrega cada execucao completa via
-- POST /api/sendgrid/ingest - por isso nao ha start_run/finish_run
-- separados aqui, so um insert unico ja com o resultado final.
CREATE TABLE IF NOT EXISTS sendgrid_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL DEFAULT 'completed',
    dry_run INTEGER NOT NULL DEFAULT 0,
    mode TEXT,          -- ontem | todos | range
    desde TEXT,
    ate TEXT,
    total_emails INTEGER,
    error TEXT
);

CREATE TABLE IF NOT EXISTS sendgrid_date_breakdown (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES sendgrid_runs(id),
    date TEXT NOT NULL,
    count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sendgrid_account_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES sendgrid_runs(id),
    account_label TEXT NOT NULL,
    list_id TEXT,
    added INTEGER NOT NULL DEFAULT 0,
    duplicates INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_sendgrid_date_breakdown_run ON sendgrid_date_breakdown(run_id);
CREATE INDEX IF NOT EXISTS idx_sendgrid_account_results_run ON sendgrid_account_results(run_id);
"""


def _connect():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    with contextlib.closing(_connect()) as conn:
        conn.executescript(SCHEMA)
        conn.commit()


def start_run(dry_run=False):
    init_db()
    with contextlib.closing(_connect()) as conn:
        cur = conn.execute(
            "INSERT INTO runs (started_at, status, dry_run) VALUES (datetime('now'), 'running', ?)",
            (1 if dry_run else 0,),
        )
        conn.commit()
        return cur.lastrowid


def record_domain_suppression(run_id, domain, kind, count):
    with contextlib.closing(_connect()) as conn:
        conn.execute(
            "INSERT INTO domain_suppressions (run_id, domain, kind, count) VALUES (?, ?, ?, ?)",
            (run_id, domain, kind, count),
        )
        conn.commit()


def record_account_result(run_id, account_label, list_id, added, duplicates, failed, error=None):
    with contextlib.closing(_connect()) as conn:
        conn.execute(
            """INSERT INTO account_results
               (run_id, account_label, list_id, added, duplicates, failed, error)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (run_id, account_label, str(list_id) if list_id is not None else None, added, duplicates, failed, error),
        )
        conn.commit()


def finish_run(run_id, status, domains_count=None, total_emails=None, error=None):
    with contextlib.closing(_connect()) as conn:
        conn.execute(
            """UPDATE runs SET finished_at = datetime('now'), status = ?,
               domains_count = ?, total_emails = ?, error = ? WHERE id = ?""",
            (status, domains_count, total_emails, error, run_id),
        )
        conn.commit()


def list_runs(limit=50):
    with contextlib.closing(_connect()) as conn:
        rows = conn.execute(
            "SELECT * FROM runs ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(row) for row in rows]


def get_run(run_id):
    with contextlib.closing(_connect()) as conn:
        run = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        if not run:
            return None
        domains = conn.execute(
            "SELECT domain, kind, count FROM domain_suppressions WHERE run_id = ? ORDER BY domain, kind",
            (run_id,),
        ).fetchall()
        accounts = conn.execute(
            "SELECT account_label, list_id, added, duplicates, failed, error FROM account_results "
            "WHERE run_id = ? ORDER BY account_label, list_id",
            (run_id,),
        ).fetchall()
        result = dict(run)
        result["domain_suppressions"] = [dict(r) for r in domains]
        result["account_results"] = [dict(r) for r in accounts]
        return result


def sendgrid_start_run(mode=None, desde=None, ate=None, dry_run=False):
    """Inicia um run do pipeline SendGrid rodando neste mesmo processo
    (negativacao_sendgrid.py). Grava direto - sem HTTP, sem ingest."""
    init_db()
    with contextlib.closing(_connect()) as conn:
        cur = conn.execute(
            """INSERT INTO sendgrid_runs (started_at, status, dry_run, mode, desde, ate)
               VALUES (datetime('now'), 'running', ?, ?, ?, ?)""",
            (1 if dry_run else 0, mode, desde, ate),
        )
        conn.commit()
        return cur.lastrowid


def sendgrid_record_date_breakdown(run_id, date, count):
    with contextlib.closing(_connect()) as conn:
        conn.execute(
            "INSERT INTO sendgrid_date_breakdown (run_id, date, count) VALUES (?, ?, ?)",
            (run_id, date, count),
        )
        conn.commit()


def sendgrid_record_account_result(run_id, account_label, list_id, added, duplicates, failed, error=None):
    with contextlib.closing(_connect()) as conn:
        conn.execute(
            """INSERT INTO sendgrid_account_results
               (run_id, account_label, list_id, added, duplicates, failed, error)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (run_id, account_label, str(list_id) if list_id is not None else None, added, duplicates, failed, error),
        )
        conn.commit()


def sendgrid_finish_run(run_id, status, total_emails=None, error=None):
    with contextlib.closing(_connect()) as conn:
        conn.execute(
            """UPDATE sendgrid_runs SET finished_at = datetime('now'), status = ?,
               total_emails = ?, error = ? WHERE id = ?""",
            (status, total_emails, error, run_id),
        )
        conn.commit()


def ingest_sendgrid_run(payload):
    """Grava uma execucao ja concluida do pipeline SendGrid (entregue via
    POST /api/sendgrid/ingest), com seus filhos date_breakdown/account_results
    numa unica transacao. Retorna o id do run criado."""
    init_db()
    with contextlib.closing(_connect()) as conn:
        cur = conn.execute(
            """INSERT INTO sendgrid_runs
               (started_at, finished_at, status, dry_run, mode, desde, ate, total_emails, error)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                payload["started_at"],
                payload.get("finished_at"),
                payload.get("status", "completed"),
                1 if payload.get("dry_run") else 0,
                payload.get("mode"),
                payload.get("desde"),
                payload.get("ate"),
                payload.get("total_emails"),
                payload.get("error"),
            ),
        )
        run_id = cur.lastrowid

        for d in payload.get("date_breakdown", []):
            conn.execute(
                "INSERT INTO sendgrid_date_breakdown (run_id, date, count) VALUES (?, ?, ?)",
                (run_id, d["date"], d["count"]),
            )

        for a in payload.get("account_results", []):
            conn.execute(
                """INSERT INTO sendgrid_account_results
                   (run_id, account_label, list_id, added, duplicates, failed, error)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    run_id,
                    a["account_label"],
                    str(a["list_id"]) if a.get("list_id") is not None else None,
                    a.get("added", 0),
                    a.get("duplicates", 0),
                    a.get("failed", 0),
                    a.get("error"),
                ),
            )

        conn.commit()
        return run_id


def list_sendgrid_runs(limit=50):
    with contextlib.closing(_connect()) as conn:
        rows = conn.execute(
            "SELECT * FROM sendgrid_runs ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(row) for row in rows]


def get_sendgrid_run(run_id):
    with contextlib.closing(_connect()) as conn:
        run = conn.execute("SELECT * FROM sendgrid_runs WHERE id = ?", (run_id,)).fetchone()
        if not run:
            return None
        dates = conn.execute(
            "SELECT date, count FROM sendgrid_date_breakdown WHERE run_id = ? ORDER BY date",
            (run_id,),
        ).fetchall()
        accounts = conn.execute(
            "SELECT account_label, list_id, added, duplicates, failed, error FROM sendgrid_account_results "
            "WHERE run_id = ? ORDER BY account_label, list_id",
            (run_id,),
        ).fetchall()
        result = dict(run)
        result["date_breakdown"] = [dict(r) for r in dates]
        result["account_results"] = [dict(r) for r in accounts]
        return result


if __name__ == "__main__":
    init_db()
    print(json.dumps({"db_path": DB_PATH, "status": "initialized"}))
