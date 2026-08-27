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


if __name__ == "__main__":
    init_db()
    print(json.dumps({"db_path": DB_PATH, "status": "initialized"}))
