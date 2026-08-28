"""
Le contatos do MySQL (base que o webhook-api de supressoes SendGrid
mantem) e envia quem esta com ativo=0 (bounces/blocks/spam reports que
marcaram o contato como inativo) para a Lista de e-mails a nao enviar
(negativacao) de TODAS as contas Snov.io ativas cadastradas no
snov-am-api - mesma fonte de contas que o negativacao_mailgun.py usa,
via CREDENTIALS_API_URL/CREDENTIALS_API_KEY.

Uso:
    python negativacao_sendgrid.py --dry-run   # so mostra contagens, nada enviado
    python negativacao_sendgrid.py             # roda de verdade (dia anterior)
    python negativacao_sendgrid.py --todos     # roda de verdade (todo o historico)
"""
import argparse
import builtins
import datetime
import functools
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import mysql.connector
import requests
from dotenv import load_dotenv

import db

load_dotenv()

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOG_DIR, f"negativacao_sendgrid_{datetime.datetime.now():%Y%m%d_%H%M%S}.txt")
_log_file = open(LOG_PATH, "a", encoding="utf-8")


def _print_and_log(*args, **kwargs):
    builtins.print(*args, **kwargs)
    kwargs["file"] = _log_file
    builtins.print(*args, **kwargs)


# Forca flush em cada print: sem isso, a saida fica em buffer quando redirecionada
# para um arquivo (ex: nohup/background), e o log so aparece em blocos grandes.
print = functools.partial(_print_and_log, flush=True)

SNOVIO_TOKEN_URL = "https://api.snov.io/v1/oauth/access_token"
SNOVIO_DO_NOT_EMAIL_URL = "https://api.snov.io/v1/do-not-email-list"

IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Snov.io limita a API a 60 requisicoes/minuto.
SNOVIO_RATE_LIMIT_PER_MIN = int(os.getenv("SNOVIO_RATE_LIMIT_PER_MIN", "60"))
MIN_SECONDS_BETWEEN_REQUESTS = 60 / SNOVIO_RATE_LIMIT_PER_MIN
MAX_RETRIES_PER_BATCH = 8
TOKEN_REFRESH_SECONDS = 3000

# (connect_timeout, read_timeout) - sem isso, uma conexao travada trava a thread
# para sempre, pois requests nao tem timeout por padrao.
HTTP_TIMEOUT = (10, 30)


def env(name, required=True, default=None):
    value = os.getenv(name, default)
    if required and not value:
        sys.exit(f"Variavel de ambiente obrigatoria ausente: {name}")
    return value


# ==============================================================================
# MySQL: contatos inativos (ativo=0)
# ==============================================================================


def _mysql_connect():
    return mysql.connector.connect(
        host=env("DB_HOST"),
        port=int(env("DB_PORT", default="3306")),
        user=env("DB_USER"),
        password=env("DB_PASSWORD"),
        database=env("DB_NAME"),
        connection_timeout=30,
    )


def _validate_identifiers(*pairs):
    for identifier, label in pairs:
        if not IDENTIFIER_RE.match(identifier):
            sys.exit(f"Valor invalido em {label}: {identifier!r}")


def get_inactive_emails(limit=None, only_yesterday=False, desde=None, ate=None):
    table = env("DB_TABLE")
    email_col = env("DB_EMAIL_COLUMN", default="email")
    ativo_col = env("DB_ATIVO_COLUMN", default="ativo")
    needs_date_col = only_yesterday or desde or ate
    date_col = env("DB_DATE_COLUMN", required=needs_date_col, default="data_alteracao")

    pairs = [(table, "DB_TABLE"), (email_col, "DB_EMAIL_COLUMN"), (ativo_col, "DB_ATIVO_COLUMN")]
    if needs_date_col:
        pairs.append((date_col, "DB_DATE_COLUMN"))
    _validate_identifiers(*pairs)

    conn = _mysql_connect()
    try:
        cursor = conn.cursor()
        query = f"SELECT `{email_col}` FROM `{table}` WHERE `{ativo_col}` = 0"
        params_list = []
        if only_yesterday:
            query += f" AND DATE(`{date_col}`) = CURDATE() - INTERVAL 1 DAY"
        elif desde or ate:
            if desde:
                query += f" AND DATE(`{date_col}`) >= %s"
                params_list.append(desde)
            if ate:
                query += f" AND DATE(`{date_col}`) <= %s"
                params_list.append(ate)
        if limit:
            query += " LIMIT %s"
            params_list.append(limit)
        cursor.execute(query, tuple(params_list))
        emails = [row[0] for row in cursor.fetchall() if row[0]]
        cursor.close()
        return emails
    finally:
        conn.close()


def get_inactive_email_date_breakdown(only_yesterday=False, desde=None, ate=None):
    table = env("DB_TABLE")
    ativo_col = env("DB_ATIVO_COLUMN", default="ativo")
    date_col = env("DB_DATE_COLUMN", default="data_alteracao")
    _validate_identifiers((table, "DB_TABLE"), (ativo_col, "DB_ATIVO_COLUMN"), (date_col, "DB_DATE_COLUMN"))

    conn = _mysql_connect()
    try:
        cursor = conn.cursor()
        query = f"SELECT DATE(`{date_col}`) AS d, COUNT(*) FROM `{table}` WHERE `{ativo_col}` = 0"
        params_list = []
        if only_yesterday:
            query += f" AND DATE(`{date_col}`) = CURDATE() - INTERVAL 1 DAY"
        elif desde or ate:
            if desde:
                query += f" AND DATE(`{date_col}`) >= %s"
                params_list.append(desde)
            if ate:
                query += f" AND DATE(`{date_col}`) <= %s"
                params_list.append(ate)
        query += " GROUP BY d ORDER BY d"
        cursor.execute(query, tuple(params_list))
        breakdown = [(str(d), count) for d, count in cursor.fetchall() if d is not None]
        cursor.close()
        return breakdown
    finally:
        conn.close()


# ==============================================================================
# snov-am-api: contas Snov.io ativas + credenciais + list_ids (identico ao
# negativacao_mailgun.py - mesma fonte de contas pros dois pipelines)
# ==============================================================================


def fetch_active_snov_accounts(base_url, api_key):
    accounts = []
    page = 1
    page_size = 100
    while True:
        resp = requests.get(
            f"{base_url}/api/accounts",
            params={"status": "ACTIVE", "page": page, "page_size": page_size},
            headers={"X-API-Key": api_key},
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
        accounts.extend(items)
        if not items or len(accounts) >= data.get("total", len(accounts)):
            break
        page += 1
    return accounts


def fetch_snov_credentials(base_url, api_key, account_id):
    resp = requests.get(
        f"{base_url}/api/internal/accounts/{account_id}/credentials",
        headers={"X-API-Key": api_key},
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


# ==============================================================================
# Snov.io: token + envio para a Lista de e-mails a nao enviar
# ==============================================================================


def get_access_token(client_id, client_secret):
    resp = requests.post(
        SNOVIO_TOKEN_URL,
        data={"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret},
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_access_token_with_retry(client_id, client_secret, log_prefix=""):
    for attempt in range(1, MAX_RETRIES_PER_BATCH + 1):
        try:
            return get_access_token(client_id, client_secret)
        except requests.exceptions.RequestException as exc:
            backoff = min(60, 2**attempt)
            print(
                f"{log_prefix}Falha ao obter/renovar token Snov.io ({exc}), "
                f"tentativa {attempt}/{MAX_RETRIES_PER_BATCH}. Aguardando {backoff:.0f}s..."
            )
            time.sleep(backoff)
    raise RuntimeError("Nao foi possivel obter o token de acesso do Snov.io apos varias tentativas.")


class TokenHolder:
    def __init__(self, client_id, client_secret, log_prefix=""):
        self.client_id = client_id
        self.client_secret = client_secret
        self.log_prefix = log_prefix
        self.token = get_access_token_with_retry(client_id, client_secret, log_prefix)
        self.obtained_at = time.monotonic()

    def get(self):
        if time.monotonic() - self.obtained_at > TOKEN_REFRESH_SECONDS:
            self.token = get_access_token_with_retry(self.client_id, self.client_secret, self.log_prefix)
            self.obtained_at = time.monotonic()
        return self.token


def send_to_do_not_email_list(token_holder, list_id, emails, batch_size, log_prefix="", failed_log_path=None):
    total_added = 0
    all_duplicates = []
    failed_emails = []
    last_request_time = None
    total_batches = (len(emails) + batch_size - 1) // batch_size

    for i in range(0, len(emails), batch_size):
        batch = emails[i : i + batch_size]
        batch_num = i // batch_size + 1
        batch_ok = False

        for attempt in range(1, MAX_RETRIES_PER_BATCH + 1):
            if last_request_time is not None:
                elapsed = time.monotonic() - last_request_time
                wait = MIN_SECONDS_BETWEEN_REQUESTS - elapsed
                if wait > 0:
                    time.sleep(wait)
            last_request_time = time.monotonic()

            try:
                resp = requests.post(
                    SNOVIO_DO_NOT_EMAIL_URL,
                    data={"access_token": token_holder.get(), "listId": list_id, "items[]": batch},
                    timeout=HTTP_TIMEOUT,
                )
            except requests.exceptions.RequestException as exc:
                backoff = min(60, 2**attempt)
                print(
                    f"{log_prefix}Falha no lote {batch_num} (erro de conexao: {exc}), "
                    f"tentativa {attempt}/{MAX_RETRIES_PER_BATCH}. Aguardando {backoff:.0f}s..."
                )
                time.sleep(backoff)
                continue

            transient_http_error = resp.status_code == 429 or resp.status_code >= 500
            entries = None
            api_reported_failure = False
            if not transient_http_error:
                resp.raise_for_status()
                result = resp.json()
                entries = result if isinstance(result, list) else [result]
                api_reported_failure = any(not entry.get("success") for entry in entries)

            if transient_http_error or api_reported_failure:
                backoff = min(60, 2**attempt)
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        backoff = max(backoff, float(retry_after))
                    except ValueError:
                        pass
                reason = f"HTTP {resp.status_code}" if transient_http_error else f"resposta {entries}"
                print(
                    f"{log_prefix}Falha no lote {batch_num} ({reason}), "
                    f"tentativa {attempt}/{MAX_RETRIES_PER_BATCH}. Aguardando {backoff:.0f}s..."
                )
                time.sleep(backoff)
                continue

            for entry in entries:
                duplicates = entry.get("data", {}).get("duplicates", [])
                all_duplicates.extend(duplicates)

            total_added += len(batch)
            batch_ok = True
            break

        if not batch_ok:
            print(f"{log_prefix}ERRO: lote {batch_num} falhou apos {MAX_RETRIES_PER_BATCH} tentativas, descartado.")
            failed_emails.extend(batch)

        if batch_num == 1 or batch_num % 20 == 0 or batch_num == total_batches:
            print(f"{log_prefix}Lote {batch_num}/{total_batches}: {total_added} e-mail(s) enviados ate agora.")

    if failed_emails and failed_log_path:
        with open(failed_log_path, "w", encoding="utf-8") as f:
            f.write("\n".join(failed_emails))
        print(f"{log_prefix}{len(failed_emails)} e-mail(s) nao processados, salvos em {failed_log_path}")

    return total_added, all_duplicates, failed_emails


def process_account(account, email_list, batch_size, credentials_api_url, credentials_api_key):
    account_label = account.get("email") or account.get("id")
    prefix = f"[{account_label}] "
    list_ids = account.get("list_ids") or []

    try:
        creds = fetch_snov_credentials(credentials_api_url, credentials_api_key, account["id"])
        token_holder = TokenHolder(creds["snov_id"], creds["snov_secret"], log_prefix=prefix)
    except Exception as exc:
        msg = str(exc)
        print(f"{prefix}ERRO ao obter credenciais/token: {msg}")
        return [(account_label, list_id, 0, 0, 0, msg) for list_id in list_ids]

    results = []
    for list_id in list_ids:
        lp = f"{prefix}[list={list_id}] "
        failed_log_path = f"falhas_sendgrid_{account['id']}_{list_id}.txt"
        try:
            added, duplicates, failed = send_to_do_not_email_list(
                token_holder, list_id, email_list, batch_size, log_prefix=lp, failed_log_path=failed_log_path
            )
            print(f"{lp}Concluido: {added} enviados, {len(duplicates)} duplicados, {len(failed)} falharam.")
            results.append((account_label, list_id, added, len(duplicates), len(failed), None))
        except requests.HTTPError as exc:
            msg = f"{exc} -> {exc.response.text[:300]}"
            print(f"{lp}ERRO: {msg}")
            results.append((account_label, list_id, 0, 0, 0, msg))
        except Exception as exc:
            msg = str(exc)
            print(f"{lp}ERRO: {msg}")
            results.append((account_label, list_id, 0, 0, 0, msg))

    return results


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="So mostra contagens e contas/list_ids alvo, sem enviar nada.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limita o numero de e-mails buscados no banco (util para testes).",
    )
    parser.add_argument(
        "--todos",
        action="store_true",
        help="Busca todos os contatos com ativo=0, sem filtrar por data.",
    )
    parser.add_argument(
        "--desde",
        type=str,
        default=None,
        help="Data inicial (YYYY-MM-DD) do filtro por data, inclusive. Ignora --todos/ontem.",
    )
    parser.add_argument(
        "--ate",
        type=str,
        default=None,
        help="Data final (YYYY-MM-DD) do filtro por data, inclusive. Ignora --todos/ontem.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=int(os.getenv("PIPELINE_WORKERS", "5")),
        help="Quantas contas Snov.io processar em paralelo.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    for value, label in ((args.desde, "--desde"), (args.ate, "--ate")):
        if value and not DATE_RE.match(value):
            sys.exit(f"Valor invalido em {label}: {value!r} (esperado YYYY-MM-DD)")

    use_range = bool(args.desde or args.ate)
    only_yesterday = not args.todos and not use_range
    mode = "range" if use_range else ("ontem" if only_yesterday else "todos")

    credentials_api_url = env("CREDENTIALS_API_URL").rstrip("/")
    credentials_api_key = env("CREDENTIALS_API_KEY")
    batch_size = int(env("SNOVIO_BATCH_SIZE", default="100"))

    run_id = db.sendgrid_start_run(mode=mode, desde=args.desde, ate=args.ate, dry_run=args.dry_run)
    print(f"Run #{run_id} iniciado.\n")

    try:
        if use_range:
            periodo = f"{args.desde or '...'} ate {args.ate or '...'}"
            print(f"Buscando contatos inativos (ativo=0) no periodo {periodo} no banco de dados...")
        elif only_yesterday:
            print("Buscando contatos inativos (ativo=0) do dia anterior no banco de dados...")
        else:
            print("Buscando todos os contatos inativos (ativo=0) no banco de dados...")

        emails = get_inactive_emails(limit=args.limit, only_yesterday=only_yesterday, desde=args.desde, ate=args.ate)

        date_breakdown = get_inactive_email_date_breakdown(only_yesterday=only_yesterday, desde=args.desde, ate=args.ate)
        for date, count in date_breakdown:
            db.sendgrid_record_date_breakdown(run_id, date, count)

        if not emails:
            print("Nenhum contato com ativo=0 encontrado. Nada a fazer.")
            db.sendgrid_finish_run(run_id, status="completed", total_emails=0)
            return

        print(f"{len(emails)} e-mail(s) encontrado(s) para negativacao.\n")

        print("Buscando contas Snov.io ativas no snov-am-api...")
        accounts = fetch_active_snov_accounts(credentials_api_url, credentials_api_key)
        targets = [(a, lid) for a in accounts for lid in (a.get("list_ids") or [])]
        print(f"{len(accounts)} conta(s) Snov.io ativa(s), {len(targets)} lista(s) (list_id) no total.\n")

        contas_sem_list_id = [a.get("email") or a.get("id") for a in accounts if not a.get("list_ids")]
        if contas_sem_list_id:
            print(f"Contas ativas SEM list_ids cadastrado (nada sera enviado para elas): {contas_sem_list_id}\n")

        if args.dry_run:
            db.sendgrid_finish_run(run_id, status="completed", total_emails=len(emails))
            print("--dry-run: nada foi enviado.")
            return

        if not targets:
            db.sendgrid_finish_run(run_id, status="completed", total_emails=len(emails))
            print("Nenhuma lista Snov (list_id) encontrada nas contas ativas. Nada a fazer.")
            return

        flat_results = []
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [
                executor.submit(process_account, account, emails, batch_size, credentials_api_url, credentials_api_key)
                for account in accounts
                if account.get("list_ids")
            ]
            for future in as_completed(futures):
                flat_results.extend(future.result())

        print("\nResumo final:")
        for account_label, list_id, added, duplicates, failed, error in sorted(flat_results, key=lambda r: (str(r[0]), str(r[1]))):
            db.sendgrid_record_account_result(run_id, str(account_label), list_id, added, duplicates, failed, error)
            if error:
                print(f"  {account_label} [list={list_id}]: ERRO -> {error}")
            else:
                extra = f", {failed} falharam" if failed else ""
                print(f"  {account_label} [list={list_id}]: {added} enviados, {duplicates} duplicados{extra}")

        db.sendgrid_finish_run(run_id, status="completed", total_emails=len(emails))
    except Exception as exc:
        db.sendgrid_finish_run(run_id, status="failed", error=str(exc))
        raise


if __name__ == "__main__":
    main()
