"""
Consome os eventos de webhook do Postal acumulados no SQLite local
(postal_events, gravados por POST /api/postal/webhook em app.py) e envia os
e-mails com falha de entrega (MessageDeliveryFailed / MessageHeld /
MessageBounced) para a Lista de e-mails a nao enviar (negativacao) de TODAS
as contas Snov.io ativas cadastradas no snov-am-api - mesma fonte de contas
que negativacao_mailgun.py e negativacao_sendgrid.py usam, via
CREDENTIALS_API_URL/CREDENTIALS_API_KEY.

Diferente do Mailgun (pull via API) e do SendGrid (pull via MySQL), o Postal
so entrega o dado via webhook (push) - por isso este script nao busca nada
externamente, so consome o buffer local que o proprio painel ja recebeu.

Uso:
    python negativacao_postal.py --dry-run   # so mostra contagens, nada enviado
    python negativacao_postal.py             # roda de verdade (so eventos novos)
    python negativacao_postal.py --todos     # roda de verdade (todo o historico de eventos)
"""
import argparse
import builtins
import datetime
import functools
import os
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from dotenv import load_dotenv

import db

load_dotenv()

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOG_DIR, f"negativacao_postal_{datetime.datetime.now():%Y%m%d_%H%M%S}.txt")
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
# snov-am-api: contas Snov.io ativas + credenciais + list_ids (identico aos
# outros dois pipelines - mesma fonte de contas para os tres)
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
        failed_log_path = f"falhas_postal_{account['id']}_{list_id}.txt"
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
        "--todos",
        action="store_true",
        help="Reprocessa TODOS os eventos ja recebidos (inclusive os ja marcados como processados).",
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

    credentials_api_url = env("CREDENTIALS_API_URL").rstrip("/")
    credentials_api_key = env("CREDENTIALS_API_KEY")
    batch_size = int(env("SNOVIO_BATCH_SIZE", default="100"))

    mode = "todos" if args.todos else "novos"
    run_id = db.postal_start_run(mode=mode, dry_run=args.dry_run)
    print(f"Run #{run_id} iniciado.\n")

    try:
        if args.todos:
            print("Buscando TODOS os eventos de webhook do Postal ja recebidos (--todos)...")
            events = db.list_all_postal_events()
        else:
            print("Buscando eventos de webhook do Postal ainda nao processados...")
            events = db.list_unprocessed_postal_events()

        if not events:
            print("Nenhum evento de negativacao pendente. Nada a fazer.")
            db.postal_finish_run(run_id, status="completed", total_emails=0)
            return

        emails = sorted({e["recipient"] for e in events})
        print(f"{len(events)} evento(s) / {len(emails)} e-mail(s) unico(s) encontrado(s) para negativacao.\n")

        breakdown = Counter((e["received_at"][:10], e["event_kind"]) for e in events)
        for (date, kind), count in sorted(breakdown.items()):
            db.postal_record_date_breakdown(run_id, date, kind, count)

        print("Buscando contas Snov.io ativas no snov-am-api...")
        accounts = fetch_active_snov_accounts(credentials_api_url, credentials_api_key)
        targets = [(a, lid) for a in accounts for lid in (a.get("list_ids") or [])]
        print(f"{len(accounts)} conta(s) Snov.io ativa(s), {len(targets)} lista(s) (list_id) no total.\n")

        contas_sem_list_id = [a.get("email") or a.get("id") for a in accounts if not a.get("list_ids")]
        if contas_sem_list_id:
            print(f"Contas ativas SEM list_ids cadastrado (nada sera enviado para elas): {contas_sem_list_id}\n")

        if args.dry_run:
            db.postal_finish_run(run_id, status="completed", total_emails=len(emails))
            print("--dry-run: nada foi enviado, eventos NAO marcados como processados.")
            return

        if not targets:
            db.postal_finish_run(run_id, status="completed", total_emails=len(emails))
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
            db.postal_record_account_result(run_id, str(account_label), list_id, added, duplicates, failed, error)
            if error:
                print(f"  {account_label} [list={list_id}]: ERRO -> {error}")
            else:
                extra = f", {failed} falharam" if failed else ""
                print(f"  {account_label} [list={list_id}]: {added} enviados, {duplicates} duplicados{extra}")

        db.postal_finish_run(run_id, status="completed", total_emails=len(emails))
        db.mark_postal_events_processed([e["id"] for e in events])
    except Exception as exc:
        db.postal_finish_run(run_id, status="failed", error=str(exc))
        raise


if __name__ == "__main__":
    main()
