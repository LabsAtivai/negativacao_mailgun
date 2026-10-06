"""
Negativa manualmente um unico e-mail ou dominio (digitado na aba "Manual" do
painel) na Lista de e-mails a nao enviar de TODAS as contas Snov.io ativas
cadastradas no snov-am-api - mesma fonte de contas que os 3 pipelines
automaticos (negativacao_mailgun.py, negativacao_sendgrid.py,
negativacao_postal.py) usam, via CREDENTIALS_API_URL/CREDENTIALS_API_KEY.

Disparado sob demanda por POST /api/manual/runs/trigger, um valor por vez -
sem fonte externa (Mailgun/SendGrid/Postal).

Um dominio inteiro (ex.: "spamtrap.com") e negativado no formato "@dominio"
aceito pela Lista de e-mails a nao enviar da Snov.io, que bloqueia qualquer
endereco daquele dominio. Um e-mail completo negativa so aquele endereco.

Uso:
    python negativacao_manual.py --value fulano@dominio.com
    python negativacao_manual.py --value dominio-ruim.com
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

import requests
from dotenv import load_dotenv

import db

load_dotenv()

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOG_DIR, f"negativacao_manual_{datetime.datetime.now():%Y%m%d_%H%M%S}.txt")
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

SNOVIO_RATE_LIMIT_PER_MIN = int(os.getenv("SNOVIO_RATE_LIMIT_PER_MIN", "60"))
MIN_SECONDS_BETWEEN_REQUESTS = 60 / SNOVIO_RATE_LIMIT_PER_MIN
MAX_RETRIES_PER_BATCH = 8
TOKEN_REFRESH_SECONDS = 3000

# (connect_timeout, read_timeout) - sem isso, uma conexao travada trava a thread
# para sempre, pois requests nao tem timeout por padrao.
HTTP_TIMEOUT = (10, 30)

# A negativacao manual so vai para estas contas Snov.io (campo "email" no
# snov-am-api). Conta ativa fora desta lista e ignorada; conta da lista que
# nao existir/estiver inativa no snov-am-api simplesmente nao e usada.
ALLOWED_SNOV_ACCOUNTS = frozenset(
    email.lower()
    for email in (
        "sdr@ativa.ai",
        "sdr2@ativa.ai",
        "sdr3@ativa.ai",
        "sdr4@ativa.ai",
        "sdr5@ativa.ai",
        "sdr6@ativa.ai",
        "sdr7@ativa.ai",
        "sdr8@ativa.ai",
        "sdr9@ativa.ai",
        "sdr10@ativa.ai",
        "salesops@ativa.ai",
        "salesops2@ativa.ai",
        "proativa.io@ativa.ai",
        "proativa.io2@ativa.ai",
        "sdrremoto@ativa.ai",
        "sdrremoto2@ativa.ai",
    )
)

EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
DOMAIN_RE = re.compile(r"^([a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z]{2,}$", re.IGNORECASE)


def env(name, required=True, default=None):
    value = os.getenv(name, default)
    if required and not value:
        sys.exit(f"Variavel de ambiente obrigatoria ausente: {name}")
    return value


def normalize_value(raw):
    """Um e-mail completo negativa so aquele endereco. Um dominio sozinho
    (com ou sem "@" na frente) negativa TODO o dominio - convertido pro
    formato "@dominio" que a Lista de e-mails a nao enviar da Snov.io trata
    como bloqueio de dominio inteiro, nao de um endereco literal."""
    value = raw.strip().lower()
    if EMAIL_RE.match(value):
        return value, "email"
    domain = value[1:] if value.startswith("@") else value
    if DOMAIN_RE.match(domain):
        return f"@{domain}", "domain"
    raise ValueError(f"Valor invalido, nao parece e-mail nem dominio: {raw!r}")


# ==============================================================================
# snov-am-api: contas Snov.io ativas + credenciais + list_ids (identico aos
# outros 3 pipelines - mesma fonte de contas para todos)
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


def send_single_to_do_not_email_list(token_holder, list_id, value, log_prefix=""):
    """Versao de item unico do envio (sem lote/paginacao - so faz sentido
    aqui porque a negativacao manual e sempre um valor por vez)."""
    last_request_time = None
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
                data={"access_token": token_holder.get(), "listId": list_id, "items[]": [value]},
                timeout=HTTP_TIMEOUT,
            )
        except requests.exceptions.RequestException as exc:
            backoff = min(60, 2**attempt)
            print(f"{log_prefix}Falha (erro de conexao: {exc}), tentativa {attempt}/{MAX_RETRIES_PER_BATCH}. Aguardando {backoff:.0f}s...")
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
            print(f"{log_prefix}Falha ({reason}), tentativa {attempt}/{MAX_RETRIES_PER_BATCH}. Aguardando {backoff:.0f}s...")
            time.sleep(backoff)
            continue

        duplicates = []
        for entry in entries:
            duplicates.extend(entry.get("data", {}).get("duplicates", []))
        return 1, duplicates, []

    print(f"{log_prefix}ERRO: falhou apos {MAX_RETRIES_PER_BATCH} tentativas, descartado.")
    return 0, [], [value]


def process_account(account, value, credentials_api_url, credentials_api_key):
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
        try:
            added, duplicates, failed = send_single_to_do_not_email_list(token_holder, list_id, value, log_prefix=lp)
            print(f"{lp}Concluido: {added} enviado(s), {len(duplicates)} duplicado(s), {len(failed)} falhou(aram).")
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
    parser.add_argument("--value", required=True, help="E-mail ou dominio a negativar em todas as contas Snov.io.")
    parser.add_argument(
        "--workers",
        type=int,
        default=int(os.getenv("PIPELINE_WORKERS", "5")),
        help="Quantas contas Snov.io processar em paralelo.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    try:
        value, kind = normalize_value(args.value)
    except ValueError as exc:
        sys.exit(str(exc))

    credentials_api_url = env("CREDENTIALS_API_URL").rstrip("/")
    credentials_api_key = env("CREDENTIALS_API_KEY")

    run_id = db.manual_start_run(value, kind)
    print(f"Run #{run_id} iniciado: negativando {kind} {value!r} em todas as contas Snov.io ativas.\n")

    try:
        print("Buscando contas Snov.io ativas no snov-am-api...")
        all_accounts = fetch_active_snov_accounts(credentials_api_url, credentials_api_key)
        accounts = [a for a in all_accounts if (a.get("email") or "").strip().lower() in ALLOWED_SNOV_ACCOUNTS]
        print(f"{len(all_accounts)} conta(s) ativa(s) no snov-am-api, {len(accounts)} na lista permitida.")
        ausentes = sorted(ALLOWED_SNOV_ACCOUNTS - {(a.get("email") or "").strip().lower() for a in accounts})
        if ausentes:
            print(f"Permitidas ausentes/inativas no snov-am-api (ignoradas): {ausentes}")
        targets = [(a, lid) for a in accounts for lid in (a.get("list_ids") or [])]
        print(f"{len(accounts)} conta(s) Snov.io ativa(s), {len(targets)} lista(s) (list_id) no total.\n")

        contas_sem_list_id = [a.get("email") or a.get("id") for a in accounts if not a.get("list_ids")]
        if contas_sem_list_id:
            print(f"Contas ativas SEM list_ids cadastrado (nada sera enviado para elas): {contas_sem_list_id}\n")

        if not targets:
            db.manual_finish_run(run_id, status="completed", total_emails=1)
            print("Nenhuma lista Snov (list_id) encontrada nas contas ativas. Nada a fazer.")
            return

        flat_results = []
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [
                executor.submit(process_account, account, value, credentials_api_url, credentials_api_key)
                for account in accounts
                if account.get("list_ids")
            ]
            for future in as_completed(futures):
                flat_results.extend(future.result())

        print("\nResumo final:")
        for account_label, list_id, added, duplicates, failed, error in sorted(flat_results, key=lambda r: (str(r[0]), str(r[1]))):
            db.manual_record_account_result(run_id, str(account_label), list_id, added, duplicates, failed, error)
            if error:
                print(f"  {account_label} [list={list_id}]: ERRO -> {error}")
            else:
                extra = f", {failed} falharam" if failed else ""
                print(f"  {account_label} [list={list_id}]: {added} enviados, {duplicates} duplicados{extra}")

        db.manual_finish_run(run_id, status="completed", total_emails=1)
    except Exception as exc:
        db.manual_finish_run(run_id, status="failed", error=str(exc))
        raise


if __name__ == "__main__":
    main()
