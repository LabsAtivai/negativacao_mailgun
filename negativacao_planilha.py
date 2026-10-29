"""
Le a planilha de negativacao (Google Sheets, aba "Negativacao Geral") e envia
para a Lista de e-mails a nao enviar de 5 contas Snov.io especificas
(snov_accounts.SHEET_SNOV_ACCOUNTS) tudo que ainda nao estiver marcado como
"OK" na coluna Negativado. Roda todo dia as 00:00 (agendado em app.py).

Colunas da planilha: Email, nome, Observacao, Data, Negativado. So Email e
Negativado sao usadas.
  - E-mail: negativa aquele endereco.
  - Dominio ("@dominio.com" ou "dominio.com"): a API de Domain Search da
    Snov.io lista os e-mails do dominio e todos eles sao negativados.
  - Provedor de e-mail livre (gmail.com etc.) e valores que nao sao nem
    e-mail nem dominio: nao sao processados e ficam sem marca.
Depois de enviar, grava "OK" nas linhas processadas com sucesso. Se qualquer
conta/lista falhar, nenhuma linha e marcada (a proxima run reenvia tudo; a
Snov.io trata e-mail repetido como duplicado).

Uso:
    python negativacao_planilha.py --dry-run   # le a planilha e mostra contagens, sem Snov.io nem escrita
    python negativacao_planilha.py             # roda de verdade
"""
import argparse
import builtins
import datetime
import functools
import os
import re
import sys
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from dotenv import load_dotenv

import db
from snov_accounts import SHEET_SNOV_ACCOUNTS

load_dotenv()

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOG_DIR, f"negativacao_planilha_{datetime.datetime.now():%Y%m%d_%H%M%S}.txt")
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
SNOVIO_DOMAIN_EMAILS_START_URL = "https://api.snov.io/v2/domain-search/domain-emails/start"
SNOVIO_DOMAIN_EMAILS_RESULT_URL = "https://api.snov.io/v2/domain-search/domain-emails/result/{task_hash}"

SNOVIO_RATE_LIMIT_PER_MIN = int(os.getenv("SNOVIO_RATE_LIMIT_PER_MIN", "60"))
MIN_SECONDS_BETWEEN_REQUESTS = 60 / SNOVIO_RATE_LIMIT_PER_MIN
MAX_RETRIES_PER_BATCH = 8
TOKEN_REFRESH_SECONDS = 3000
DOMAIN_POLL_SECONDS = 3
DOMAIN_POLL_MAX_ATTEMPTS = 40

# (connect_timeout, read_timeout) - sem isso, uma conexao travada trava a thread
# para sempre, pois requests nao tem timeout por padrao.
HTTP_TIMEOUT = (10, 30)

EMAIL_FIND_RE = re.compile(r"[^\s<>@,;\"']+@[^\s<>@,;\"']+\.[^\s<>@,;\"']+")
DOMAIN_RE = re.compile(r"^([a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z]{2,}$")
# Valida o e-mail inteiro: pega TLD truncado ("...com.b") e caracteres que a Snov.io rejeita
# com 400 "items.N must be a valid email address or domain".
STRICT_EMAIL_RE = re.compile(r"^[a-z0-9._%+\-]+@([a-z0-9](?:[a-z0-9\-]*[a-z0-9])?\.)+[a-z]{2,}$")

# Dominios de e-mail livre: o Domain Search nao lista "todos os usuarios"
# deles, so gastaria credito.
_DEFAULT_FREE_DOMAINS = (
    "gmail.com,googlemail.com,outlook.com,hotmail.com,live.com,msn.com,yahoo.com,yahoo.com.br,"
    "icloud.com,me.com,aol.com,uol.com.br,bol.com.br,terra.com.br,ig.com.br,globo.com,protonmail.com"
)
FREE_DOMAINS = frozenset(
    d.strip().lower() for d in (os.getenv("DOMAIN_SKIP") or _DEFAULT_FREE_DOMAINS).split(",") if d.strip()
)

OK_MARK = "OK"


def env(name, required=True, default=None):
    value = os.getenv(name, default)
    if required and not value:
        sys.exit(f"Variavel de ambiente obrigatoria ausente: {name}")
    return value


# ==============================================================================
# Planilha (Google Sheets)
# ==============================================================================


def _norm_header(text):
    text = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode()
    return text.strip().lower()


def open_worksheet():
    import gspread  # import tardio: so este pipeline precisa da dependencia

    key_path = env("GOOGLE_SERVICE_ACCOUNT_JSON")
    client = gspread.service_account(filename=key_path)
    sheet = client.open_by_key(env("SHEET_ID"))
    return sheet.worksheet(env("SHEET_TAB", default="Negativação Geral"))


def read_rows(ws):
    """Linhas com algum valor no Email e Negativado vazio.
    Retorna [(numero_da_linha, valor_bruto)], mais o indice (1-based) das colunas Email e Negativado."""
    values = ws.get_all_values()
    if not values:
        raise RuntimeError("Planilha vazia")
    header = [_norm_header(h) for h in values[0]]
    try:
        email_idx = header.index("email")
        mark_idx = header.index("negativado")
    except ValueError:
        raise RuntimeError(f"Cabecalho sem coluna Email/Negativado: {values[0]}")

    rows = []
    for number, row in enumerate(values[1:], start=2):
        raw = row[email_idx] if email_idx < len(row) else ""
        mark = row[mark_idx] if mark_idx < len(row) else ""
        if raw.strip() and not mark.strip():
            rows.append((number, raw))
    return rows, email_idx + 1, mark_idx + 1


def classify(raw):
    """('email', valor) | ('domain', valor) | ('skip', motivo). Limpa espacos, <>, lixo colado e
    maiusculas; se a celula tem varios enderecos, usa o primeiro."""
    text = raw.strip().lower()
    match = EMAIL_FIND_RE.search(text)
    if match:
        email = match.group(0).strip(".")
        if not STRICT_EMAIL_RE.match(email):
            return "skip", f"e-mail invalido ({email})"
        return "email", email
    domain = text.strip(" <>\"'\\")
    domain = domain[1:] if domain.startswith("@") else domain
    if DOMAIN_RE.match(domain):
        if domain in FREE_DOMAINS:
            return "skip", f"dominio de e-mail livre ({domain})"
        return "domain", domain
    return "skip", "nao e e-mail nem dominio"


def mark_rows_ok(ws, rows_to_mark, email_col, mark_col):
    """Grava OK em lote. So marca a linha se o e-mail dela ainda for o mesmo lido no inicio da run
    (protege de linhas inseridas/removidas durante a execucao)."""
    from gspread.utils import rowcol_to_a1

    current = ws.col_values(email_col)
    updates = []
    skipped = 0
    for number, raw in rows_to_mark:
        if number - 1 < len(current) and current[number - 1] == raw:
            updates.append({"range": rowcol_to_a1(number, mark_col), "values": [[OK_MARK]]})
        else:
            skipped += 1
    for i in range(0, len(updates), 500):
        ws.batch_update(updates[i : i + 500])
    return len(updates), skipped


# ==============================================================================
# snov-am-api: contas Snov.io ativas + credenciais + list_ids
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
# Snov.io: token, Domain Search e envio para a Lista de e-mails a nao enviar
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


def _extract_emails(payload):
    """Os e-mails ficam numa lista de objetos com a chave "email"; o nome da lista varia por versao
    da API, entao procura em "data"/"emails"/"result"."""
    found = []
    for key in ("data", "emails", "result"):
        items = payload.get(key)
        if isinstance(items, list):
            for item in items:
                value = item.get("email") if isinstance(item, dict) else item
                if isinstance(value, str) and "@" in value:
                    found.append(value.strip().lower())
    return found


def _next_start(payload, domain):
    """Proxima pagina da Domain Search: links.next e uma URL de START (POST, gera novo task_hash e
    custa 1 credito), nao de resultado. Sem ela, monta a partir de meta.next (cursor)."""
    links = payload.get("links")
    nxt = links.get("next") if isinstance(links, dict) else None
    if nxt:
        return nxt, None
    meta = payload.get("meta")
    cursor = meta.get("next") if isinstance(meta, dict) else None
    if cursor:
        return SNOVIO_DOMAIN_EMAILS_START_URL, {"domain": domain, "next": cursor}
    return None, None


def search_domain_emails(token_holder, domain, log_prefix=""):
    """Todos os e-mails do dominio pela Domain Search da Snov.io (assincrona: start + result, paginada
    de 50 em 50). Levanta excecao se a busca falhar - a linha fica sem marca."""
    headers = lambda: {"Authorization": f"Bearer {token_holder.get()}"}  # noqa: E731

    emails = []
    start_url, params = SNOVIO_DOMAIN_EMAILS_START_URL, {"domain": domain}
    while start_url:
        resp = requests.post(start_url, params=params, headers=headers(), timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        start = resp.json()
        task_hash = start.get("meta", {}).get("task_hash") or start.get("task_hash")
        links = start.get("links")
        url = (links.get("result") if isinstance(links, dict) else None) or start.get("result")
        if not url:
            if not task_hash:
                raise RuntimeError(f"Domain Search sem task_hash/result: {start}")
            url = SNOVIO_DOMAIN_EMAILS_RESULT_URL.format(task_hash=task_hash)

        for _ in range(DOMAIN_POLL_MAX_ATTEMPTS):
            time.sleep(max(DOMAIN_POLL_SECONDS, MIN_SECONDS_BETWEEN_REQUESTS))
            resp = requests.get(url, headers=headers(), timeout=HTTP_TIMEOUT)
            if resp.status_code == 429 or resp.status_code >= 500:
                continue
            resp.raise_for_status()
            payload = resp.json()
            if payload.get("status") != "in_progress":
                break
        else:
            raise RuntimeError("Domain Search nao concluiu a tempo")
        emails.extend(e for e in _extract_emails(payload) if STRICT_EMAIL_RE.match(e))
        start_url, params = _next_start(payload, domain)
    return sorted(set(emails))


def send_to_do_not_email_list(token_holder, list_id, emails, batch_size, log_prefix=""):
    total_added = 0
    all_duplicates = []
    failed_emails = []
    rejected_emails = []  # a Snov.io disse que nao e e-mail/dominio valido (400 items.N): descartados, nao reenviados
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

            if resp.status_code == 400:
                bad = re.search(r"items\.(\d+)", resp.text)
                if bad and int(bad.group(1)) < len(batch):
                    dropped = batch.pop(int(bad.group(1)))
                    rejected_emails.append(dropped)
                    print(f"{log_prefix}Snov.io rejeitou {dropped!r} ({resp.text[:120]}); descartado, reenviando o lote.")
                    last_request_time = None
                    if not batch:
                        batch_ok = True
                        break
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
                all_duplicates.extend(entry.get("data", {}).get("duplicates", []))

            total_added += len(batch)
            batch_ok = True
            break

        if not batch_ok:
            print(f"{log_prefix}ERRO: lote {batch_num} falhou apos {MAX_RETRIES_PER_BATCH} tentativas, descartado.")
            failed_emails.extend(batch)

        if batch_num == 1 or batch_num % 20 == 0 or batch_num == total_batches:
            print(f"{log_prefix}Lote {batch_num}/{total_batches}: {total_added} e-mail(s) enviados ate agora.")

    return total_added, all_duplicates, failed_emails, rejected_emails


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
        return [(account_label, list_id, 0, 0, 0, msg, []) for list_id in list_ids]

    results = []
    for list_id in list_ids:
        lp = f"{prefix}[list={list_id}] "
        try:
            added, duplicates, failed, rejected = send_to_do_not_email_list(token_holder, list_id, email_list, batch_size, log_prefix=lp)
            print(f"{lp}Concluido: {added} enviados, {len(duplicates)} duplicados, {len(failed)} falharam, {len(rejected)} rejeitados.")
            results.append((account_label, list_id, added, len(duplicates), len(failed), None, rejected))
        except requests.HTTPError as exc:
            msg = f"{exc} -> {exc.response.text[:300]}"
            print(f"{lp}ERRO: {msg}")
            results.append((account_label, list_id, 0, 0, 0, msg, []))
        except Exception as exc:
            msg = str(exc)
            print(f"{lp}ERRO: {msg}")
            results.append((account_label, list_id, 0, 0, 0, msg, []))

    return results


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Le a planilha e mostra contagens, sem Domain Search, sem enviar e sem gravar OK.",
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
    batch_size = int(env("SNOVIO_BATCH_SIZE", required=False, default="100"))
    credentials_api_url = env("CREDENTIALS_API_URL").rstrip("/")
    credentials_api_key = env("CREDENTIALS_API_KEY")

    run_id = db.planilha_start_run(dry_run=args.dry_run)
    print(f"Run #{run_id} iniciado{' (dry-run)' if args.dry_run else ''}.\n")
    stats = {}

    try:
        print("Lendo a planilha...")
        ws = open_worksheet()
        rows, email_col, mark_col = read_rows(ws)
        print(f"{len(rows)} linha(s) sem OK com algum valor na coluna Email.")

        direct_emails = {}   # email -> [numeros de linha]
        domains = {}         # dominio -> [numeros de linha]
        invalid = []
        for number, raw in rows:
            kind, value = classify(raw)
            if kind == "email":
                direct_emails.setdefault(value, []).append(number)
            elif kind == "domain":
                domains.setdefault(value, []).append(number)
            else:
                invalid.append((number, raw, value))
        for number, raw, reason in invalid:
            print(f"Linha {number} ignorada ({reason}): {raw!r}")
        print(f"{len(direct_emails)} e-mail(s) unico(s), {len(domains)} dominio(s) unico(s), {len(invalid)} linha(s) ignorada(s).\n")
        stats.update(rows_new=len(rows), rows_invalid=len(invalid), domains_count=len(domains))

        if not rows or (not direct_emails and not domains):
            db.planilha_finish_run(run_id, status="completed", total_emails=0, rows_marked=0, **stats)
            print("Nada novo para negativar.")
            return

        if args.dry_run:
            db.planilha_finish_run(run_id, status="completed", total_emails=len(direct_emails), rows_marked=0, **stats)
            print("Dry-run: nada buscado, enviado nem marcado.")
            return

        print("Buscando contas Snov.io ativas no snov-am-api...")
        all_accounts = fetch_active_snov_accounts(credentials_api_url, credentials_api_key)
        accounts = [a for a in all_accounts if (a.get("email") or "").strip().lower() in SHEET_SNOV_ACCOUNTS]
        ausentes = sorted(SHEET_SNOV_ACCOUNTS - {(a.get("email") or "").strip().lower() for a in accounts})
        print(f"{len(all_accounts)} conta(s) ativa(s) no snov-am-api, {len(accounts)} das 5 da planilha.")
        if ausentes:
            print(f"AVISO: ausentes/inativas no snov-am-api (ignoradas): {ausentes}")
        accounts = [a for a in accounts if a.get("list_ids")]
        if not accounts:
            raise RuntimeError("Nenhuma das 5 contas da planilha esta ativa e com list_id no snov-am-api")

        # Domain Search: uma busca por dominio, com a primeira conta que tiver credenciais validas.
        expanded = {}   # dominio -> [emails]
        failed_domains = set()
        if domains:
            search_holder = None
            for account in accounts:
                try:
                    creds = fetch_snov_credentials(credentials_api_url, credentials_api_key, account["id"])
                    search_holder = TokenHolder(creds["snov_id"], creds["snov_secret"], log_prefix=f"[{account.get('email')}] ")
                    print(f"Domain Search usando a conta {account.get('email')}.")
                    break
                except Exception as exc:
                    print(f"[{account.get('email')}] sem credenciais para Domain Search: {exc}")
            for domain in sorted(domains):
                try:
                    if search_holder is None:
                        raise RuntimeError("nenhuma conta com credenciais para Domain Search")
                    found = search_domain_emails(search_holder, domain)
                    expanded[domain] = found
                    db.planilha_record_domain_expansion(run_id, domain, len(found))
                    print(f"Dominio {domain}: {len(found)} e-mail(s) encontrado(s).")
                except Exception as exc:
                    failed_domains.add(domain)
                    db.planilha_record_domain_expansion(run_id, domain, 0, error=str(exc)[:300])
                    print(f"Dominio {domain}: ERRO na Domain Search ({exc}); linha(s) ficam sem marca.")

        all_emails = set(direct_emails)
        for found in expanded.values():
            all_emails.update(found)
        email_list = sorted(all_emails)
        print(f"\n{len(email_list)} e-mail(s) unico(s) a negativar em {len(accounts)} conta(s).\n")

        send_ok = True
        rejected_all = set()  # e-mails que a Snov.io recusou: ficam sem marca na planilha
        if email_list:
            flat_results = []
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = [
                    executor.submit(process_account, a, email_list, batch_size, credentials_api_url, credentials_api_key)
                    for a in accounts
                ]
                for future in as_completed(futures):
                    flat_results.extend(future.result())

            print("\nResumo final:")
            for account_label, list_id, added, duplicates, failed, error, rejected in sorted(flat_results, key=lambda r: (str(r[0]), str(r[1]))):
                rejected_all.update(rejected)
                db.planilha_record_account_result(run_id, str(account_label), list_id, added, duplicates, failed, error)
                if error or failed:
                    send_ok = False
                if error:
                    print(f"  {account_label} [list={list_id}]: ERRO -> {error}")
                else:
                    extra = f", {failed} falharam" if failed else ""
                    print(f"  {account_label} [list={list_id}]: {added} enviados, {duplicates} duplicados{extra}")

        rows_marked = 0
        if send_ok:
            to_mark = []
            for n, raw in rows:
                kind, value = classify(raw)
                if (kind == "email" and value not in rejected_all) or (kind == "domain" and value not in failed_domains):
                    to_mark.append((n, raw))
            if rejected_all:
                print(f"\n{len(rejected_all)} e-mail(s) rejeitado(s) pela Snov.io (linhas ficam sem marca): {sorted(rejected_all)[:20]}")
            if to_mark:
                marked, changed = mark_rows_ok(ws, to_mark, email_col, mark_col)
                rows_marked = marked
                print(f"\n{marked} linha(s) marcada(s) como {OK_MARK} na planilha." + (f" {changed} mudaram durante a run e ficaram sem marca." if changed else ""))
        else:
            print("\nHouve falha de envio em alguma conta/lista: nenhuma linha foi marcada; a proxima run reenvia.")

        if send_ok and not failed_domains:
            status, error = "completed", None
        else:
            status = "failed"
            error = (
                ("envio falhou em alguma conta/lista; nenhuma linha marcada. " if not send_ok else "")
                + (f"Domain Search falhou em {len(failed_domains)} dominio(s): {sorted(failed_domains)[:10]}" if failed_domains else "")
            ).strip()
        db.planilha_finish_run(run_id, status=status, total_emails=len(email_list), rows_marked=rows_marked, error=error, **stats)
    except Exception as exc:
        db.planilha_finish_run(run_id, status="failed", error=str(exc), **stats)
        raise


if __name__ == "__main__":
    main()
