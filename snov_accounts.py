"""
Contas Snov.io (campo "email" no snov-am-api) que a negativacao manual pode
usar. Conta ativa fora desta lista e ignorada; conta da lista que nao existir
ou estiver inativa no snov-am-api simplesmente nao e usada.

Compartilhado por app.py (valida o pedido na API) e negativacao_manual.py
(filtra as contas na execucao) - fica em modulo proprio porque importar
negativacao_manual dentro do app.py abriria um arquivo de log a cada import.
"""

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


def normalize_accounts(raw):
    """Aceita lista de e-mails (ou string separada por virgula) e devolve um set em minusculas."""
    if isinstance(raw, str):
        raw = raw.split(",")
    return {item.strip().lower() for item in (raw or []) if item and item.strip()}


# Segundo nivel comum antes do TLD (mktxpto.com.br -> mktxpto).
_SECOND_LEVEL = {"com", "net", "org", "gov", "edu", "co", "ind", "eco", "adv"}
_SKIP_KEYS = {"id", "list_ids"}


def domain_term(mailbox):
    """Trecho identificador do dominio de uma caixa: contato@mktxpto.com.br -> 'mktxpto'."""
    domain = (mailbox or "").strip().lower().rsplit("@", 1)[-1]
    labels = [label for label in domain.split(".") if label]
    if len(labels) > 1:
        labels.pop()  # TLD
    if len(labels) > 1 and labels[-1] in _SECOND_LEVEL:
        labels.pop()
    return labels[-1] if labels else ""


def _strings(value):
    if isinstance(value, str):
        # Campo que parece e-mail: so a parte local conta - o dominio da conta
        # Snov.io (ex.: @ativa.ai) e comum a todas e casaria com tudo.
        yield value.split("@", 1)[0] if "@" in value else value
    elif isinstance(value, dict):
        for key, item in value.items():
            if key not in _SKIP_KEYS:
                yield from _strings(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _strings(item)


def account_matches_term(account, term):
    return bool(term) and any(term in text.lower() for text in _strings(account))


def pick_accounts_for_mailbox(accounts, mailbox):
    """
    Contas Snov.io donas de uma caixa, dentro da lista permitida:
      1) conta com e-mail igual ao da caixa; senao
      2) contas cujo qualquer campo contem o trecho do dominio da caixa.
    Retorna (contas, descricao_do_criterio).
    """
    mailbox = (mailbox or "").strip().lower()
    allowed = [a for a in accounts if (a.get("email") or "").strip().lower() in ALLOWED_SNOV_ACCOUNTS]

    exact = [a for a in allowed if (a.get("email") or "").strip().lower() == mailbox]
    if exact:
        return exact, f"e-mail igual ao da caixa ({mailbox})"

    term = domain_term(mailbox)
    if len(term) < 3:
        return [], f"trecho de dominio muito curto para pesquisar ({term!r})"
    return [a for a in allowed if account_matches_term(a, term)], f"trecho do dominio {term!r}"
