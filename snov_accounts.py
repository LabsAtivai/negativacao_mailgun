"""
Contas Snov.io (campo "email" no snov-am-api) do TIME COMERCIAL: so a negativacao
vinda da extensao/painel do AtivaWriter (scope "commercial") usa esta lista.
Conta ativa fora dela e ignorada; conta da lista que nao existir ou estiver
inativa no snov-am-api simplesmente nao e usada.

A negativacao do MailHub NAO usa esta lista: vai so para a conta achada pelo
e-mail da conta (pick_accounts_for_mailbox).
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


# Segundo nivel comum antes do TLD (mktxpto.com.br -> mktxpto).
_SECOND_LEVEL = {"com", "net", "org", "gov", "edu", "co", "ind", "eco", "adv"}


def domain_term(mailbox):
    """
    Trecho identificador do dominio de uma caixa, sem o prefixo "mkt" dos
    dominios de marketing: contato@mktadamofilms.com.br -> 'adamofilms'.
    """
    domain = (mailbox or "").strip().lower().rsplit("@", 1)[-1]
    labels = [label for label in domain.split(".") if label]
    if len(labels) > 1:
        labels.pop()  # TLD
    if len(labels) > 1 and labels[-1] in _SECOND_LEVEL:
        labels.pop()
    term = labels[-1] if labels else ""
    # So tira o "mkt" se sobrar um trecho pesquisavel (>= 3 letras).
    if term.startswith("mkt") and len(term) - 3 >= 3:
        term = term[3:]
    return term


def _local(email):
    return (email or "").strip().lower().split("@", 1)[0]


def pick_accounts_for_mailbox(accounts, mailbox):
    """
    Conta(s) Snov.io dona(s) de uma caixa do MailHub, entre TODAS as contas ativas
    (nao usa a lista de 16 - essa e so do time comercial / AtivaWriter).
    A conta do cliente tem o nome do cliente no e-mail: caixa
    danilo_@mktadamofilms.com.br -> trecho 'adamofilms' -> adamofilms@ativa.ai.
    Em ordem, para no primeiro criterio que achar algo:
      1) conta com e-mail igual ao da caixa;
      2) parte local do e-mail da conta igual ao trecho do dominio;
      3) parte local do e-mail da conta contendo o trecho (trecho >= 5 letras).
    Caixa no proprio dominio das contas (@ativa.ai) so usa o criterio 1.
    Retorna (contas, descricao_do_criterio).
    """
    mailbox = (mailbox or "").strip().lower()
    email = lambda a: (a.get("email") or "").strip().lower()  # noqa: E731

    exact = [a for a in accounts if email(a) == mailbox]
    if exact:
        return exact, f"e-mail igual ao da caixa ({mailbox})"

    if mailbox.rsplit("@", 1)[-1].endswith("ativa.ai"):
        return [], "caixa @ativa.ai sem conta Snov.io com o mesmo e-mail"

    term = domain_term(mailbox)
    if len(term) < 3:
        return [], f"trecho de dominio muito curto para pesquisar ({term!r})"

    same = [a for a in accounts if _local(a.get("email")) == term]
    if same:
        return same, f"e-mail da conta = {term}@... (trecho do dominio)"

    if len(term) >= 5:
        like = [a for a in accounts if term in _local(a.get("email"))]
        if like:
            return like, f"e-mail da conta parecido com {term!r} (trecho do dominio)"
    return [], f"nenhum e-mail de conta igual ou parecido com {term!r}"
