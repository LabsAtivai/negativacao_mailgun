# Spec: Negativação via planilha (diária, 00:00)

## Objetivo

Ler diariamente, às 00:00 (America/Sao_Paulo), a planilha de negativação e enviar os e-mails novos para a **Lista de e-mails a não enviar** de 5 contas Snov.io específicas. A planilha continua sendo a fonte da verdade e o ponto de entrada manual do time.

## Contas Snov.io alvo

Fixas, independentes de `ALLOWED_SNOV_ACCOUNTS` e da regra de `pick_accounts_for_mailbox`:

- `sdr10@ativa.ai`
- `salesops3@ativa.ai`
- `sdrremoto2@ativa.ai`
- `proativa.io3@ativa.ai`
- `blackfriday@ativa.ai`

Ficam em `snov_accounts.py` como `SHEET_SNOV_ACCOUNTS` (frozenset em minúsculas). As credenciais e `list_id` continuam vindo do snov-am-api (`fetch_active_snov_accounts` + `fetch_snov_credentials`). Conta da lista que não existir ou estiver inativa no snov-am-api é registrada como aviso no log da run e pulada; a run não falha por isso.

## Planilha

Colunas: `email`, `nome`, `observacao`, `data`, `negativado`.

- Só `email` é usado para negativar. `nome`, `observacao` e `data` são ignorados pelo pipeline.
- `negativado` é a marca de controle (ver abaixo).
- Cabeçalhos reais: `Email`, `nome`, `Observação`, `Data`, `Negativado`. Casados sem acento, sem diferença de maiúsculas e sem espaços nas pontas.
- Linhas sem `email` ou com e-mail inválido (`EMAIL_RE`, mesma de `negativacao_manual.py`) são puladas e contadas como inválidas, com o número da linha no log.

### Fonte

Google Sheets "Lista de negativação SDR e Proativa", aba **Negativação Geral** (A1:E~1900). A mesma planilha tem outra aba, "Contatos já negativados", que o pipeline não lê.

- `SHEET_ID=1DsZ_8rQ0vENWVnDjE1UQSK_AtzTiUyacZRgI1fzx5iA`
- `SHEET_TAB=Negativação Geral`
- `GOOGLE_SERVICE_ACCOUNT_JSON`: caminho do JSON da conta de serviço (volume no docker-compose, nunca commitado). A planilha precisa ser compartilhada com o e-mail da conta de serviço como **Editor**, porque o pipeline grava a coluna `negativado`.

### Sujeira observada nos dados reais

A coluna `email` não é limpa. Antes de validar, a normalização deve tratar:

- espaços e quebras de linha nas pontas (` aquino@x.com.br`, `

`);
- colchetes angulares e lixo colado: `<lorrane@x.com.br>`, `aline@x.com.br>`;
- texto duplicado na mesma célula: `comercial@ccweng.com.br <comercial@ccweng.com...`. Regra: extrair o primeiro e-mail válido da célula com regex;
- maiúsculas (`Henry.Petersen@...`): normalizar com `lower()`;
- células vazias com `negativado = OK` (pular);
- linhas repetidas com o mesmo e-mail e `nome` diferente (deduplicar).

### Domínios na coluna email

Linhas com `@dominio` ou domínio puro (ex.: `@costaimobiliaria.com.br`, `ceresseeding.com`) são válidas. Em vez de enviar `@dominio` para a lista, o pipeline **expande o domínio em e-mails** usando a API de Domain Search da Snov.io e negativa cada e-mail encontrado.

- Normalização: tira `@` inicial, espaços e `lower()`; valida com `DOMAIN_RE` (a mesma de `negativacao_manual.py`).
- Conta usada para a busca: a primeira das 5 contas alvo com credenciais válidas. A busca roda **uma vez por domínio** (resultado reaproveitado para as 5 contas) e consome créditos dessa conta.
- A API de Domain Search é assíncrona (inicia a busca, depois consulta o resultado) e paginada. Percorrer todas as páginas até esgotar. Confirmar endpoints e custo em créditos na documentação da Snov.io na implementação.
- Os e-mails retornados entram no mesmo conjunto dos e-mails diretos da planilha: deduplicados e enviados em lote às 5 contas.
- Domínio sem nenhum e-mail encontrado: a linha é marcada `OK` mesmo assim, com a contagem "0 e-mails" no log da run. Não reprocessar todo dia.
- Falha na busca (erro, timeout, saldo de créditos insuficiente): a linha **não** é marcada e entra na próxima run.
- Sem limite de e-mails por domínio e sem exceção por observação: tudo que está na planilha é negativado, inclusive linhas marcadas "ja é cliente".
- Único filtro: provedores de e-mail livre (gmail.com, outlook.com, hotmail.com, yahoo.com etc.) não são expandidos, porque o Domain Search não lista "todos os usuários" desses domínios e gastaria crédito à toa. Esses valores são pulados e listados no relatório da run. Lista em `DOMAIN_SKIP`.

## O que conta como "novo"

Uma linha é processada quando `negativado` está vazio. Qualquer valor preenchido (hoje o padrão do time é `OK`) conta como já negativado e a linha é ignorada, então reexecutar no mesmo dia é seguro e barato.

Deduplicação dentro da run: e-mails normalizados (`strip().lower()`) e repetidos na planilha são enviados uma vez só. Todas as linhas duplicadas recebem a mesma marca.

## Fluxo da run

1. Abrir run no SQLite (`db`), source `planilha`, mesmo padrão das outras (`start_run` / `finish_run`).
2. Buscar contas ativas no snov-am-api e filtrar pelas 5 de `SHEET_SNOV_ACCOUNTS`.
3. Ler a aba inteira, filtrar linhas novas, normalizar, separar e-mails de domínios e deduplicar. Expandir cada domínio em e-mails via Domain Search (ver seção acima) e unir ao conjunto.
4. Se não há e-mails novos, finalizar a run com 0 e sair (não toca na Snov.io).
5. Para cada uma das 5 contas, em paralelo (`PIPELINE_WORKERS`): obter token OAuth e enviar os e-mails em lotes de `SNOVIO_BATCH_SIZE` para `do-not-email-list`, respeitando `SNOVIO_RATE_LIMIT_PER_MIN`. Reaproveitar as funções de token, lote, retry e rate limit já existentes em `negativacao_manual.py` / `negativacao_mailgun.py` (extrair para módulo comum se a duplicação incomodar, sem refatorar os outros pipelines nesta entrega).
6. Gravar na planilha, em uma única chamada em lote, `negativado = OK` apenas nas linhas cujos e-mails foram aceitos em **todas** as contas ativas do alvo. Preencher só a coluna `negativado`; nunca alterar as demais.
7. Fechar a run com contagens: linhas lidas, novas, inválidas, duplicadas, e-mails enviados, erros por conta.

## Tratamento de falha

- Falha parcial (uma conta deu erro): as linhas **não** são marcadas, então a próxima execução reenvia para todas as contas. A Snov.io trata e-mail repetido na lista como idempotente, então o reenvio é aceitável. Isso evita rastrear estado por conta na planilha.
- Falha ao ler a planilha: run termina com erro, nenhuma chamada à Snov.io.
- Falha ao gravar `negativado` depois de enviar: run termina com erro e log claro; efeito é reenvio no dia seguinte, sem perda.
- Falha de rede/HTTP 429/5xx na Snov.io: retry com backoff até `MAX_RETRIES_PER_BATCH`, como nos outros pipelines.
- Timeouts HTTP `(10, 30)` em toda chamada.

## Agendamento

- Novo job APScheduler `planilha_nightly` em `app.py`, `CronTrigger(hour=0, minute=0, timezone=SHEET_SCHEDULE_TIMEZONE)`. Os 4 jobs atuais seguem às 01:00 e não são alterados.
- Usa `_run_scheduled_pipeline` (pula se já há run da planilha em andamento).
- Variáveis novas: `SHEET_SCHEDULE_TIMEZONE` (padrão `America/Sao_Paulo`), `SHEET_SCHEDULE_HOUR` (padrão `0`), `SHEET_SCHEDULE_MINUTE` (padrão `0`).
- Disparo manual: `POST /api/planilha/runs/trigger`; consulta: `GET /api/planilha/runs` e `GET /api/planilha/runs/{run_id}`, no padrão dos endpoints de Postal.
- Painel: nova aba "Planilha" em `static/index.html` com histórico de runs, mesmo layout das demais.

## Arquivos afetados

- `negativacao_planilha.py` (novo): script do pipeline, executável via `python negativacao_planilha.py`.
- `snov_accounts.py`: `SHEET_SNOV_ACCOUNTS`.
- `db.py`: tabela/funções de runs `planilha` (`start_run`, `finish_run`, `list_runs`, `get_run`), seguindo o padrão do Postal.
- `app.py`: job agendado e endpoints.
- `static/index.html`: aba "Planilha".
- `.env.example`, `docker-compose.yml`, `requirements.txt`: variáveis novas e dependência `gspread` (ou `google-api-python-client`) mais `google-auth`.

## Fora de escopo

- Remover e-mails da lista de não enviar quando a linha sair da planilha.
- Enviar `@dominio` para a lista de não enviar. Domínio sempre vira lista de e-mails.
- Usar `nome`, `observacao` ou `data` para qualquer lógica.
- Mudar os pipelines Mailgun/SendGrid/Postal/Manual.

## Critérios de aceite

1. Linha nova com e-mail válido aparece na Lista de e-mails a não enviar das 5 contas após a run das 00:00 e fica com `negativado = OK`.
2. Linha já `OK` não gera nenhuma chamada à Snov.io.
3. Duplicados na planilha geram um único envio por conta.
4. Linha inválida (nem e-mail nem domínio) é pulada, aparece no log com o número da linha e não bloqueia as demais.
5. Se uma das 5 contas falha, nenhuma linha daquele lote é marcada e a run indica qual conta falhou.
6. Duas execuções seguidas sem linhas novas: a segunda termina com 0 enviados.
7. A run aparece no painel com contagens e erros.
8. Os jobs das 01:00 continuam funcionando sem mudança.

## Critérios de aceite adicionais (domínios)

9. Linha com domínio gera uma busca na Snov.io e negativa todos os e-mails retornados nas 5 contas; a linha fica `OK`, sem limite de quantidade e independente da observação.
10. Domínio de e-mail livre (gmail.com etc.) não é expandido e aparece no relatório.
11. Falha na busca não marca a linha.

## Decisões fechadas

1. Marca em `negativado`: `OK`.
2. Domínios: expandir via Domain Search da Snov.io, sem limites, exceto e-mail livre.
3. Roda no mesmo container dos outros pipelines.
4. Primeira run: processa todas as linhas sem `OK` e marca `OK` nas processadas com sucesso.
