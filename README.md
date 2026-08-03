# Argentina ETL

Pipeline diário que consolida dados de embarques de navios na Argentina, a partir
dos boletins do NABSA, e distribui o resultado em três destinos: SQL Server,
planilha no SharePoint e relatório por e-mail.

Roda desatendido, uma vez por noite, pela tarefa agendada `new_sailed_task`.

---

## Como rodar

```bash
python main.py
```

Ou, equivalente:

```bash
python -m argentina_etl
```

`main.py` na raiz é apenas um *shim*: coloca `src/` no path e delega para
`argentina_etl.__main__`. Existe para que a tarefa agendada continue funcionando
sem alteração — ela chama `python.exe main.py` com `WorkingDirectory` na raiz.

## Instalação

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env      # e preencha
```

Todas as variáveis obrigatórias precisam estar no `.env`: `config.py` levanta
`EnvironmentError` na inicialização se faltar alguma. O `.env.example` documenta
cada uma.

**Não é necessário ter Excel instalado.** Até 2026-07-28 as pivot tables eram
geradas por automação COM; hoje são calculadas com pandas.

## Testes

```bash
pytest
```

125 testes, todos passando. Nenhuma falha conhecida — se algum ficar vermelho,
é sinal, não ruído.

---

## Estrutura

```
sailed_auto/
├── main.py                      shim de entrada
├── .env                         configuração (nunca versionado)
├── requirements.txt  pytest.ini
│
├── src/argentina_etl/
│   ├── __main__.py              orquestrador — só a sequência de etapas
│   ├── config.py                única leitura do .env
│   ├── logging_setup.py         logger compartilhado
│   ├── downloader.py            Selenium headless → data/raw/
│   ├── validation.py            continuidade, gaps, corte de rodapé
│   │
│   ├── pipelines/               regra de negócio: o que os dados devem ser
│   │   ├── sailed.py            leitura do arquivo bruto e merge
│   │   └── lineup.py            leitura e classificação de status
│   │
│   ├── storage/                 persistência: escreve o que recebeu
│   │   ├── excel.py             arquivo local
│   │   ├── onedrive.py          planilha + sheets derivadas + pivots
│   │   ├── sharepoint.py        publicação via Microsoft Graph API
│   │   └── sql_server.py        Arg_Sailed e Arg_Lineup
│   │
│   ├── reporting/report.py      relatório HTML por e-mail
│   └── utils/files.py           utilitários
│
├── tests/                       125 testes
├── sql/                         DDL e views
├── docs/                        documentos operacionais
│
├── data/                        ►► gitignored ◄◄
│   ├── raw/{sailed,lineup}/     downloads brutos do NABSA
│   ├── db/                      banco principal
│   └── archive/                 snapshots datados
└── logs/                        ►► gitignored ◄◄
```

A divisão que mais importa é **`pipelines/` vs `storage/`**: `pipelines/` decide o
que os dados devem ser; `storage/` só escreve o que recebeu. Se um módulo de
`storage/` precisa consultar regra de negócio, está no lugar errado.

## Fluxo

| Etapa | O que faz |
|---|---|
| 1 | Baixa Sailed e Line-Up via Chrome headless |
| 1b | Grava o snapshot diário do Line-Up em `Arg_Lineup` (append-only) |
| 2 | Lê o arquivo mais recente e valida o corte de rodapé |
| 3 | Lê o banco existente |
| 4 | Merge período a período, com trava de segurança, e validações |
| 5 | Salva local, no OneDrive (com pivots) e no SQL Server |
| 5b | Publica no SharePoint via Microsoft Graph — **só se `GRAPH_UPLOAD_ENABLED=true`** |
| 6 | Envia o relatório por e-mail |

### Duas regras que não devem ser alteradas sem entender

**A trava do merge.** A substituição é **em bloco**: um período aceito tem o mês
inteiro apagado e reinserido a partir do arquivo novo. Por isso ele só é aceito se
passar em **duas** condições — **volume** (tem ≥ linhas que o banco) e **cobertura**
(traz todos os dias que o banco já tem naquele mês). Sem a primeira, um arquivo
truncado do NABSA corromperia o mês inteiro, de forma permanente, já que a base é
reescrita a cada execução. Sem a segunda, um arquivo gordo no começo do mês e vazio
no fim apagaria os últimos dias sem que nenhuma validação avisasse. Contagem igual é
aceita de propósito: a fonte reformula parcelas sem mudar o número de linhas.
Ver `ESTRUTURA.md`, decisões 9.1 e 9.4.

**`fast_executemany = False` no Line-Up.** O driver legado `SQL Server` rejeita
valores `None` com o modo rápido ligado. No Sailed pode ficar `True`.

## Documentação

| Arquivo | Assunto |
|---|---|
| `README.md` | este — instalação e execução |
| `ARQUITETURA.md` | como o pipeline funciona por dentro |
| `ESTRUTURA.md` | onde cada coisa mora, e por quê |
| `CLAUDE.md` | instruções para o Claude Code |
| `docs/` | documentos operacionais |

## Convenções

- Docstrings e mensagens de log em **português**
- `from __future__ import annotations` no topo de todo módulo
- Nenhum path, URL ou credencial fora do `.env`
- SQL Server por autenticação Windows; nunca senha em código
