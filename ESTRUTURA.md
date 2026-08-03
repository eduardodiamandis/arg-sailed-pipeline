# ESTRUTURA.md — Guia de organização do projeto Argentina

> **O que é este documento.** É a referência única sobre **onde cada coisa mora** e **por quê**.
> Antes de criar um arquivo novo, mover um módulo ou adicionar uma dependência, consulte
> a seção [6. Onde colocar coisa nova](#6-onde-colocar-coisa-nova).

> ## ⚠️ Revisão de 2026-07-28 — o alvo mudou
>
> Este guia foi escrito para reorganizar `C:\Users\server\Desktop\Argentina`. Durante a
> execução da Fase 2, descobrimos que **existem dois repositórios rodando o mesmo pipeline**,
> e que o que estávamos reorganizando é o que está quebrado há 21 noites.
>
> **O alvo passou a ser `C:\Users\server\sailed_auto`.** Os princípios, convenções e a
> estrutura-alvo continuam valendo — muda o repositório onde são aplicados.
> Ver [1.1](#11-a-descoberta-dois-repositórios-divergentes) e [8. Plano](#8-plano-de-migração).
>
> **Estado em 2026-07-29:** Fases 1, 2, A, B, C, D, E, F, I e J **concluídas**. Resta
> apenas a Fase H, bloqueada por permissão externa. **As três decisões abertas (9.1, 9.2,
> 9.3) resolvidas.**
>
> A produção está funcionando com **um** pipeline e **um** agendamento; os dados de junho
> foram recuperados; a base voltou a avançar sozinha; o código está em
> `sailed_auto/src/argentina_etl/` com **91 testes**; os dados vivem dentro do projeto; e
> a automação COM do Excel foi eliminada — o pipeline não depende mais de Excel instalado
> nem de sessão interativa.
>
> **Em 2026-07-29:** a Fase G investigou a sincronização do OneDrive e concluiu que o
> mecanismo em si é o problema — o arquivo ficou preso por 9 h sem causa aparente,
> enquanto todo o resto da biblioteca sincronizava. Isso originou a
> **[Fase H](#-fase-h--publicação-via-microsoft-graph-api-em-andamento)**: publicar via
> Microsoft Graph API, que confirma a entrega. Código pronto e autenticação validada;
> **aguardando concessão de permissão** (chamado aberto em 29/07).
>
> **Fase F concluída em 2026-07-29:** a documentação migrou para o `sailed_auto`,
> que passa a ser o único repositório do projeto.
>
> **Fase E encerrada em 2026-07-29:** o `Desktop\Argentina` foi marcado com a tag
> `aposentado-2026-07-29` e movido para
> `C:\Users\server\_arquivo\Argentina-aposentado-2026-07-29`. Ele não guarda nada
> de único e **não deve ser executado** — ver [seção 10](#10-dívida-técnica-conhecida).
>
> As referências a `Desktop\Argentina` ao longo deste documento são **históricas** —
> descrevem de onde viemos, e devem permanecer.

---

## 1. Diagnóstico

### 1.1 A descoberta: dois repositórios divergentes

Duas tarefas agendadas no Windows executam o mesmo pipeline a partir de bases de código
diferentes:

| | `Desktop\Argentina` | `sailed_auto` |
|---|---|---|
| Tarefa agendada | `Argentina ETL Daily` (22:07) | `new_sailed_task` (23:46) |
| Estado | **Falha desde 2026-06-25** (21 noites) | **Funcionando** |
| Download | `webbrowser.open` + polling em `~/Downloads` | **Selenium + Chrome headless** |
| Line-Up | `lineup_processor.py` → tabela `Arg_Lineup` | só baixa, não processa |
| Validação de gaps | `validacao.py` | ausente |
| Pivot Tables | sem timeout | isoladas com timeout de 120 s |
| Testes | 7 (subconjunto dos de lá) | **38** (`test_database.py` + `test_pipeline.py`) |
| Convenção de nomes | `Donloader.py`, `Database.py` | já em snake_case |

**Nenhum dos dois é superconjunto do outro.** Reconstruindo o histórico: os dois divergiram
de um ancestral comum; em março/2026 o `sailed_auto` consertou o download e ganhou testes
(commit `3645d3a`); entre abril e maio o `Desktop\Argentina` ganhou o processamento de
Line-Up e a validação de gaps, mas manteve o downloader quebrado.

**Causa-raiz das 21 falhas**, no docstring do `downloader.py` do `sailed_auto`: *"as URLs do
Nabsa retornam uma página HTML com redirect via JavaScript — o requests simples não consegue
seguir esse tipo de redirect."* Todas as falhas são idênticas:
`Timeout (40s): arquivo 'vessels_sailed_update.xlsx' não apareceu em Downloads`.

Consequência prática: as ~34 planilhas `Arg_sailed_database_AT_<data>_<hora>.xlsx` são
**todas produzidas pelo `sailed_auto`** (`main.py:143`), não por este repositório.

### 1.2 Problemas de organização (levantados em `Desktop\Argentina`)

Vários destes já estão resolvidos no `sailed_auto`; a coluna final indica onde ainda se aplicam.

| # | Problema | Ainda aplica? |
|---|---|---|
| 1 | Sem `.gitignore` — 205 arquivos untracked | resolvido nos dois |
| 2 | Dados na raiz do código (~40 planilhas, `____.xlsx`, `texto.txt`) | resolvido (Fase 2) |
| 3 | Banco `.xlsx` versionado no git | resolvido (Fase 1) |
| 4 | Código morto: `concater.py`, `sailde.py`, `SQLmanager.py` | só `Desktop\Argentina` |
| 5 | `SQLmanager.py` duplica `salvar_sql_server` com credenciais hardcoded | só `Desktop\Argentina` |
| 6 | Nomes inconsistentes (`Donloader.py`, `Test database.py`) | só `Desktop\Argentina` |
| 7 | `README.md` documenta estrutura que nunca existiu | **sim, nos dois** |
| 8 | Backups duplicados (`Sailed/`, `line_vessell/`, `backup_db/`) | resolvido (Fase 2) |
| 9 | Sem `requirements.txt` | resolvido nos dois |
| 10 | `env.example` incompleto → `EnvironmentError` | resolvido aqui; **verificar lá** |
| 11 | Hardcode de e-mail em `main.py:53-54`, sobrescrito no import da linha 163 | só `Desktop\Argentina` |
| 12 | Log com path fixo em `Path.home()` | **sim, nos dois** |
| 13 | Testes escondidos em `Test database.py` (espaço no nome) | só `Desktop\Argentina` |
| 14 | `.idea/` versionado | resolvido (Fase 1) |
| 15 | **`sys.path.insert(..., "src")` vestigial** apontando para pasta inexistente | **sim, nos dois** |

---

## 2. Princípios

Cinco regras que explicam todas as decisões abaixo. Quando este documento for ambíguo,
decida pelo princípio.

1. **Código, dados e saídas nunca se misturam.**
   Código em `src/`, dados em `data/`, saídas em `logs/`. `data/` e `logs/` são
   *inteiramente* ignorados pelo git. Qualquer arquivo que o pipeline **escreve** é dado,
   não código — mesmo que seja `.xlsx` e você olhe para ele todo dia.

2. **O git versiona o que um humano escreveu.**
   Se um programa gerou o arquivo, ele não entra no repositório. O banco `.xlsx` é a
   *saída* do pipeline, não a fonte da verdade do projeto — a fonte da verdade é o
   SQL Server e os arquivos brutos do NABSA.

3. **Uma responsabilidade, um módulo.**
   `database.py` faz leitura, merge e escrita em Excel e SQL — motivos distintos para o
   arquivo mudar. Isso vira módulos separados.

4. **Configuração vem do `.env`, sempre.**
   Nenhum path, URL, servidor, tabela ou e-mail hardcoded em `.py`. `config.py` é o
   único módulo que lê o ambiente; todos os outros importam dele.

5. **Um documento por assunto.**
   `README.md` = como rodar. `ARQUITETURA.md` = como funciona. `ESTRUTURA.md` (este) =
   onde as coisas moram. `CLAUDE.md` = instruções para o agente. Sem sobreposição.

---

## 3. Estrutura-alvo

Aplicada sobre `C:\Users\server\sailed_auto`.

```
sailed_auto/
│
├── .env                        # segredos reais — NUNCA versionado
├── .env.example                # template completo de todas as variáveis
├── .gitignore
├── requirements.txt
├── pytest.ini
├── run_etl.bat                 # entrada do Agendador de Tarefas do Windows
│
├── README.md                   # instalação, configuração, como rodar
├── ARQUITETURA.md              # como o pipeline funciona
├── ESTRUTURA.md                # este arquivo
├── CLAUDE.md                   # instruções para o Claude Code
│
├── src/
│   └── argentina_etl/
│       ├── __init__.py
│       ├── __main__.py         # orquestrador — só sequência de etapas
│       ├── config.py           # única leitura do .env
│       ├── logging_setup.py    # logger compartilhado
│       ├── downloader.py       # Selenium headless → data/raw/
│       ├── validation.py       # ►PORTADO de Desktop\Argentina
│       │
│       ├── pipelines/          # regra de negócio — transformação de dados
│       │   ├── __init__.py
│       │   ├── sailed.py       # ler_arquivo_novo, merge_com_banco
│       │   └── lineup.py       # ►PORTADO de Desktop\Argentina
│       │
│       ├── storage/            # persistência — sem regra de negócio
│       │   ├── __init__.py
│       │   ├── excel.py        # salvar_local, _forcar_sync_onedrive
│       │   ├── onedrive.py     # salvar_onedrive + sheets derivadas
│       │   ├── sql_server.py   # salvar_sailed_sql, salvar_lineup_sql
│       │   └── pivot.py        # automação COM do Excel, com timeout
│       │
│       ├── reporting/
│       │   ├── __init__.py
│       │   ├── report.py       # relatório HTML + envio SMTP
│       │   ├── dashboard.py    # gerador do dashboard HTML autocontido
│       │   └── dashboard_template.html
│       │
│       └── utils/
│           ├── __init__.py
│           └── files.py        # get_latest_file
│
├── sql/                        # ►PORTADO — DDL e views, versionado
├── tests/                      # 38 testes já existentes + novos do port
│
├── data/                       # ►► INTEIRAMENTE GITIGNORED ◄◄
│   ├── raw/{sailed,lineup}/    # downloads brutos do NABSA
│   ├── db/                     # banco principal + saída
│   └── archive/                # snapshots datados
│
└── logs/                       # ►► INTEIRAMENTE GITIGNORED ◄◄
```

### Por que `src/`

O layout `src/` impede o Python de importar o pacote acidentalmente a partir do diretório
de trabalho — os testes exercitam o pacote da mesma forma que a execução real. Note que
**os dois `main.py` já contêm um `sys.path.insert(..., "src")`** apontando para uma pasta
que nunca existiu: a intenção sempre esteve lá, só nunca foi executada.

### `pipelines/` vs `storage/`

A divisão que mais importa. **`pipelines/`** decide *o que* os dados devem ser
(a regra do merge período-a-período, a classificação de status do lineup) —
é aí que mora o conhecimento de negócio e é aí que os testes se concentram.
**`storage/`** só escreve o que recebeu, sem opinar. Se você precisar consultar uma
regra de negócio para escrever um módulo de `storage/`, ele está no lugar errado.

---

## 4. Convenções

### Nomes de arquivo
- Módulos Python: `snake_case.py`. Sem espaços, sem PascalCase, sem typos.
- Um `__init__.py` em todo diretório de pacote, mesmo vazio.
- Testes: `tests/test_<modulo>.py` — o prefixo `test_` é obrigatório para o pytest coletar.

### Código
- `from __future__ import annotations` no topo de todo módulo.
- **Docstrings e mensagens de log em português** — a convenção do projeto, mantida.
- Imports agrupados: stdlib → terceiros → locais. **Nenhum import dentro de função**
  (o `from Config import ...` na linha 163 do `main.py` antigo é o bug que isso evita).
- Type hints em toda assinatura pública.

### Configuração
- Toda variável nova entra em **três** lugares, na mesma mudança: `.env`, `.env.example`
  e `config.py`. Um `.env.example` incompleto é um bug — foi assim que as variáveis de
  Gmail ficaram indocumentadas.
- Obrigatórias via `_require()`; opcionais via `os.getenv(chave, default)`.

### SQL Server
- Autenticação Windows (`Trusted_Connection=yes`). Nunca senha em código.
- `fast_executemany = True` no Sailed; **`False` no Lineup** — o driver legado `SQL Server`
  rejeita `None` com fast mode ligado. Não "consertar" isso sem testar o Lineup.

### Logs
- Todo módulo faz `from argentina_etl.logging_setup import logger`. Nunca `print()`.
- O diretório de log vem do `.env` (`DIR_LOGS`), não de `Path.home()`.

---

## 5. O que nunca é versionado

```gitignore
# Segredos
.env

# Dados e saídas — gerados pelo pipeline
data/
logs/

# Planilhas
*.xlsx
*.xls
*.xlsm

# Ambiente e cache Python
.venv/
__pycache__/
*.py[cod]
.pytest_cache/

# Claude Code
.claude/settings.local.json
.claude/worktrees/

# IDE
.idea/
.vscode/

# Windows
Thumbs.db
desktop.ini
~$*
```

**Nota sobre `*.xlsx`:** a regra é ampla de propósito — nenhuma planilha deve entrar no
repositório. Se algum dia existir uma planilha que seja genuinamente *fonte* (um mapa de
de-para mantido à mão, por exemplo), ela entra com exceção explícita
(`!config/mapeamento.xlsx`) e um comentário dizendo por quê.

---

## 6. Onde colocar coisa nova

| Você está criando... | Vai em | Regra |
|---|---|---|
| Nova regra de transformação de dados | `src/argentina_etl/pipelines/` | Com teste em `tests/` |
| Novo destino de gravação (S3, API, outro banco) | `src/argentina_etl/storage/` | Recebe DataFrame pronto; não transforma |
| Nova validação pós-merge | `src/argentina_etl/validation.py` | Retorna achados, não levanta exceção |
| Nova variável de ambiente | `.env` + `.env.example` + `config.py` | Os três, na mesma mudança |
| Novo script SQL / view | `sql/` | Versionado |
| Script de uso único / investigação | **Fora do repositório** | Ver abaixo |
| Planilha, log, download | `data/` ou `logs/` | Nunca na raiz |
| Nova etapa do pipeline | `__main__.py` chama; a lógica mora no módulo | `__main__.py` não ganha regra de negócio |

**Sobre scripts de uso único.** Foi assim que `concater.py`, `sailde.py`, `SQLmanager.py`
e `teste_lineup_processor.py` nasceram: úteis por um dia, permanentes por um ano. Se você
precisa de um script descartável, rode-o de fora do repositório ou apague-o quando terminar.
Se ele sobreviveu a três usos, ele merece virar um módulo com teste.

---

## 7. Mapa de-para

### 7.1 `sailed_auto` → estrutura-alvo

| Hoje | Destino |
|---|---|
| `main.py` | `src/argentina_etl/__main__.py` |
| `config.py` | `src/argentina_etl/config.py` (+ `SQL_TABLE_LINEUP`, `DIR_LOGS`) |
| `logger_config.py` | `src/argentina_etl/logging_setup.py` |
| `downloader.py` | `src/argentina_etl/downloader.py` — **manter como está**, é a peça que funciona |
| `latest_file.py` | `src/argentina_etl/utils/files.py` |
| `pivot_tables.py` | `src/argentina_etl/storage/pivot.py` — já isolado, com timeout |
| `email_report.py` | `src/argentina_etl/reporting/report.py` |
| `test_database.py`, `test_pipeline.py` | `tests/` |
| `2025.rar` | avaliar; provavelmente `data/archive/` ou descarte |

**`database.py` — o desmembramento**

| Função | Destino |
|---|---|
| `ler_arquivo_novo`, `merge_com_banco`, `_cortar_apos_duas_linhas_vazias` | `pipelines/sailed.py` |
| `salvar_local`, `_forcar_sync_onedrive` | `storage/excel.py` |
| `salvar_onedrive` | `storage/onedrive.py` |
| `salvar_sql_server` | `storage/sql_server.py` |

### 7.2 O que é portado de `Desktop\Argentina`

Verificação de dependências: **os dois módulos importam apenas `pandas`, `pyodbc`,
`datetime`, `re` e `from logger_config import logger`** — e `logger_config.py` é idêntico
nos dois repositórios. **Copiáveis sem alteração de código.**

| Origem | Destino | Status |
|---|---|---|
| `Database.merge_com_banco` (trava de segurança) | `database.py` | ✅ **portado** (`a18e1d7`) — ver 9.1 |
| `lineup_processor.py` (~300 linhas) | `pipelines/lineup.py` | ✅ **portado** (`6b47363`), com 2 bugs corrigidos |
| `validacao.py` (~110 linhas) | `validation.py` | ✅ **portado** (`b3dfad8`) + `validar_continuidade` nova |
| `sql/` (6 arquivos) | `sql/` | ✅ **portado** (`bdf04a3`) — Fase E |
| `DOCUMENTACAO.md` | `ARQUITETURA.md` | ✅ **portado** (`a401192`) — Fase F |
| `CLAUDE.md` | `CLAUDE.md` | ✅ **portado** (`a401192`) — Fase F |
| `harvest_arg_templete.html` | — | **não portar**: o `report.py` do `sailed_auto` tem HTML próprio |

**Não portar:** `Test database.py`. Os 8 testes do `sailed_auto/test_database.py` têm nomes
idênticos e cobrem mais — os 7 daqui são subconjunto.

> **O merge não estava no plano original e precisou ser portado.** O
> `merge_com_banco` do `sailed_auto` substituía os períodos sobrepostos **cegamente**,
> sem comparar contagens. Isso é tolerável enquanto a base nunca é reescrita, mas vira
> corrupção permanente assim que ela passa a ser — por isso o port virou pré-requisito
> da 9.1, não item independente. Lição para as Fases B e C: **antes de portar, comparar
> a função equivalente dos dois lados.** "Copiável sem alteração" descreve o arquivo
> portado, não garante que o destino esteja pronto para recebê-lo.

### 7.3 Código morto de `Desktop\Argentina` — **não apagado, arquivado junto**

`concater.py`, `sailde.py`, `SQLmanager.py`, `teste_lineup_processor.py`, `texto.txt`,
`run_argentina_etl_DEBUG.bat`.

O plano previa apagá-los antes de arquivar. **Não foi o que aconteceu, e não faz falta:**
a pasta inteira saiu de circulação de uma vez, então apagar arquivo por arquivo dentro de
um repositório que ninguém mais executa seria trabalho sem efeito. Eles continuam lá,
alcançáveis pela tag: `git show aposentado-2026-07-29:concater.py`.

O que importava era não deixá-los no repositório **vivo** — e nenhum deles foi portado.

---

## 8. Plano de migração

### ✅ Concluído em `Desktop\Argentina`

Executado antes da descoberta dos dois repositórios. Continua útil: os dados moveram-se para
`data/`, que é o layout que o `sailed_auto` também vai adotar.

- **Fase 1** (`dae76f8`) — `.gitignore`, destrackeio de 27 arquivos gerados,
  `requirements.txt`, `.env.example` completo. `git status`: 223 → 20 linhas.
- **Fase 2** (`4d05fee`) — dados movidos para `data/` (2 no `db/`, 38 no `archive/`,
  146 raw Sailed, 431 raw Line-Up, 189 logs). Removidos `Sailed/`, `line_vessell/`,
  `backup_db/`. Validado: `get_latest_file` preserva o `ctime`.

### ✅ Fase A — Destravar produção *(concluída em 2026-07-28)*

A Fase 2 moveu os dados para onde o `sailed_auto` não apontava, quebrando o job das 23:46.

- [x] Caminhos atualizados em `C:\Users\server\sailed_auto\.env`
      (backup em `.env.bak-20260728-1050`, protegido por `.gitignore`)
- [x] `PATH_DATABASE_OUTPUT` aponta para `data\archive\` — o `main.py:143` deriva o nome
      datado dele via `with_stem()`, então os snapshots caem junto com os 34 históricos
- [x] Execução manual confirmada de ponta a ponta: 169,7 s, exit 0, as 7 etapas.
      A ETAPA 1 — que falhava há 21 noites — passou em 27 s
- [x] Permissão de escrita no `sailed_auto` persistida em `.claude/settings.local.json`
      (`additionalDirectories` + regras `Edit`/`Write`/`Read`)

**Recuperação de dados executada na sequência.** A primeira execução expôs a perda de
26–30/06/2026 descrita na [9.1](#91-o-banco-base-nunca-é-atualizado--resolvido). Origem
usada: `data/archive/Arg_sailed_database_AT_2026-07-01_2346.xlsx` (46.398 linhas, junho
30/30, mesmas 19 colunas). Base antiga preservada em
`data/archive/Arg_sailed_database_PRE-RECUPERACAO_2026-07-28.xlsx`.
Confirmado no SQL Server: 46.904 linhas, dias 26–30/06 presentes (85 linhas), junho 30/30.
OneDrive atualizado — Power BI com a série completa.

### ✅ Fase B — Portar `validacao.py` *(concluída em 2026-07-28, `b3dfad8`)*

- [x] `validacao.py` → `sailed_auto/validation.py`, sem alteração no código portado
- [x] `validar_corte_rodape` na ETAPA 2; `validar_continuidade` e `detectar_gaps` na ETAPA 4
- [x] Chamadas protegidas por `try/except` — as ETAPAS 2–4 não têm guarda própria, e uma
      validação que só informa não pode derrubar o pipeline
- [x] `test_validation.py`: 12 casos novos. Suíte: 48 passando
- [x] Execução manual confirmada

**A premissa da fase estava errada, e o escopo mudou por causa disso.** O plano dizia que
`detectar_gaps` teria alertado sobre a perda de 26–30/06. Testado contra o cenário real
(base `PRE-RECUPERACAO` + arquivo do NABSA de 02/07): reportou *"nenhum gap detectado"*
com os 5 dias ausentes do resultado.

Ele só itera sobre os períodos presentes no **arquivo novo**. Como o arquivo trazia apenas
`2026-07`, junho nunca era examinado. O alcance real é mais estreito: pegar dias que
sobrevivem no banco quando a trava de merge rejeita um período.

**`validar_continuidade` (nova)** cobre o ângulo que faltava — compara o fim da base com o
início do arquivo novo. Verificada nos dois sentidos com dados reais:

| Cenário | Resultado |
|---|---|
| Real de 02/07 — base em 25/06, arquivo em julho | detecta **5 dias** de vão |
| Base saudável de hoje | sem alarme |

Tolerância padrão de 3 dias, porque nem todo dia tem embarque e um mês pode legitimamente
começar no dia 2 ou 3.

`gaps` **não** foi para o `db_stats` como o plano previa — ver [9.3](#93-dois-modelos-de-e-mail--resolvido-2026-07-28-b3dfad8).

### ✅ Fase C — Portar `lineup_processor.py` *(concluída em 2026-07-28, `6b47363`)*

- [x] `dbo.Arg_Lineup` confirmada com as 14 colunas de `COLUNAS_SQL`
- [x] Arquivo copiado como `lineup.py`
- [x] `SQL_TABLE_LINEUP` em `config.py`, `.env` e `.env.example`
- [x] ETAPA 1b após os downloads, em `try/except` não-crítico
- [x] `test_lineup.py`: 26 casos novos
- [x] Verificado em produção: 202 registros, 0 com `Status=SAILED`, 11 com `ETF_Date`

⚠️ `cursor.fast_executemany = False` é obrigatório: o driver legado `SQL Server`
rejeita `None` com fast mode ligado.

**A tabela estava parada em 25/06** — sem snapshots havia 33 dias, mesma data em que este
repositório parou de rodar. Diferente do Sailed, esse histórico **não se recompõe sozinho**:
cada linha é o retrato do que estava previsto naquele dia. Os arquivos brutos de 26/06 em
diante existem em `data/raw/lineup/`, então a reconstituição é possível — mas exigiria que
`_classificar_status` passasse a usar a data do snapshot em vez de `date.today()`, senão os
status seriam recalculados com o conhecimento de hoje. Não feito; ver seção 10.

**Dois bugs latentes encontrados pelos testes novos:**

1. **`pd.NaT` passa no `isinstance(x, datetime.date)`** — NaT herda de `datetime.datetime`,
   que herda de `datetime.date`. Em `_classificar_status` estourava `TypeError` na
   comparação; em `salvar_lineup_sql` gravaria a string `"NaT"`. Um único NaT derrubava a
   ETAPA 1b inteira e, por ela ser não-crítica, o snapshot do dia sumiria em silêncio.
   Corrigido com o helper `_e_data`.
2. **`NaN` sobrevivia ao `df.where(pd.notna(df), other=None)`** — em colunas `object` o
   pandas devolve `NaN` em vez de `None`. Como o pyodbc infere o tipo de cada parâmetro pela
   **primeira linha**, um `NaN` em `ETF_Date` na linha 1 ligava o parâmetro como `float` e a
   primeira linha seguinte com data em texto era rejeitada. Dependia dos dados do dia — por
   isso funcionou até 25/06 e falhou na primeira execução real.

Nenhum dos dois apareceria em revisão de código: os dois exigiram execução contra o SQL real.

### ✅ Fase D — Aplicar a estrutura-alvo *(concluída em 2026-07-28, `c917ba8`)*

- [x] `src/argentina_etl/` criado com os `__init__.py`
- [x] Módulos movidos e renomeados conforme [7.1](#71-sailed_auto--estrutura-alvo);
      `database.py` desmembrado
- [x] `pytest.ini` com `testpaths=tests` e `pythonpath=src`
- [x] 74 testes passando; execução manual completa pelo shim, 8 etapas, exit 0

**A tarefa agendada não precisou ser alterada.** `main.py` na raiz virou um shim que põe
`src/` no path e delega para `argentina_etl.__main__`. A `new_sailed_task` continua rodando
`python.exe main.py` com `WorkingDirectory` na raiz. Equivale a `python -m argentina_etl`.
Os dois `main.py` do projeto já traziam um `sys.path.insert(..., "src")` vestigial — era
essa a intenção original. Foi o que eliminou o maior risco da fase.

Os dados já estavam em `data/` desde a Fase 2, e o `sailed_auto` não tem `run_etl.bat`
(a tarefa chama o Python direto), então esses dois itens do plano original não se aplicaram.

**Quatro quebras que o refactor expôs**, todas corrigidas:

| Quebra | Causa |
|---|---|
| `.env` não encontrado, aplicação não subia | `config.py` calculava a raiz com `Path(__file__).parent`; em `src/argentina_etl/` isso aponta para a pasta errada. Agora `parents[2]`. O comentário original já dizia *"dois níveis acima de src/"* |
| 8 testes falhando | `patch()` com caminhos de módulo extintos (`patch("database.pyodbc")`) |
| 3 imports não migrados | Estavam **dentro** de funções de teste; a reescrita só casava início de linha |
| `IndentationError` no `sql_server.py` | A última linha do `lineup.py` não tinha newline final, o `wc -l` reportou 340 em vez de 341 e a extração por intervalo perdeu um `conn.close()` |

### ✅ Fase E — Aposentar `Desktop\Argentina` *(concluída em 2026-07-29)*

- [x] Tarefa `Argentina ETL Daily` **excluída** — passa a existir **um** pipeline
      e **um** agendamento
- [x] `sql/` portado (6 scripts) — `bdf04a3`
- [x] Referência morta a `Desktop\Argentina\Sailed` removida do `utils/files.py`
- [x] **Dados movidos para dentro do projeto** — 658 arquivos de dados e 189 logs
      saíram de `Desktop\Argentina\data\` para `sailed_auto\data\`; os quatro
      caminhos do `.env` atualizados — `7c162d4`
- [x] `logging_setup.py` desamarrado: `_DEFAULT_LOG_FILE` estava fixo em
      `Path.home()/"Desktop"/"Argentina"/"logs"`, o que prendia o `sailed_auto`
      ao repositório que ele veio substituir. Agora deriva da raiz do projeto
- [x] **Tag + mover a pasta para fora do Desktop** *(2026-07-29)* — commit final
      `af68eea` ("Estado final antes do arquivamento"), tag `aposentado-2026-07-29`,
      árvore de trabalho limpa. A pasta foi para
      `C:\Users\server\_arquivo\Argentina-aposentado-2026-07-29`

**Por que tag antes de mover.** A pasta continua sendo um repositório git completo, com
remoto e 4 branches. A tag é o que permite responder *"como estava o código aposentado?"*
sem depender de a pasta continuar existindo nesse caminho — `git show
aposentado-2026-07-29:<arquivo>` recupera qualquer um dos arquivos deletados listados
em [7.3](#73-deleções-em-desktopargentina-ao-arquivar).

**Verificado após o arquivamento:** nenhuma tarefa agendada aponta para o caminho antigo
(só a `new_sailed_task`, que roda no `sailed_auto`); nenhum caminho do `.env` referencia
`Desktop\Argentina`; as menções que sobraram no código são comentários históricos
(`logging_setup.py`, docstrings de `test_lineup.py` e `test_validation.py`) e devem
permanecer. Suíte após o arquivamento: **123 passando, 2 falhando** — as duas já
conhecidas e anteriores à migração.

**Correção de um item da Fase D.** Aquela fase marcou *"mover dados para `data/`"*
como não aplicável, alegando que já estavam lá desde a Fase 2 — **errado**: estavam
em `Desktop\Argentina\data\`, não em `sailed_auto\data\`. O `.env` apontava para lá,
então o repositório antigo não podia ser arquivado. Resolvido nesta fase.

**A tarefa agendada não precisou de novos valores.** O shim `main.py` na raiz manteve
o comando idêntico (`python.exe main.py`, `WorkingDirectory` em `sailed_auto`). A
`new_sailed_task` foi excluída e recriada com os mesmos parâmetros, mas com
`LogonType: Interactive` — o `S4U` exige elevação. **Consequência: a tarefa só roda
com o usuário logado.** Para corrigir, num PowerShell como administrador:

```powershell
Set-ScheduledTask -TaskName "new_sailed_task" -Principal `
  (New-ScheduledTaskPrincipal -UserId "SERVER\server" -LogonType S4U -RunLevel Limited)
```

### ✅ Fora do plano — Pivot Tables em pandas *(2026-07-28, `edad51a`)*

Não estava previsto em nenhuma fase, mas resolveu um problema que a Fase E expôs e
que sete tentativas de correção no COM não resolveram.

**O sintoma:** a `Pivot_2026` saía vazia apesar de existirem 145 registros e
3.906.772,43 tons casando o filtro (`Year=2026`, `Month=7`, `ARGENTINA`, `CORN`).
A `Pivot_2025`, com a mesma lógica, funcionava.

**A causa estava fora do código.** O Excel resolvia o caminho local para a URL do
SharePoint (`https://cgbent.sharepoint.com/...`) e construía a pivot sobre a **cópia
do servidor**, não sobre o arquivo recém-gravado. Como o log avisava
`OneDrive.exe não encontrado — sync automático indisponível` em toda execução, essa
cópia podia estar defasada: num diagnóstico ela parava em 29/05 enquanto a local já
tinha 27/07. Nenhuma correção no código do pivot resolveria isso.

**A solução:** gerar as pivots com pandas, dentro de `salvar_onedrive`, na mesma
escrita das demais sheets. Os filtros deixam de ser *page fields* escondidos no
`.xlsx` e viram constantes nomeadas — `PIVOT_ORIGIN`, `PIVOT_CARGO`,
`PIVOT_MES_ANO_ANTERIOR`. O layout foi preservado para não quebrar consumidores.

Também eliminou uma **duplicação**: `salvar_onedrive` já escrevia as duas sheets com
pandas (sem filtro), e o COM depois as sobrescrevia com a versão filtrada.

**Verificação:** `Grand Total` de 3.906.772,426, idêntico à soma calculada direto dos
dados; valores por destino batendo com os que o COM produzia quando funcionava
(`ALGERIA` 203.118,47 em 2026/07 e 133.950,33 em 2025/12).

**O que desapareceu junto:** 253 linhas de `storage/pivot.py`, os processos
`EXCEL.EXE` órfãos, o timeout de 120 s, o `OLE error 0x800a01a8`, a dependência de
Excel instalado e de sessão interativa, e a ETAPA 6 (o e-mail passou a ser a 6).

**Testes:** 17 casos novos cobrindo cada filtro isoladamente, insensibilidade a caixa
e espaços, agregação por destino e filtro sem resultado — tudo impossível de testar
enquanto dependia do Excel.

### ✅ Fase G — Sincronização do OneDrive *(investigada em 2026-07-29)*

O arquivo que o Power BI e as pessoas consomem vive numa biblioteca do SharePoint
sincronizada pelo OneDrive. O pipeline grava a cópia local, e a propagação para o
servidor não é garantida — foi essa defasagem que fez o Excel construir a
`Pivot_2026` sobre dados de 29/05 quando a cópia local já tinha 27/07.

Tirar o Excel do caminho resolveu o sintoma na geração das pivots, mas **não resolve
a sincronização**: quem abrir o arquivo pela web ainda pode ver dados velhos.

**Causa já identificada — o aviso é falso alarme.** `_forcar_sync_onedrive`
(`storage/onedrive.py`) procura o executável em um único lugar:

```python
onedrive_exe = Path(os.environ["LOCALAPPDATA"]) / "Microsoft" / "OneDrive" / "OneDrive.exe"
```

Esse é o caminho da instalação **por usuário**. Nesta máquina o OneDrive está
instalado **por máquina**, em `C:\Program Files\Microsoft OneDrive\OneDrive.exe`, e
**está em execução** (verificado em 2026-07-28). Ou seja: o cliente sempre esteve
vivo, o código nunca o encontrou, e o log registra
`OneDrive.exe não encontrado — sync automático indisponível` em toda execução desde
sempre.

**Feito** *(2026-07-29, `3be37db`)*

- [x] Executável procurado nos três locais de instalação, com detecção do processo
      via `tasklist`. O aviso só sai quando o cliente realmente não está rodando —
      e aí declara a consequência: *quem abrir pela web verá dados antigos*
- [x] Verificação pós-gravação: `status_sincronizacao()` lê o status por arquivo na
      coluna do shell do Windows, localizando-a pelo **nome** (`Status de
      disponibilidade` / `Availability status`) em vez de fixar o índice, que varia
      por versão e idioma. `verificar_sincronizacao()` aguarda até
      `SYNC_ESPERA_SEGUNDOS` (padrão 60) e emite `WARNING` se não sair de pendente
- [x] 16 testes cobrindo os dois idiomas do shell e a detecção do executável

Duas decisões de projeto: falha ao **ler** o status devolve sucesso, não alarme — um
aviso errado toda noite treinaria as pessoas a ignorar o e-mail. E nada disso levanta
exceção: é diagnóstico, não pode derrubar o pipeline.

### O diagnóstico mudou a conclusão da fase

A investigação mostrou que **o problema não é de código, e a correção acima não o
resolve** — apenas o torna visível.

O arquivo gravado às 23:48 de 28/07 continuava `Sincronização pendente` **nove horas
depois**. O que foi eliminado como causa:

| Hipótese | Verificação |
|---|---|
| Falta de espaço em disco | ❌ 43,5 GB liberados; seguiu pendente com 50 GB livres |
| Cliente parado | ❌ Reiniciado; varreu por 1 min e ignorou o arquivo |
| Arquivo travado localmente | ❌ Livre para escrita, zero processos Excel |
| Biblioteca ou pasta com problema | ❌ Arquivo novo criado na mesma pasta subiu na hora |
| *Check-out* no SharePoint | ❌ Verificado, não há |

Todos os outros arquivos da biblioteca — inclusive um `.docx` na mesma pasta —
mostram `Disponível neste dispositivo`. **Só este arquivo está preso, sem causa
aparente e sem mensagem de erro.**

**Ganho colateral:** os 43,5 GB vieram de uma distro WSL/Ubuntu que se acreditava
excluída, mas seguia registrada. O VHDX havia inchado para 43,5 GB com ~4,5 GB de
conteúdo real. Disco livre: 6,5 GB → 50 GB.

**A conclusão:** sincronizar via cliente desktop não é um mecanismo de integração
defensável. O cliente do OneDrive foi desenhado para o computador de uma pessoa —
sincroniza quando quer, sem contrato de entrega, sem código de retorno, sem log
acessível ao processo. Nenhuma quantidade de código nosso conserta isso. Daí a
[Fase H](#-fase-h--publicação-via-microsoft-graph-api-em-andamento).

### 🔜 Fase H — Publicação via Microsoft Graph API *(em andamento)*

Substituir a pasta sincronizada pela API do Graph, que **confirma a entrega**: o
upload retorna `201` com o `eTag` do item no servidor. Ou chegou, ou sabemos o código
e a mensagem do porquê não chegou.

**Feito** *(2026-07-29, `65e7470`, `5535f54`, `d0e15b6`)*

- [x] `storage/sharepoint.py`: fluxo *client credentials*, descoberta de site e
      biblioteca, upload por sessão em blocos de 5 MiB (múltiplo de 320 KiB, exigido
      pela API), `conflictBehavior=replace` para não gerar `arquivo 1.xlsx`,
      cancelamento da sessão se um bloco falhar
- [x] 16 testes com a API inteira mockada — sem credenciais, rede ou permissão
- [x] App Registration criado: `ETL Argentina - Upload SharePoint`, single tenant
- [x] **Autenticação validada**: token emitido com sucesso em 2026-07-29
- [x] Configuração no `.env`, com `GRAPH_UPLOAD_ENABLED=false`
- [x] `docs/graph-permission-request.md`: e-mail de solicitação, enviado em 2026-07-29
- [x] **Ligado no `__main__.py` como ETAPA 5b, atrás da flag** *(2026-07-29)* — com a
      flag em `false` o pipeline registra uma linha de INFO e segue; nada muda em
      produção. Quando a permissão sair, **basta virar a flag**
- [x] **Variáveis do Graph no `config.py`** — estavam no `.env` e no `.env.example`,
      mas nunca chegaram ao `config.py`, violando a regra dos três lugares da
      [seção 4](#configuração). Sem isso, virar a flag não publicaria nada
- [x] `validar_config_graph()` + 7 testes novos (`test_config_graph.py`)

**Aguardando:** concessão da permissão `Sites.Selected` (Aplicativo) mais a concessão
de escrita neste site específico. Até lá as chamadas retornam `401`.

**Verificado em 2026-07-29, 14h41: a permissão ainda não saiu.** O teste de ponta a
ponta contra o tenant real para no segundo passo:

| Passo | Resultado |
|---|---|
| Token de aplicativo | ✅ emitido |
| `GET /sites/cgbent.sharepoint.com:/sites/ZGC-PBIResearch` | ❌ **HTTP 401** `generalException` |

O token sair e a chamada falhar é a assinatura exata do que falta: a aplicação existe
e se autentica, mas **não tem acesso a nada**. Repetir esse teste é a forma de saber
quando a permissão foi concedida — antes de virar a flag.

**As variáveis do Graph não usam `_require`, de propósito.** Elas são lidas com
`os.getenv` e default vazio. Se usassem `_require`, uma variável faltando derrubaria o
`config.py` no import — e com isso o pipeline inteiro, por causa de um recurso
*opcional que está desligado*. A verificação acontece onde importa: com a flag ligada,
`validar_config_graph()` devolve o que falta e a etapa falha com o nome das variáveis
no log. É o mesmo princípio das validações — informar sem derrubar.

⚠️ **`Sites.Selected` não concede acesso a nada sozinha.** É preciso um segundo passo —
`POST /sites/{site-id}/permissions` — senão o aplicativo fica com a permissão e sem
acesso, retornando `403`. É o ponto em que a maioria trava.

**Quando a permissão sair:** rodar o teste dos três passos acima; saindo verde,
`GRAPH_UPLOAD_ENABLED=true` no `.env` e executar o pipeline uma vez à mão. Não há mais
código a escrever. A verificação de sincronização da Fase G torna-se desnecessária —
ela existe porque hoje não há confirmação de entrega; com o Graph, a confirmação é a
resposta.

**Falha na publicação marca o pipeline como ERRO** (`pipeline_ok = False`), então o
e-mail sai com o cabeçalho de erro. Não aborta: o SQL Server já foi gravado a essa
altura, e derrubar a execução não desfaria nada nem entregaria o arquivo. O que não
pode acontecer é a falha passar em silêncio — foi assim que se perderam 21 noites.

⚠️ **O client secret expira em 28/07/2028.** Na expiração a publicação para, e o
sintoma seria de novo uma falha silenciosa.

**Considerado e adiado:** apontar o Power BI direto para o SQL Server, que já é
atualizado pelo pipeline e esteve correto durante todo o incidente. Resolveria o caso
de uso principal sem código nenhum, e os `.pbix` da pasta já são relatórios de conexão
viva a um dataset publicado. Fora de escopo por ora, por decisão.

**Por que isso importa mais do que parece:** o SQL Server sempre esteve certo. Quem
consome o `.xlsx` depende inteiramente dessa publicação, sem nenhum sinal de que os
dados estão velhos. É exatamente o formato de falha silenciosa que já custou 21 noites
e 5 dias de dados neste projeto.

### ✅ Fase F — Documentação *(concluída em 2026-07-29, `a401192`)*

- [x] `README.md` reescrito — com a árvore **real**, o fluxo em 6 etapas e as regras que
      não devem ser alteradas sem entender. O anterior descrevia um `src/config.py`,
      `src/database.py` que nunca existiu: o problema #7 estava nos dois repositórios,
      cada um inventando uma estrutura diferente
- [x] `ARQUITETURA.md` escrito a partir do `DOCUMENTACAO.md` do repositório antigo
- [x] `CLAUDE.md` atualizado para a estrutura de pacote
- [x] `ESTRUTURA.md` (este) migrado — ele vivia no repositório que estávamos aposentando,
      o que obrigava a alternar entre dois diretórios a cada sessão e já tinha causado um
      `git add` no diretório errado
- [x] `.env.example` criado — o projeto tinha **29 variáveis de ambiente e nenhum
      template**, e `config.py` aborta em variável faltando. Não estava no plano da fase;
      apareceu ao documentar a instalação

Fora do plano original da fase, mas parte dela: `docs/`, com o pedido de permissão do
Graph. Documento operacional não é nenhum dos quatro assuntos do princípio 5 — daí o
diretório próprio.

### ✅ Fase J — Dívida técnica *(concluída em 2026-07-29)*

Sete itens da [seção 10](#10-dívida-técnica-conhecida) fechados. Detalhe de cada um lá;
o que vale destacar é que **três eram diferentes do que estava registrado**:

1. **`EMAIL_BACKEND` não era cosmético.** Registrado como "polui o log", na verdade
   matava a dica de diagnóstico que aparece quando o envio falha — o valor sujo não
   casava com `"smtp"` nem com `"graph"`, e o e-mail só funcionava porque o `_dispatch`
   cai no `else`.
2. **Os logs órfãos eram de dois produtores, não um.** Os 133 `process_log_*.log` do
   `concater.py` não estavam registrados em lugar nenhum.
3. **A `Unnamed: 18` não estava vazia** — tinha uma anotação escrita por uma pessoa.
   Uma limpeza automática de "colunas vazias" a teria apagado sem deixar rastro.

Daí a regra que ficou no código: `remover_colunas_sem_nome()` **avisa em `WARNING` antes
de descartar** qualquer coluna sem nome que tenha conteúdo. Limpeza silenciosa é o
mesmo formato de falha que já custou 21 noites e 5 dias de dados aqui.

**Fora de alcance por decisão:** o `LogonType: Interactive` da tarefa agendada continua
aberto — exige um PowerShell **elevado**, que este processo não tem.

### ✅ Fase I — Suíte verde *(concluída em 2026-07-29)*

Os dois testes que falhavam desde antes da migração foram consertados: **125 passando,
nenhum falhando.** A suíte é a rede de segurança principal do projeto — dois testes
vermelhos permanentes treinam quem executa a ignorar o resultado, e é exatamente assim
que uma falha nova passa despercebida.

**As duas causas eram do teste, não do código de produção.**

| Teste | Causa real |
|---|---|
| `TestSalvarOnedrive::test_cria_cinco_sheets` | Mockava a gravação inteira, então o `.xlsx` nunca existia em disco — e `salvar_onedrive` passou a chamar `_forcar_sync_onedrive`, cujo `os.utime` estoura em arquivo inexistente. Corrigido mockando também as duas etapas de sincronização, que são assunto do `test_sync.py` |
| `TestDownloadFile::test_salva_arquivo_com_nome_enriquecido` | **As asserções estavam fora do bloco `with tempfile.TemporaryDirectory()`.** Quando `result.exists()` rodava, o diretório de destino já tinha sido apagado pelo próprio context manager. O download funcionava o tempo todo |

O segundo caso **não era o que a dívida registrada dizia** — a seção 10 atribuía as duas
falhas à mesma origem. O `assertIn` do nome passava e só o `exists()` falhava, o que
apontava para o arquivo, não para a indentação. Vale como lembrete: *dívida documentada
descreve o sintoma da época, não necessariamente a causa.*

---

## 9. Decisões

### 9.1 O banco base nunca é atualizado — ✅ **RESOLVIDO** *(2026-07-28, `a18e1d7`)*

O `sailed_auto` **lia** `PATH_DATABASE` (`main.py:107`) mas **nunca escrevia de volta** — gravava
apenas o snapshot datado, o OneDrive e o SQL. O `Desktop\Argentina` escrevia (`main.py:138`).

**O defeito já tinha causado perda de dados.** Enquanto o arquivo do NABSA entregava o mês
corrente, o merge recompunha o mês inteiro e o problema ficava invisível. Na virada de junho
para julho o arquivo passou a trazer só julho, e **26–30/06/2026 caiu num vão que ninguém
preenchia**: sumiu do SQL Server e do OneDrive — logo, do Power BI — por 26 dias, desde 02/07.
Foram 85 linhas em 5 dias. Recuperado; ver [Fase A](#-fase-a--destravar-produção-concluída-em-2026-07-28).

**Resolução, em duas partes indissociáveis:**

1. **Trava de segurança no merge** (pré-requisito). O `merge_com_banco` do `sailed_auto`
   substituía os períodos sobrepostos cegamente. Com a base sendo reescrita, um arquivo
   truncado do NABSA corromperia o mês inteiro de forma permanente — trocaríamos um buraco
   lento e detectável por corrupção silenciosa. Portada a comparação período a período do
   `Desktop\Argentina`: o período novo só substitui se tiver linhas ≥ às do banco; senão é
   rejeitado com aviso no log.
2. **Write-back** em `main.py`: `salvar_local(db_atualizado, PATH_DATABASE)`.

**Bug encontrado no caminho.** O teste `test_arquivo_novo_vazio_nao_remove_dados` pegou uma
regressão no ramo "nenhum período aceito": concatenar um `DataFrame` vazio convertia a coluna
`Date` para dtype `object` e quebrava o acessor `.dt`. Corrigido eliminando o concat quando
não há nada a inserir. **O mesmo bug segue no `Database.py` deste repositório** — os 7 testes
daqui nunca cobriram "arquivo novo vazio" (ver seção 10).

Testes após a mudança: 36 de 38, incluindo os 8 de merge. As 2 falhas restantes são anteriores
e independentes (ver seção 10).

### 9.3 Dois modelos de e-mail — ✅ **RESOLVIDO** *(2026-07-28, `b3dfad8`)*

`sailed_auto/email_report.py` manda resumo do **log** (`send_log_report`, com `db_stats`);
`Desktop\Argentina/reporter.py` manda relatório de **dados** (`gerar_relatorio`, com
antes/depois e gaps). A pergunta era qual fica, ou se fundíamos os dois.

**Nenhum dos dois — a integração já existia.** O `_extract_warnings`
(`email_report.py:120`) varre o texto do log atrás de linhas `- WARNING - `, e as
validações registram exatamente assim. Os gaps aparecem sozinhos na seção "Avisos" do
e-mail, que já tem contador próprio, sem tocar em `email_report.py` nem em `db_stats`.

Fica o `send_log_report`, que está em produção. Bônus: os avisos de período REJEITADO
da trava de merge (9.1) também passaram a ser visíveis no e-mail.

---

### 9.2 `force=True` fixo no Lineup — ✅ **RESOLVIDO** *(2026-07-28, `6b47363`)*

Em `Desktop\Argentina\main.py:93`, `salvar_lineup_sql(..., force=True)` estava fixo,
sobrescrevendo o snapshot do dia a cada execução, o que contradizia o design append-only
descrito no próprio módulo.

**Exposto como `LINEUP_FORCE_SNAPSHOT` no `.env`**, padrão `true` — preserva o comportamento
que já existia, sem surpresa na virada. A escolha só afeta reexecuções **no mesmo dia**:
com `true` a reexecução substitui o snapshot, com `false` é ignorada. O histórico de outros
dias nunca é tocado em nenhum dos casos, então o append-only entre dias está preservado
de qualquer forma.

Verificado: três inserções seguidas deixam 202 linhas, sem duplicação.

---

### 9.4 A trava de merge olhava só a contagem — ✅ **RESOLVIDO** *(2026-07-29)*

A substituição de um período é **em bloco**: o mês inteiro sai do banco e volta a
partir do arquivo novo. A trava da [9.1](#91-o-banco-base-nunca-é-atualizado--resolvido)
protegia isso comparando **apenas o número de linhas**.

**O que passava.** Um arquivo com **mais linhas porém menos dias** — gordo no começo do
mês, vazio no fim — era aceito, e os dias do fim sumiam. Reproduzido:

| Cenário | Antes | Agora |
|---|---|---|
| Arquivo truncado (25 linhas vs 100) | ✅ rejeitado | ✅ rejeitado |
| Arquivo maior, mesmos dias | ✅ aceito | ✅ aceito |
| **120 linhas / dias 1–15 vs 100 linhas / dias 1–20** | ❌ **aceito, dias 16–20 perdidos em silêncio** | ✅ **rejeitado** |

**E nenhuma validação pegava.** Depois da substituição em bloco, `db_atualizado` fica
idêntico ao arquivo novo — então `detectar_gaps`, que compara os dois, não vê diferença
(ele só dispara quando o período foi *rejeitado*). E `validar_continuidade` compara o fim
da base com o início do arquivo, que nesse caso coincidem. Mesmo formato da perda de
26–30/06/2026: sem sinal nenhum.

**A resolução.** O aceite passou a exigir **duas** condições:

1. **Volume** — o arquivo novo tem ≥ linhas que o banco.
2. **Cobertura** — o arquivo novo traz todos os dias que o banco já tem naquele mês.

O log da rejeição diz qual das duas falhou, porque as causas são diferentes: menos
linhas costuma ser download truncado; dias faltando costuma ser arquivo parcial.

**Contagem igual continua sendo aceita**, e isso é deliberado — o NABSA reformula
parcelas sem mudar o número de linhas (SAROCHA NAREE, 14/04/2026: 14.859,14 + 25.179,20
virou 20.000,00 + 20.038,34, soma idêntica). Com `>` no lugar de `>=`, correções da fonte
nunca entrariam.

**Impacto medido em produção: nenhum.** Com o arquivo do NABSA de hoje, 2026-07 tem
547 linhas / 28 dias dos dois lados — aceito pelas duas regras. A trava nova só age no
cenário que antes passava em silêncio.

### 9.5 O dashboard achatado por uma linha impossível — ✅ **RESOLVIDO** *(2026-08-03)*

Ao montar o dashboard HTML, a série mensal de 2018–2026 apareceu como uma linha
quase reta com **um pico isolado de 60,6 Mt** em novembro de 2025, contra uma média
de 9,2 Mt. Não era erro de agregação.

**A causa.** Uma única linha da base:

| Data | Navio | Carga | Destino | Tons |
|---|---|---|---|---|
| 20/11/2025 | EPIC RADIANCE | SOYBEANMEAL | TURKEY | **49.806.070,5** |

Quase 50 milhões de toneladas num navio só. O maior graneleiro em operação carrega
cerca de **400 mil**; o segundo maior carregamento de toda a base é 445 mil. A linha
sozinha valia **5,3% das 942,9 Mt** do histórico inteiro.

**O estrago não era só o pico.** Como todas as escalas derivam do máximo, aquela
linha achatava tudo: o heatmap de sazonalidade ficava de um tom só (uma célula no
topo da rampa, 102 espremidas na base), o mix de cargas por ano tinha 2025 fora de
proporção, e SOYBEANMEAL aparecia com 281,1 Mt em vez de 231,3 Mt.

**A resolução.** `LIMITE_TONELAGEM_NAVIO = 500_000` em `reporting/dashboard.py`.
Linhas acima disso saem das agregações e vão inteiras para `payload.sailed.anomalias`,
que a página exibe **em destaque no topo**, com navio, data, carga, destino e
tonelagem.

**Não é filtro de outlier estatístico, e a distinção importa.** O corte é físico —
o que nenhum navio pode carregar — e não relativo à distribuição. Por isso o
carregamento de 445 mil t de minério continua nos gráficos: é grande, não é
impossível. Um corte estatístico (3σ, percentil 99) tiraria dados legítimos e
mudaria de resultado a cada rodada.

**Nada é descartado em silêncio** — esse era o requisito. O dado não some, muda de
lugar: sai do gráfico, onde mentia sobre a escala, e vira um aviso que diz
exatamente o que foi encontrado. `test_tonelagem_impossivel_e_exibida_e_nao_descartada`
prende isso.

**A correção real é na origem** — resolvida na [9.6](#96-corrigir-à-mão-não-gruda--resolvido-2026-08-03).
A quarentena virou a **segunda** linha de defesa: o caso conhecido chega ao dashboard
já corrigido, e o que ela pega agora é o caso *novo*, que ninguém ainda registrou.

### 9.6 Corrigir à mão não gruda — ✅ **RESOLVIDO** *(2026-08-03)*

Sequência da [9.5](#95-o-dashboard-achatado-por-uma-linha-impossível--resolvido-2026-08-03).
O relato de quem opera: *"eu já tinha corrigido na mão, porém sempre volta"*.

**Onde nasce.** No NABSA, não em casa. O arquivo bruto baixado em 02/12/2025
(`vessels_sailed_update_Sailed Vessels_2025-12-02.xlsx`, linha 326) já traz
`49806067.00`. Uma linha isolada num arquivo de 614; as vizinhas estão normais
(46.598,47 / 7.700 / 21.890). Nosso parser está correto.

**Por que voltava.** Tudo depois do merge é reescrito a partir do mesmo
`db_atualizado`:

| Onde a correção era feita | O que acontece na rodada seguinte |
|---|---|
| `Arg_Sailed` no SQL | `DELETE` + `INSERT` total a partir da base → **volta** |
| Planilha do OneDrive | regerada a partir da base → **volta** |
| `data/db/Arg_sailed_database.xlsx` | é a fonte de verdade → gruda, até o NABSA republicar o mês |

E se o NABSA voltar a publicar novembro/2025, a substituição em bloco do merge
(9.1/9.4) traz o valor errado de volta mesmo na base. **Nenhum ponto do sistema
tornava uma correção permanente.**

**A resolução.** `config/correcoes_sailed.csv`, versionado, aplicado por
`pipelines/correcoes.py` como etapa 4b — depois do merge, antes da persistência.
Sendo reaplicada a cada rodada, a correção alcança todos os destinos (base, SQL,
OneDrive, dashboard, Power BI) e sobrevive tanto à reescrita quanto à
substituição em bloco.

**O valor errado faz parte da chave de casamento** — e essa é a decisão de
projeto que importa. Uma regra só age quando data, navio, carga, destino **e o
valor errado** batem todos. No dia em que a origem corrigir o dado, a regra deixa
de casar sozinha e não faz nada, em vez de sobrescrever o valor certo com o nosso
palpite. Regra que não casou vira WARNING: ou a origem consertou, ou a linha mudou
de forma e a correção precisa ser revista. O arquivo não apodrece em silêncio.

**O valor adotado: 49.806,07 t — erro de digitação na base do NABSA, confirmado
pelo operador em 03/08/2026.** O valor publicado é o correto multiplicado por mil.

A confirmação fechou o que a evidência já indicava: os dígitos batem exatamente com
49.806,067 (ponto decimal deslocado em três casas); o valor cabe no perfil da carga
(SOYBEANMEAL tem percentil 99 em 49.993 t, e o maior embarque já feito para a Turquia
foi 51.508 t); e o próprio EPIC RADIANCE aparece em 22/09/2024 levando 35.305 t da
mesma carga para a Espanha. Tudo registrado na coluna `motivo` do CSV — inclusive a
distinção entre o que era inferência e o que passou a ser fato confirmado, porque
daqui a dois anos ninguém lembra qual era qual.

**Uma correção tem duas vidas, e confundi-las gera alarme falso diário.** Daí a
coluna `modo`:

| modo | Quando usar | Não casou | Casou |
|---|---|---|---|
| `aplicar` | a origem ainda publica o valor errado | **WARNING** — ou a origem consertou, ou a chave está errada | INFO, corrige |
| `guarda` | o dado já foi consertado na base | silêncio: é o estado esperado | **WARNING** — regressão, a origem republicou o erro |

Isto não é refinamento teórico. Em 03/08/2026, horas depois de a camada entrar no ar,
o operador corrigiu a base à mão — e a regra passou a avisar "correção sem efeito"
toda rodada. Num projeto onde os avisos vão para o e-mail, um alarme que sempre dispara
treina quem lê a ignorar todos. A regra do EPIC RADIANCE está em `guarda` desde então:
cala enquanto o dado estiver certo e grita se o erro voltar. Coluna opcional — arquivo
sem ela se comporta como `aplicar`.

**E uma validação para o próximo.** `validar_tonelagem` em `validation.py` sinaliza
qualquer embarque acima do limite físico, no log e no e-mail. O erro do EPIC
RADIANCE passou meses despercebido porque nenhuma validação olhava a **grandeza**
dos valores: `detectar_gaps` conta dias, `validar_continuidade` compara bordas, e um
número absurdo dentro de um dia que existe não viola nenhuma das duas. Roda depois
das correções, para o caso conhecido não alarmar toda noite.

**Efeito medido:** total do histórico de 942.869.139 t para 893.112.878 t;
linhas impossíveis de 1 para 0; contagem de linhas inalterada em 47.023.

### Decisões ainda abertas

*(nenhuma no momento)*

---

## 10. Dívida técnica conhecida

Fora do escopo da migração, mas registrada para não se perder:

- **`Arg_sailed_databease.xlsx` continua preso em `Sincronização pendente`** — desde
  28/07 23:48, sem causa identificada. Não é espaço em disco, cliente parado, trava
  local, problema de biblioteca nem *check-out*. Um arquivo novo criado na mesma pasta
  sincroniza na hora. Contorno possível: renomear o local para o OneDrive tratá-lo como
  novo, com risco de gerar cópia em conflito. A Fase H torna isso irrelevante.
- **Client secret do Graph expira em 28/07/2028** — na expiração a publicação para. O
  erro do Graph é claro (`invalid_client: secret expired`), mas só aparece se alguém
  ler o log ou o e-mail.
- **Line-Up sem snapshots de 26/06 a 27/07** — 33 dias em que a tabela `Arg_Lineup` não
  recebeu nada. Reconstituível a partir de `data/raw/lineup/`, mas exige que
  `_classificar_status` receba a data do snapshot em vez de usar `date.today()`: sem isso, um
  navio que estava `EXPECTED` em 30/06 entraria como `WAITING`, porque seria classificado com
  o conhecimento de hoje. Decidir se vale o esforço — o Lineup é forward-looking e o valor
  histórico dele é menor que o do Sailed.
- **O repositório aposentado não deve ser executado** — `lineup_processor.py` e
  `Database.py` de lá mantêm três bugs corrigidos aqui: o do `pd.NaT`, o do
  `NaN`/`pyodbc` e o do `DataFrame` vazio no `merge_com_banco` (o ramo "nenhum período
  aceito" converte `Date` para dtype `object` e quebra o `.dt`; os 7 testes de lá nunca
  cobriram "arquivo novo vazio"). Além disso, o downloader dele está quebrado desde
  sempre — foi a causa das 21 noites. Ele existe como registro histórico, em
  `C:\Users\server\_arquivo\Argentina-aposentado-2026-07-29`, tag
  `aposentado-2026-07-29`.
- ~~**2 testes falhando no `sailed_auto`, anteriores à migração**~~ — ✅ **resolvido**
  em 2026-07-29; ver [Fase I](#-fase-i--suíte-verde-concluída-em-2026-07-29).
- **Tarefa agendada com `LogonType: Interactive`** — só roda com o usuário logado. O `S4U`
  (rodar sem login) foi negado por falta de elevação ao recriar a `new_sailed_task`. Se a
  máquina for deslogada, o pipeline não executa. Hoje isso é uma fragilidade **dupla**: a
  tarefa **e** o cliente do OneDrive dependem de sessão ativa — a Fase H elimina a segunda
  metade. Comando de correção na [Fase E](#fase-e--aposentar-desktopargentina-quase-concluída-em-2026-07-28).
- **`detectar_gaps` é cego para períodos fora do arquivo novo** — limite de escopo, não bug,
  mas precisa continuar documentado: quem ler o nome da função vai supor mais alcance do que
  ela tem. Coberto por `test_gaps_e_cego_para_periodo_fora_do_arquivo_novo` e complementado
  por `validar_continuidade`. **A docstring, que descrevia outro comportamento, foi
  reescrita em 2026-07-29** e agora declara o limite explicitamente.

### ✅ Resolvidas em 2026-07-29 *(Fase J)*

- ~~**Docstring de `detectar_gaps`**~~ — dizia comparar "com o banco ANTES da atualização";
  o código sempre comparou com o banco **depois**. Reescrita, com o limite de escopo e a
  história da suposição errada da Fase B dentro dela.
- ~~**`EMAIL_BACKEND` com texto colado no valor**~~ — era **pior que "polui o log"**. O valor
  `smtp         ← adicione essa linha` fazia `_BACKEND` não ser igual nem a `"smtp"` nem a
  `"graph"`. O envio funcionava **por acidente** (`_dispatch` cai no `else`), mas o
  `if _BACKEND == "smtp"` do tratamento de erro nunca era verdadeiro: a dica de configurar
  `EMAIL_BACKEND=graph`, que só apareceria numa falha de envio, estava **morta**. Corrigido
  nos dois lados — valor limpo no `.env` (backup em `.env.bak-20260729-dividatecnica`) e
  `_primeira_palavra()` no `report.py`, que lê só o primeiro token.
- ~~**Logs de outros produtores em `logs/`**~~ — eram **três** produtores, não dois:
  `argentina_updater.log*` (vivo, 31 arquivos), `argentina_etl_*.log` (o `.bat` aposentado,
  25 arquivos, até 27/07/2026) e `process_log_*.log` (**133 arquivos**, do `concater.py`,
  parado desde 10/03/2026 — este não estava registrado). Os 158 órfãos foram **movidos, não
  apagados**, para `logs/_produtores-aposentados/`: são a evidência das 21 noites de falha.
- ~~**6 colunas `Unnamed: 13`–`Unnamed: 18`**~~ — `remover_colunas_sem_nome()` em
  `pipelines/sailed.py`, aplicada na leitura da base para não voltarem pelo write-back.
  19 → 13 colunas, 46.945 linhas preservadas.

  ⚠️ **A `Unnamed: 18` não estava vazia.** Escondia uma anotação humana —
  *"ultima linha do banco do almyr"* — na linha do **PRABHU PUNI, SAN LORENZO,
  14/01/2020, 14.010,34 t de WHEAT**, marcando a fronteira de uma base anterior.
  Registrada aqui porque a coluna some, e a função avisa em `WARNING` antes de
  descartar qualquer coluna sem nome que tenha conteúdo.
- ~~**Esquemas divergentes**~~ — **intencional, verificado no servidor.** `dbo.Arg_Sailed`
  tem exatamente 8 colunas (`Date`, `Destination`, `Origin`, `Cargo`, `Tons`, `Month`,
  `Year` + `UpdatedAt`), então `COLUNAS` projetar 7 está certo — acrescentar colunas
  quebraria o `INSERT`. As colunas ricas (`Port`, `Terminal`, `Vessel`, `Status`,
  `Coordinator`, `Charterer`) vivem só no `.xlsx`. Base e SQL batem em **46.945 linhas**.
- ~~**`data/archive/____.xlsx`**~~ — **identificado: snapshot velho, superado.** 8.684 linhas
  únicas, das quais 8.681 já estão na base; cobre até 17/04/2026 (a base vai a 28/07). As
  3 exclusivas são **reformulações da fonte, não perda**:

  | Registro | Situação |
  |---|---|
  | SAROCHA NAREE, 14/04, 2 linhas | O NABSA redividiu as parcelas: 14.859,14 + 25.179,20 virou 20.000,00 + 20.038,34. **Soma idêntica: 40.038,34 t** |
  | DITLEV REEFER, 12/04, CITRUS 25.000 t | Removido pela fonte. A base tem *mais* linhas nesse dia (19 vs 18) e mais CITRUS no total (18 vs 12) — não é truncamento |

  Não guarda nada de único. Mantido em `data/archive/` (que é gitignored); descartá-lo é
  seguro, mas não urgente.

---

## Apêndice A — Reescrita de histórico (avaliada e descartada)

Considerada para remover binários do histórico de `Desktop\Argentina` com `git-filter-repo`.
**A medição não justificou:**

```
.git completo ........... 1,1 MB
pack .................... 191 KB
maior blob do histórico . 0,32 MB  (Arg_sailed_database.xlsx, versão antiga)
```

A versão de 3,9 MB do banco estava modificada e não commitada — nunca entrou no histórico.
O `git rm --cached` da Fase 1 impediu que entrasse. Não havia inchaço a limpar.

Contra o ganho ~zero pesava o custo: o repositório tem remoto
(`github.com/eduardodiamandis/Argentina_sailed`) e 4 branches, exigindo `push --force` em
todas e invalidando qualquer clone existente.

Se for necessário no futuro (um segredo commitado por engano, por exemplo):

```bash
cp -r .git ../git-backup-$(date +%Y%m%d)     # backup antes de qualquer coisa
pip install git-filter-repo
git filter-repo --path-glob '*.xlsx' --invert-paths
git remote add origin <url>                   # filter-repo remove o remoto por segurança
git push --force --all
```

Avise quem mais tiver clone: todos precisam re-clonar, não fazer `pull`.
