# ARQUITETURA.md — como o pipeline funciona

Descreve o comportamento do código como ele é hoje. Para *onde as coisas moram e
por quê*, ver `ESTRUTURA.md`. Para *como instalar e rodar*, ver `README.md`.

---

## 1. Os dois fluxos

O pipeline processa duas fontes do NABSA, com naturezas opostas:

| | **Sailed** | **Line-Up** |
|---|---|---|
| Arquivo | `vessels_sailed_update.xlsx` | `vessel_update.xlsx` |
| Natureza | histórico de navios que já partiram | previsão do que está por vir |
| Tabela | `Arg_Sailed` | `Arg_Lineup` |
| Persistência | `DELETE` + `INSERT` total | apenas `INSERT`, um snapshot por dia |
| Criticidade | aborta o pipeline se falhar | não-crítico |

**Por que a diferença.** O Sailed é o estado do mundo: substituí-lo por inteiro
garante que o banco reflete a última verdade conhecida. O Line-Up é um *retrato* do
que se previa num determinado dia — por isso acumula, nunca apaga. Comparar
snapshots de dias diferentes mostra como as previsões mudaram.

Navios com `Status=SAILED` são removidos do Line-Up antes da inserção, para não
duplicar o que já está em `Arg_Sailed`.

## 2. As etapas

`__main__.py` orquestra e nada mais — a regra de negócio mora em `pipelines/` e a
escrita em `storage/`.

### 1 — Download

`downloader.py` abre a URL num Chrome headless, numa pasta temporária isolada, e
aguarda o `.xlsx` aparecer sem `.crdownload`.

**Por que Selenium e não `requests`:** as URLs do NABSA devolvem uma página HTML com
redirecionamento por JavaScript. Um cliente HTTP simples recebe o HTML, não o
arquivo — foi o que fez o pipeline antigo falhar por 21 noites seguidas, sempre com
o mesmo timeout.

O arquivo baixado é validado (tamanho mínimo e *magic bytes* `PK` de ZIP) e
renomeado com a data máxima encontrada no conteúdo. Falha aqui **aborta o pipeline**:
sem o arquivo não há o que processar.

### 1b — Snapshot do Line-Up

Lê o arquivo do dia, classifica cada navio e grava em `Arg_Lineup`.

A classificação (`_classificar_status`) usa as datas disponíveis, em ordem de
prioridade: `ETF` no passado → `SAILED`; `ETB` no passado → `LOADING`; `ETA` no
passado → `WAITING`; `ETA` no futuro → `EXPECTED`; sem datas → `TBC`.

⚠️ A comparação usa `date.today()`, não a data do snapshot. Para a execução diária dá
no mesmo, mas **reconstituir dias passados produziria status recalculados com o
conhecimento de hoje** — ver `ESTRUTURA.md`, seção 10.

Etapa não-crítica: falha aqui só registra erro e segue.

### 2 — Leitura do arquivo novo

`pipelines/sailed.py` lê com `header=7`, descarta o rodapé e coage as datas.

O detector de rodapé (`_cortar_apos_duas_linhas_vazias`) só corta a partir de duas
linhas vazias consecutivas **que estejam depois de 70% dos dados** — há dias sem
embarque no meio do arquivo, e cortar neles truncaria o mês.

`validar_corte_rodape` avisa se a última data for anterior ao dia 15, sinal de corte
prematuro.

### 3 e 4 — Leitura do banco e merge

**`merge_com_banco` é a peça mais importante do pipeline.** Compara período a
período (mês/ano):

- Período do arquivo novo com linhas **≥** às do banco → substitui
- Período com **menos** linhas → **rejeitado**, o banco prevalece, com aviso no log

Sem essa trava, um arquivo truncado do NABSA destruiria o mês inteiro — e de forma
permanente, já que a base é reescrita a cada execução (etapa 5).

Em seguida, duas validações complementares:

**`validar_continuidade`** compara o fim da base com o início do arquivo novo. É esta
que detecta a classe de perda ocorrida em junho/2026: base parada em 25/06 e arquivo
trazendo só julho abriram um vão de 5 dias que ninguém preenchia. Tolerância padrão
de 3 dias, porque nem todo dia tem embarque.

**`detectar_gaps`** olha os dias que existiam no banco e sumiram após o merge — na
prática, quando a trava rejeitou um período. **Não** pega o caso acima: só examina os
períodos presentes no arquivo novo.

Os avisos das duas saem como `WARNING`, e o `_extract_warnings` do `report.py` os
recolhe do log — aparecem na seção "Avisos" do e-mail sem acoplamento extra.

### 5 — Persistência

Quatro destinos, cada um em seu `try`:

| Destino | Módulo | Observação |
|---|---|---|
| Snapshot datado | `storage/excel.py` | nome derivado de `PATH_DATABASE_OUTPUT` |
| **Base principal** | `storage/excel.py` | reescrita a cada execução |
| OneDrive | `storage/onedrive.py` | 5 sheets, incluindo as pivots |
| SQL Server | `storage/sql_server.py` | `DELETE` + `INSERT` |

**A reescrita da base é o que impede o congelamento.** Enquanto o arquivo do NABSA
entrega o mês corrente, o merge recompõe o mês e um banco parado passa despercebido.
Na virada do mês, o vão aparece — e some do SQL e do Power BI sem sinal algum.

As pivots são calculadas com pandas, com os filtros explícitos no código
(`PIVOT_ORIGIN`, `PIVOT_CARGO`). A automação COM do Excel foi removida: além de
exigir Excel instalado e deixar processos órfãos, ela resolvia o caminho local para
a URL do SharePoint e construía a pivot sobre a cópia do servidor.

### 6 — Relatório

`reporting/report.py` monta um HTML com as estatísticas do banco e a lista de avisos
extraída do log, e envia por SMTP.

## 3. Publicação no SharePoint

Duas rotas coexistem:

**Pasta sincronizada (atual).** O pipeline grava a cópia local e o cliente do
OneDrive publica. Não há confirmação de entrega — `verificar_sincronizacao` lê o
status pelo shell do Windows e avisa quando o arquivo não sobe, mas não conserta.

**Microsoft Graph API (`storage/sharepoint.py`).** Fluxo *client credentials*, upload
por sessão em blocos. Retorna `201` com o `eTag` do item no servidor: confirmação
real de entrega. Implementada e testada; **aguardando concessão de permissão**. Ligar
com `GRAPH_UPLOAD_ENABLED=true`.

## 4. Configuração e execução

`config.py` é o único módulo que lê o ambiente; obrigatórias via `_require()`
levantam `EnvironmentError` na inicialização — falha cedo e clara, em vez de
`None` propagando.

O logger (`logging_setup.py`) escreve em `logs/`, derivado da raiz do projeto, com
rotação diária e retenção de 30 dias.

A tarefa `new_sailed_task` executa `python.exe main.py` diariamente às 23:45.

## 5. Onde o pipeline pode falhar em silêncio

Registrado porque **as três falhas graves deste projeto foram todas silenciosas** —
o sistema parecia estar funcionando.

| Risco | Mitigação |
|---|---|
| Download falha | Aborta o pipeline e envia e-mail de erro |
| Arquivo truncado do NABSA | Trava do merge rejeita o período |
| Base congelada | Reescrita a cada execução + `validar_continuidade` |
| Dias somem no merge | `detectar_gaps` |
| Arquivo não chega ao SharePoint | `verificar_sincronizacao` avisa; o Graph confirmará |
| Line-Up falha | Não-crítico por desenho — mas o snapshot do dia se perde |
| Secret do Graph expira | **Sem mitigação.** Expira em 28/07/2028 |
