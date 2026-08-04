"""
reporting/dashboard.py
----------------------
Gera o dashboard HTML de Sailed e Line-Up.

A saida e um **arquivo unico e autocontido**: os dados vao embutidos como JSON e
nao ha nenhuma requisicao externa (sem CDN, sem fonte remota, sem fetch). Isso e
requisito, nao estilo — o arquivo foi feito para ser embutido em outra aplicacao
via iframe, possivelmente atras de um CSP restritivo ou sem rede.

Fontes de dados, e por que sao duas:
  - Sailed vem do **Excel**, nao do SQL. A tabela Arg_Sailed guarda so 8 colunas
    (Date, Destination, Origin, Cargo, Tons, Month, Year, UpdatedAt); Port,
    Terminal, Vessel e Charterer existem apenas na planilha. O dashboard usa
    essas dimensoes, entao le a fonte mais rica.
  - Line-Up vem do **SQL**, porque e la que mora o historico de snapshots. O
    arquivo bruto do dia so tem a foto do dia.

Sobre o tamanho do payload: as 47 mil linhas do Sailed nao vao cruas para o HTML
(seriam megabytes). Elas viram tres tabelas-fato pre-agregadas com as caudas
longas colapsadas em OUTROS, o que da ~7,8 mil linhas. O corte por (ano, carga)
aparece nas tres, e e por isso que o filtro de ano e carga consegue reescopar
todos os graficos de uma vez sem precisar dos dados brutos no navegador.

Uso:
    python -m argentina_etl.reporting.dashboard
"""
from __future__ import annotations

import datetime
import json
import warnings
from pathlib import Path

import pandas as pd

from argentina_etl.logging_setup import logger
from argentina_etl.validation import LIMITE_TONELAGEM_NAVIO

# Quantos valores distintos cada dimensao mantem antes de colapsar o resto em
# OUTROS. Sao o botao de tamanho do arquivo: subir estes numeros multiplica as
# linhas da tabela-fato, porque elas entram no produto cartesiano do group by.
TOPO_CARGAS = 12
TOPO_DESTINOS = 15
TOPO_TERMINAIS = 20

# Rotulo do balde de cauda longa. Aparece na tela, entao acompanha o idioma da
# interface (ingles). Vai no payload como `rotuloResiduo`: a pagina precisa saber
# qual e o rotulo para nao deixa-lo disputar posicao num ranking de destinos nem
# virar uma segunda fatia cinza no mix — e ler daqui e melhor do que repetir a
# string no template, onde as duas poderiam divergir em silencio.
ROTULO_OUTROS = "OTHER"

# Mesmo limite fisico usado pela validacao do pipeline — uma fonte so, para os
# dois nao divergirem. As linhas acima dele saem das agregacoes e vao inteiras
# para payload["sailed"]["anomalias"], que a pagina exibe em destaque com navio,
# data e tonelagem. Nada e descartado em silencio.
#
# Esta quarentena e a **segunda** linha de defesa. A primeira e
# config/correcoes_sailed.csv, aplicado logo na carga: o caso conhecido chega
# aqui ja corrigido. O que a quarentena pega e o caso novo, que ninguem ainda
# registrou — sem ela, uma linha absurda achataria a escala de todos os
# graficos de uma vez.
_TEMPLATE = Path(__file__).with_name("dashboard_template.html")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _colapsar_cauda(serie: pd.Series, topo: int) -> pd.Series:
    """
    Mantem os `topo` valores mais frequentes e troca o resto por OUTROS.

    Trabalha sobre texto: nulos viram OUTROS tambem, para nao abrir uma
    categoria vazia no grafico.
    """
    limpa = serie.fillna(ROTULO_OUTROS).astype(str).str.strip().replace("", ROTULO_OUTROS)
    mantidos = limpa.value_counts().index[:topo]
    return limpa.where(limpa.isin(mantidos), ROTULO_OUTROS)


def _indexar(serie: pd.Series) -> tuple[list[str], dict[str, int]]:
    """
    Constroi a lista de rotulos de uma dimensao e o mapa rotulo -> indice.

    OUTROS vai sempre para o fim da lista, para que o grafico consiga pinta-lo
    de cinza sem depender da ordem alfabetica.
    """
    rotulos = sorted(r for r in serie.unique() if r != ROTULO_OUTROS)
    if (serie == ROTULO_OUTROS).any():
        rotulos.append(ROTULO_OUTROS)
    return rotulos, {r: i for i, r in enumerate(rotulos)}


def _tons(valor) -> float:
    """Tonelagem arredondada — uma casa basta e encolhe bastante o JSON."""
    return round(float(valor), 1)


def _texto(valor) -> str | None:
    """Normaliza celula de texto; nulo e string vazia viram None."""
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return None
    if pd.isna(valor):
        return None
    texto = str(valor).strip()
    return texto or None


def _iso(valor) -> str | None:
    """
    Data em ISO, ou None.

    Cuidado conhecido do projeto: pd.NaT passa em isinstance(x, datetime.date)
    porque herda de datetime.datetime. Por isso o pd.isna vem primeiro.
    """
    if valor is None or pd.isna(valor):
        return None
    if isinstance(valor, str):
        return valor[:10] or None
    return pd.Timestamp(valor).date().isoformat()


# ---------------------------------------------------------------------------
# Agregacao — Sailed
# ---------------------------------------------------------------------------

def agregar_sailed(df: pd.DataFrame) -> dict:
    """
    Transforma o historico do Sailed nas tres tabelas-fato do dashboard.

    Por que duas e nao uma: uma unica tabela no grao
    (ano, mes, carga, destino, terminal) daria ~20 mil linhas, porque o group by
    cruza todas as dimensoes. Separando por assunto — o que cada grafico
    realmente precisa — sao ~13 mil.

      fatoMes      (ano, mes, carga, destino) -> serie temporal, sazonalidade,
                                                 mix de cargas, ranking destinos
      fatoTerminal (ano, mes, carga, terminal) -> ranking de terminais

    **As duas carregam (ano, mes, carga), e isso e requisito, nao coincidencia.**
    E o que permite aos filtros de ano, mes e carga reescoparem todos os graficos
    de uma vez. Uma tabela sem alguma dessas chaves ficaria fora do filtro e
    passaria a contar uma historia diferente das outras na mesma tela.
    """
    df = df[df["Tons"].notna() & df["Date"].notna()].copy()
    if df.empty:
        raise ValueError("Sailed sem linhas utilizaveis (Tons/Date todos nulos).")

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df[df["Date"].notna()].copy()

    # Quarentena das tonelagens impossiveis — ver LIMITE_TONELAGEM_NAVIO.
    suspeitas = df[df["Tons"] > LIMITE_TONELAGEM_NAVIO]
    anomalias = [
        {
            "data": _iso(r.Date),
            "navio": _texto(r.Vessel),
            "carga": _texto(r.Cargo),
            "destino": _texto(r.Destination),
            "porto": _texto(r.Port),
            "tons": _tons(r.Tons),
        }
        for r in suspeitas.sort_values("Tons", ascending=False).itertuples(index=False)
    ]
    if anomalias:
        logger.warning(
            f"  Dashboard: {len(anomalias)} embarque(s) com tonelagem acima de "
            f"{LIMITE_TONELAGEM_NAVIO:,} t fora das agregacoes (exibidos como aviso)."
        )
    df = df[df["Tons"] <= LIMITE_TONELAGEM_NAVIO].copy()
    if df.empty:
        raise ValueError("Sailed sem linhas plausiveis apos a quarentena de tonelagem.")

    # Month/Year existem na planilha, mas sao derivados e podem estar
    # dessincronizados de Date. Date e a fonte de verdade.
    df["_ano"] = df["Date"].dt.year.astype(int)
    df["_mes"] = df["Date"].dt.month.astype(int)

    df["_carga"] = _colapsar_cauda(df["Cargo"], TOPO_CARGAS)
    df["_destino"] = _colapsar_cauda(df["Destination"], TOPO_DESTINOS)
    df["_terminal"] = _colapsar_cauda(df["Terminal"], TOPO_TERMINAIS)

    cargas, i_carga = _indexar(df["_carga"])
    destinos, i_destino = _indexar(df["_destino"])
    terminais, i_terminal = _indexar(df["_terminal"])

    def _fato(chaves: list[str], mapas: list[dict | None]) -> list[list]:
        g = (
            df.groupby(chaves, dropna=False)
            .agg(tons=("Tons", "sum"), n=("Tons", "size"))
            .reset_index()
        )
        linhas = []
        for registro in g.itertuples(index=False):
            valores = list(registro)[: len(chaves)]
            codificados = [
                mapa[valor] if mapa is not None else int(valor)
                for valor, mapa in zip(valores, mapas)
            ]
            linhas.append(codificados + [_tons(registro.tons), int(registro.n)])
        return linhas

    return {
        "dims": {
            "cargas": cargas,
            "destinos": destinos,
            "terminais": terminais,
        },
        "fatoMes": _fato(
            ["_ano", "_mes", "_carga", "_destino"], [None, None, i_carga, i_destino]
        ),
        "fatoTerminal": _fato(
            ["_ano", "_mes", "_carga", "_terminal"], [None, None, i_carga, i_terminal]
        ),
        "anos": sorted(int(a) for a in df["_ano"].unique()),
        "periodo": {
            "inicio": _iso(df["Date"].min()),
            "fim": _iso(df["Date"].max()),
        },
        "totais": {
            "tons": _tons(df["Tons"].sum()),
            "embarques": int(len(df)),
            # Contagens reais, antes de colapsar as caudas. A pagina nao pode
            # deriva-las das dimensoes: la 'destinos' tem 16 entradas (top 15 +
            # OUTROS), nao os 146 distintos que existem de verdade.
            "destinos": int(df["Destination"].nunique()),
            "cargas": int(df["Cargo"].nunique()),
        },
        "anomalias": anomalias,
        "limiteTonelagem": LIMITE_TONELAGEM_NAVIO,
        "rotuloResiduo": ROTULO_OUTROS,
    }


# ---------------------------------------------------------------------------
# Agregacao — Line-Up
# ---------------------------------------------------------------------------

def agregar_lineup(df: pd.DataFrame) -> dict:
    """
    Monta a foto atual da fila e o historico de snapshots.

    O snapshot mais recente vai linha a linha (sao ~200 navios, cabe folgado no
    JSON e o operador quer ver navio por navio). Os snapshots anteriores viram
    so totais por status, para o grafico de evolucao da fila.

    Status vem classificado por pipelines/lineup.py: EXPECTED, WAITING, LOADING
    e TBC (quando nao ha data nenhuma para decidir). SAILED nunca chega aqui —
    o pipeline filtra antes de gravar, para nao duplicar o que ja e Sailed.
    """
    if df.empty:
        return {"snapshot": None, "navios": [], "historico": [], "statuses": []}

    df = df.copy()
    df["SnapshotDate"] = pd.to_datetime(df["SnapshotDate"], errors="coerce")
    df = df[df["SnapshotDate"].notna()]
    if df.empty:
        return {"snapshot": None, "navios": [], "historico": [], "statuses": []}

    ultimo = df["SnapshotDate"].max()
    atual = df[df["SnapshotDate"] == ultimo]

    navios = [
        {
            "porto": _texto(r.Port),
            "terminal": _texto(r.Terminal),
            "navio": _texto(r.Vessel),
            "commodity": _texto(r.Commodity),
            "destino": _texto(r.Destination),
            "charterer": _texto(r.Charterer),
            "status": _texto(r.Status) or "TBC",
            "tons": _tons(r.Tons) if pd.notna(r.Tons) else None,
            "eta": _iso(r.ETA_Date),
            "etb": _iso(r.ETB_Date),
            "etf": _iso(r.ETF_Date),
        }
        for r in atual.itertuples(index=False)
    ]

    # O historico e uma tabela-fato, e nao um total por snapshot, para que os
    # filtros da pagina (porto, status, carga) tambem o alcancem. Com totais
    # prontos, o grafico de evolucao da fila contaria uma historia diferente do
    # resto da tela assim que qualquer filtro fosse ligado.
    #
    # Sao ~600 linhas para 8 snapshots — cabe folgado, e por isso os rotulos vao
    # como texto em vez de indices: legivel na inspecao, sem custo relevante.
    agrupado = (
        df.assign(
            porto=df["Port"].fillna("—"),
            status=df["Status"].fillna("TBC"),
            carga=df["Commodity"].fillna("—"),
        )
        .groupby(["SnapshotDate", "porto", "status", "carga"], dropna=False)
        .agg(tons=("Tons", "sum"), navios=("Tons", "size"))
        .reset_index()
        .sort_values("SnapshotDate")
    )
    historico = [
        [
            _iso(r.SnapshotDate),
            str(r.porto),
            str(r.status),
            str(r.carga),
            _tons(r.tons),
            int(r.navios),
        ]
        for r in agrupado.itertuples(index=False)
    ]

    return {
        "snapshot": _iso(ultimo),
        "navios": navios,
        "historico": historico,
        "datasHistorico": sorted({h[0] for h in historico if h[0]}),
        "statuses": sorted({n["status"] for n in navios}),
    }


# ---------------------------------------------------------------------------
# Carga das fontes
# ---------------------------------------------------------------------------

def carregar_sailed(caminho: Path, path_correcoes: Path | None = None) -> pd.DataFrame:
    """
    Le a aba data_base da planilha do Sailed e aplica as correcoes conhecidas.

    As correcoes entram aqui, e nao so no pipeline, porque o dashboard le a base
    direto do disco. Sem isto ele mostraria o valor errado ate a proxima rodada
    noturna reescrever a base — e ficaria em desacordo com o SQL, que ja teria
    sido gravado corrigido.
    """
    logger.info(f"  Dashboard: lendo Sailed de {caminho}")
    df = pd.read_excel(caminho, sheet_name="data_base", engine="openpyxl")

    if path_correcoes is not None:
        from argentina_etl.pipelines.correcoes import aplicar_correcoes, carregar_correcoes

        try:
            df = aplicar_correcoes(df, carregar_correcoes(path_correcoes))
        except Exception as erro:  # noqa: BLE001 — dashboard nao cai por isso
            logger.warning(f"  Dashboard: correcoes nao aplicadas ({erro}).")
    return df


def carregar_lineup(server: str, database: str, table: str) -> pd.DataFrame:
    """Le o historico completo de snapshots do Line-Up."""
    import pyodbc

    from argentina_etl.storage.sql_server import _conn_str

    logger.info(f"  Dashboard: lendo Line-Up de {database}.{table}")
    conexao = pyodbc.connect(_conn_str(server, database))
    try:
        # O pandas avisa que so da suporte oficial a SQLAlchemy. Aqui e leitura
        # pura com a mesma conexao pyodbc que o resto do projeto ja usa, entao o
        # aviso so sujaria o log — trocar por SQLAlchemy traria uma dependencia
        # nova sem ganho.
        # Filtra pela mensagem, nao por module=: o stacklevel do aviso aponta
        # para este arquivo, entao um filtro por modulo 'pandas' nao casaria.
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", message=".*only supports SQLAlchemy.*", category=UserWarning
            )
            return pd.read_sql(f"SELECT * FROM {table}", conexao)
    finally:
        conexao.close()


# ---------------------------------------------------------------------------
# Renderizacao
# ---------------------------------------------------------------------------

def renderizar(payload: dict, template: str) -> str:
    """
    Injeta o payload no template.

    O escape de '<' e obrigatorio: sem ele, um valor de dado contendo
    '</script>' fecharia a tag e quebraria a pagina. Como os rotulos vem do
    arquivo do NABSA, sao dados nao confiaveis.
    """
    bruto = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    seguro = bruto.replace("<", "\\u003c").replace("\u2028", "\\u2028").replace(
        "\u2029", "\\u2029"
    )
    if "/*__PAYLOAD__*/" not in template:
        raise ValueError("Template sem o marcador /*__PAYLOAD__*/.")
    return template.replace("/*__PAYLOAD__*/", seguro)


def gerar_dashboard(
    caminho_sailed: Path,
    destino: Path,
    server: str | None = None,
    database: str | None = None,
    table_lineup: str | None = None,
    path_correcoes: Path | None = None,
) -> Path:
    """
    Gera o arquivo HTML e devolve o caminho escrito.

    O Line-Up e opcional de proposito: sem SQL alcancavel o dashboard ainda sai,
    so que com a pagina operacional vazia e um aviso. Uma falha de conexao nao
    pode custar a pagina historica inteira.
    """
    sailed = agregar_sailed(carregar_sailed(caminho_sailed, path_correcoes))

    lineup: dict = {"snapshot": None, "navios": [], "historico": [], "statuses": []}
    if server and database and table_lineup:
        try:
            lineup = agregar_lineup(carregar_lineup(server, database, table_lineup))
        except Exception as erro:  # noqa: BLE001 — dashboard nao derruba por isso
            logger.warning(f"  Dashboard: Line-Up indisponivel ({erro}). Pagina operacional vazia.")

    payload = {
        "geradoEm": datetime.datetime.now().isoformat(timespec="seconds"),
        "sailed": sailed,
        "lineup": lineup,
    }

    html = renderizar(payload, _TEMPLATE.read_text(encoding="utf-8"))
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(html, encoding="utf-8")

    tamanho = destino.stat().st_size / 1024
    logger.info(f"  Dashboard gerado: {destino} ({tamanho:.0f} KB)")
    return destino


def main() -> None:
    from argentina_etl import config

    # PATH_DATABASE, nao PATH_DATABASE_OUTPUT: a primeira e a base viva, relida
    # e reescrita a cada rodada. A segunda e so o radical dos snapshots datados
    # do archive (..._AT_2026-06-08_2347.xlsx); o arquivo sem timestamp nunca e
    # gravado, entao apontar para ela daria FileNotFoundError.
    gerar_dashboard(
        caminho_sailed=config.PATH_DATABASE,
        destino=config.PATH_DASHBOARD_OUTPUT,
        server=config.SQL_SERVER,
        database=config.SQL_DATABASE,
        table_lineup=config.SQL_TABLE_LINEUP,
        path_correcoes=config.PATH_CORRECOES,
    )


if __name__ == "__main__":
    main()
