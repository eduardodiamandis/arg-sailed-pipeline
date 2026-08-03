"""
pipelines/correcoes.py
----------------------
Correcoes conhecidas do Sailed, reaplicadas a cada rodada.

**Por que isto e um passo do pipeline e nao um conserto manual.** A base e
reescrita a cada rodada, o Arg_Sailed leva DELETE + INSERT total e a planilha do
OneDrive e regerada — as tres a partir do mesmo `db_atualizado`. Uma correcao
feita a mao em qualquer uma dessas pontas e desfeita na madrugada seguinte. Pior:
se o NABSA republicar um mes que ja foi corrigido, a substituicao em bloco do
merge sobrescreve ate a base. Uma correcao so e permanente se for reaplicada
toda vez, e e o que este modulo faz.

**O valor errado faz parte da chave de casamento.** Uma regra so age quando data,
navio, carga, destino e o valor errado batem todos. Assim, no dia em que a origem
consertar o dado, a regra deixa de casar sozinha e nao faz nada — em vez de
sobrescrever o valor correto com o nosso palpite. Toda regra que nao casou vira
WARNING, porque uma regra obsoleta e informacao, nao ruido: ou a origem corrigiu,
ou a linha mudou de forma e a correcao precisa ser revista.

Nao escreve em lugar nenhum — persistencia mora em storage/.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from argentina_etl.logging_setup import logger

COLUNAS_CORRECAO = [
    "data",
    "navio",
    "carga",
    "destino",
    "tons_errado",
    "acao",
    "tons_correto",
    "motivo",
    "registrado_em",
]

ACOES_VALIDAS = ("corrigir", "remover")

# Uma correcao tem duas vidas, e confundi-las produz alarme falso todo dia.
#
#   aplicar — a origem ainda publica o valor errado. Espera-se que case; nao
#             casar e digno de aviso, porque ou a origem consertou (e a regra
#             pode ser aposentada) ou a chave esta errada.
#
#   guarda  — o dado ja foi consertado na base. A regra fica de sentinela para
#             o caso de o NABSA republicar o mes e a substituicao em bloco
#             trazer o erro de volta. Aqui o silencio e o estado esperado: nao
#             casar nao diz nada. Casar, sim — significa regressao, e sai como
#             WARNING.
#
# Sem essa distincao, uma correcao ja resolvida gritaria toda noite para sempre,
# e num projeto onde os avisos vao para o e-mail isso treina quem le a ignora-los.
MODOS_VALIDOS = ("aplicar", "guarda")
MODO_PADRAO = "aplicar"

# Tolerancia ao comparar a tonelagem errada. O valor faz o trajeto
# Excel -> pandas -> CSV e volta como float; exigir igualdade binaria faria uma
# regra legitima falhar por um bit de arredondamento.
TOLERANCIA_TONS = 0.01


def carregar_correcoes(path: Path) -> pd.DataFrame:
    """
    Le o arquivo de correcoes.

    Arquivo ausente devolve tabela vazia em vez de levantar: nao ter correcao
    nenhuma e o estado normal e saudavel deste projeto.
    """
    if not path.exists():
        logger.info(f"  Correcoes: nenhum arquivo em {path} — nada a aplicar.")
        return pd.DataFrame(columns=COLUNAS_CORRECAO)

    df = pd.read_csv(path, comment="#", dtype=str).fillna("")
    faltando = [c for c in COLUNAS_CORRECAO if c not in df.columns]
    if faltando:
        raise ValueError(f"Arquivo de correcoes sem as colunas: {faltando}")

    df["acao"] = df["acao"].str.strip().str.lower()
    invalidas = df[~df["acao"].isin(ACOES_VALIDAS)]
    if not invalidas.empty:
        raise ValueError(
            f"Acao invalida no arquivo de correcoes: {sorted(invalidas['acao'].unique())}. "
            f"Use uma de {ACOES_VALIDAS}."
        )

    # 'modo' e opcional: um arquivo escrito antes da coluna existir continua
    # valendo, com o comportamento de sempre.
    if "modo" not in df.columns:
        df["modo"] = MODO_PADRAO
    df["modo"] = df["modo"].str.strip().str.lower().replace("", MODO_PADRAO)
    modos_ruins = df[~df["modo"].isin(MODOS_VALIDOS)]
    if not modos_ruins.empty:
        raise ValueError(
            f"Modo invalido no arquivo de correcoes: {sorted(modos_ruins['modo'].unique())}. "
            f"Use uma de {MODOS_VALIDOS}."
        )

    sem_valor = df[(df["acao"] == "corrigir") & (df["tons_correto"].str.strip() == "")]
    if not sem_valor.empty:
        raise ValueError(
            f"{len(sem_valor)} correcao(oes) com acao=corrigir e tons_correto vazio."
        )

    return df


def _casa(db: pd.DataFrame, regra: pd.Series) -> pd.Series:
    """Mascara das linhas que a regra atinge."""
    data = pd.to_datetime(regra["data"]).normalize()
    tons_errado = float(regra["tons_errado"])

    def _txt(coluna: str) -> pd.Series:
        return db[coluna].astype(str).str.strip().str.upper()

    return (
        (pd.to_datetime(db["Date"], errors="coerce").dt.normalize() == data)
        & (_txt("Vessel") == str(regra["navio"]).strip().upper())
        & (_txt("Cargo") == str(regra["carga"]).strip().upper())
        & (_txt("Destination") == str(regra["destino"]).strip().upper())
        & ((db["Tons"].astype(float) - tons_errado).abs() <= TOLERANCIA_TONS)
    )


def aplicar_correcoes(db: pd.DataFrame, correcoes: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica as correcoes e devolve a base corrigida.

    Nunca levanta por causa de uma regra que nao casou — isso e aviso, nao erro.
    O pipeline nao pode cair porque uma linha antiga mudou de forma.
    """
    if correcoes.empty:
        return db

    if "Vessel" not in db.columns:
        # A base do Excel tem Vessel; o Arg_Sailed do SQL nao. Se um dia este
        # passo for chamado sobre o recorte do SQL, o casamento seria por
        # data+carga+destino e poderia pegar a linha errada. Melhor nao agir.
        logger.warning(
            "  Correcoes: base sem a coluna 'Vessel' — nenhuma correcao aplicada "
            "(o casamento depende do navio para nao atingir a linha errada)."
        )
        return db

    db = db.copy()
    remover = pd.Series(False, index=db.index)
    aplicadas = 0

    for _, regra in correcoes.iterrows():
        try:
            alvo = _casa(db, regra)
        except Exception as erro:  # noqa: BLE001 — regra malformada nao derruba a rodada
            logger.warning(f"  Correcao ignorada ({regra.get('navio')}): {erro}")
            continue

        atingidas = int(alvo.sum())
        rotulo = f"{regra['navio']} em {regra['data']}"
        modo = regra.get("modo", MODO_PADRAO) or MODO_PADRAO

        if atingidas == 0:
            if modo == "guarda":
                # Estado esperado: o dado ja esta certo e a sentinela nao teve
                # o que fazer. Silencio, para nao virar alarme diario.
                logger.debug(f"  Guarda sem regressao: {rotulo}")
            else:
                logger.warning(
                    f"  ⚠️ Correcao sem efeito: {rotulo} nao encontrado com "
                    f"Tons={regra['tons_errado']}. A origem pode ter corrigido o dado — "
                    f"se for o caso, mude a regra para modo=guarda em "
                    f"config/correcoes_sailed.csv."
                )
            continue

        if modo == "guarda":
            # A sentinela disparou: o valor errado voltou. Vale WARNING, porque
            # significa que a origem republicou o mes ou alguem restaurou um
            # snapshot antigo.
            logger.warning(
                f"  ⚠️ Regressao detectada e corrigida: {rotulo} voltou com "
                f"Tons={regra['tons_errado']}. A origem republicou o dado errado."
            )

        if regra["acao"] == "remover":
            remover |= alvo
            logger.info(f"  Correcao: {atingidas} linha(s) removida(s) — {rotulo}")
        else:
            novo = float(regra["tons_correto"])
            db.loc[alvo, "Tons"] = novo
            logger.info(
                f"  Correcao: {rotulo} — Tons {regra['tons_errado']} -> {novo:,.2f} "
                f"({atingidas} linha(s))"
            )
        aplicadas += atingidas

    if remover.any():
        db = db[~remover]

    if aplicadas:
        logger.info(f"  Correcoes aplicadas: {aplicadas} linha(s) no total.")
    return db.reset_index(drop=True)
