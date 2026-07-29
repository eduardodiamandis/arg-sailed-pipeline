"""
pipelines/sailed.py
-------------------
Regra de negocio do fluxo Sailed: leitura do arquivo bruto do NABSA e merge
periodo a periodo com a base.

Nao escreve em lugar nenhum — persistencia mora em storage/.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from argentina_etl.logging_setup import logger

COLUNAS = ["Date", "Destination", "Origin", "Cargo", "Tons", "Month", "Year"]

# ---------------------------------------------------------------------------
# Limpeza do arquivo bruto
# ---------------------------------------------------------------------------

def remover_colunas_sem_nome(df: pd.DataFrame) -> pd.DataFrame:
    """
    Descarta as colunas 'Unnamed: N' que o Excel arrasta para dentro da base.

    A base tinha seis delas (13 a 18), reescritas a cada execucao porque o
    merge as carregava adiante. Nao pertencem ao esquema: o Arg_Sailed do SQL
    tem sete colunas e o pandas as inventa ao ler celulas vazias a direita dos
    dados.

    Nao remove em silencio. Se uma coluna sem nome tiver conteudo, o conteudo
    vai para o log como WARNING antes de a coluna sair — foi assim que se
    descobriu a anotacao 'ultima linha do banco do almyr' escondida na
    'Unnamed: 18', na linha do PRABHU PUNI de 14/01/2020 (registrada na secao
    10 do ESTRUTURA.md). Perder um dado por limpeza automatica seria
    exatamente o tipo de falha silenciosa que este projeto ja pagou caro.
    """
    sem_nome = [c for c in df.columns if str(c).startswith("Unnamed:")]
    if not sem_nome:
        return df

    for coluna in sem_nome:
        preenchidas = df[coluna].notna()
        if preenchidas.any():
            valores = df.loc[preenchidas, coluna].astype(str).tolist()
            logger.warning(
                f"⚠️ Coluna sem nome '{coluna}' descartada, mas tinha "
                f"{len(valores)} valor(es): {valores[:5]}"
            )

    logger.info(f"Colunas sem nome removidas da base: {sem_nome}")
    return df.drop(columns=sem_nome)


def _cortar_apos_duas_linhas_vazias(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove todas as linhas a partir de duas linhas consecutivas completamente vazias.
    Isso elimina rodapés e notas de rodapé presentes nos arquivos originais.
    """
    empty = df.isna().all(axis=1)
    for i in range(len(empty) - 1):
        if empty.iloc[i] and empty.iloc[i + 1]:
            logger.info(f"Rodapé detectado na linha {i} — descartando o restante.")
            return df.iloc[:i].copy()
    return df


def ler_arquivo_novo(path: Path) -> pd.DataFrame:
    """
    Lê o arquivo Excel baixado, remove o rodapé e garante os tipos corretos.

    O arquivo original tem 7 linhas de cabeçalho antes dos dados,
    por isso usamos header=7.
    """
    logger.info(f"Lendo arquivo novo: {path.name}")
    df = pd.read_excel(path, header=7, engine="openpyxl")
    df = _cortar_apos_duas_linhas_vazias(df)

    df["Date"] = pd.to_datetime(df["Date"])
    df["Month"] = df["Date"].dt.month
    df["Year"] = df["Date"].dt.year

    logger.info(f"  {len(df)} linhas carregadas | "
                f"períodos: {sorted(df['Date'].dt.to_period('M').unique().astype(str))}")
    return df


# ---------------------------------------------------------------------------
# Merge com o banco
# ---------------------------------------------------------------------------

def merge_com_banco(df_novo: pd.DataFrame, db: pd.DataFrame) -> pd.DataFrame:
    """
    Atualiza o banco de forma inteligente, período a período.

    Para CADA período (mês/ano) presente no arquivo novo, compara a quantidade
    de registros com o que já existe no banco:
      - Se o arquivo novo tem MAIS ou IGUAL → substitui (dados mais atualizados)
      - Se o arquivo novo tem MENOS → mantém o banco e loga um alerta

    Isso evita perda de dados quando o pipeline roda com um arquivo parcial
    (ex: arquivo do dia 29 substituindo um março completo que já estava no banco).

    Trava de segurança obrigatória desde que a base voltou a ser reescrita a cada
    execução: sem ela, um arquivo truncado do NABSA corromperia o mês inteiro de
    forma permanente. Ver ESTRUTURA.md, decisão 9.1.
    """
    df_novo["Date"] = pd.to_datetime(df_novo["Date"])
    db["Date"] = pd.to_datetime(db["Date"])

    periodos_novos = df_novo["Date"].dt.to_period("M").unique()
    logger.info(f"Períodos do arquivo novo: {sorted(periodos_novos.astype(str))}")

    periodos_aceitos = []
    periodos_rejeitados = []

    for periodo in sorted(periodos_novos):
        mes, ano = periodo.month, periodo.year

        mask_novo = (df_novo["Date"].dt.month == mes) & (df_novo["Date"].dt.year == ano)
        linhas_novo = mask_novo.sum()
        dias_novo = df_novo.loc[mask_novo, "Date"].dt.day.nunique()

        mask_db = (db["Date"].dt.month == mes) & (db["Date"].dt.year == ano)
        linhas_db = mask_db.sum()
        dias_db = db.loc[mask_db, "Date"].dt.day.nunique()

        if linhas_db == 0 or linhas_novo >= linhas_db:
            periodos_aceitos.append(periodo)
            logger.info(
                f"  {periodo}: ACEITO — novo={linhas_novo} linhas/{dias_novo} dias "
                f"vs banco={linhas_db} linhas/{dias_db} dias"
            )
        else:
            periodos_rejeitados.append(periodo)
            logger.warning(
                f"  ⚠️ {periodo}: REJEITADO — novo={linhas_novo} linhas/{dias_novo} dias "
                f"vs banco={linhas_db} linhas/{dias_db} dias "
                f"→ mantendo dados do banco para não perder informação"
            )

    if periodos_rejeitados:
        logger.warning(
            f"⚠️ {len(periodos_rejeitados)} período(s) rejeitado(s) por ter menos dados: "
            f"{[str(p) for p in periodos_rejeitados]}"
        )

    # Remove do banco apenas os períodos aceitos
    if periodos_aceitos:
        mascara_remover = db["Date"].dt.to_period("M").isin(periodos_aceitos)
        linhas_removidas = mascara_remover.sum()
        db_limpo = db[~mascara_remover].copy()
        logger.info(f"Linhas removidas do banco (períodos aceitos): {linhas_removidas}")

        mask_aceitos = df_novo["Date"].dt.to_period("M").isin(periodos_aceitos)
        df_inserir = df_novo[mask_aceitos].copy()
        db_atualizado = pd.concat([db_limpo, df_inserir], ignore_index=True)
    else:
        # Nada a inserir. Não concatenar: um DataFrame vazio tem colunas de dtype
        # object e o concat converteria 'Date' para object, quebrando o .dt abaixo.
        db_atualizado = db.copy()
        logger.warning("Nenhum período aceito — banco mantido sem alterações.")

    db_atualizado = db_atualizado.sort_values("Date").reset_index(drop=True)

    # Garante Month e Year consistentes
    db_atualizado["Month"] = db_atualizado["Date"].dt.month
    db_atualizado["Year"] = db_atualizado["Date"].dt.year

    linhas_adicionadas = len(db_atualizado) - len(db)
    logger.info(f"Linhas líquidas adicionadas ao banco: {linhas_adicionadas}")
    logger.info(f"Total de linhas no banco atualizado: {len(db_atualizado)}")

    return db_atualizado


