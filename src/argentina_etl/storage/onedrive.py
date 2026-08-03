"""
storage/onedrive.py
-------------------
Escrita do arquivo consumido pelo Power BI, com as sheets derivadas, e o
empurrao no cliente do OneDrive para que a sincronizacao ocorra na hora.
"""
from __future__ import annotations

import datetime
import os
from pathlib import Path

import pandas as pd

from argentina_etl.logging_setup import logger

# ---------------------------------------------------------------------------
# Pivots
# ---------------------------------------------------------------------------
# Filtros de negocio das pivots. Ficam explicitos aqui, no codigo, em vez de
# escondidos como "page fields" de um objeto PivotTable dentro do .xlsx.
#
# Ate 2026-07-28 estas sheets eram geradas duas vezes: primeiro aqui (sem
# filtro nenhum) e depois sobrescritas por uma automacao COM do Excel, que
# aplicava os filtros abaixo. A rota COM foi removida — ela dependia de o Excel
# estar instalado, deixava processos EXCEL.EXE orfaos segurando o arquivo,
# travava no timeout de 120s e, o pior, era o Excel quem resolvia o caminho
# local para a URL do SharePoint, construindo a pivot sobre a copia do servidor
# em vez do arquivo recem-gravado.
PIVOT_ORIGIN = "ARGENTINA"
PIVOT_CARGO = "CORN"

# Quanto esperar a sincronizacao confirmar antes de avisar. Zero desliga a
# verificacao. Configuravel por SYNC_ESPERA_SEGUNDOS no .env.
SYNC_ESPERA_SEGUNDOS = int(os.getenv("SYNC_ESPERA_SEGUNDOS", "60"))

# Mes fixo da pivot do ano anterior; o ano corrente usa o mes de hoje.
PIVOT_MES_ANO_ANTERIOR = 12


def montar_pivot(
    df: pd.DataFrame,
    *,
    year: int,
    month: int,
    origin: str = PIVOT_ORIGIN,
    cargo: str = PIVOT_CARGO,
) -> pd.DataFrame:
    """
    Soma de Tons por Destination, com os quatro filtros aplicados.

    Devolve um DataFrame de duas colunas sem cabecalho, reproduzindo o layout
    que a versao COM gerava, para nao quebrar quem ja consome as sheets:

        Month        7
        Cargo        CORN
        Origin       ARGENTINA
        Year         2026
        (vazio)
        Row Labels   Sum of Tons
        ALGERIA      203118.47
        ...
        Grand Total  3906772.43
    """
    filtrado = df[
        (df["Year"] == year)
        & (df["Month"] == month)
        & (df["Origin"].astype(str).str.strip().str.upper() == origin.upper())
        & (df["Cargo"].astype(str).str.strip().str.upper() == cargo.upper())
    ]

    somas = (
        filtrado.groupby("Destination", dropna=False)["Tons"]
        .sum()
        .sort_index()
    )

    linhas: list[tuple] = [
        ("Month", month),
        ("Cargo", cargo),
        ("Origin", origin),
        ("Year", year),
        (None, None),
        ("Row Labels", "Sum of Tons"),
    ]
    linhas += [(str(dest), float(tons)) for dest, tons in somas.items()]
    linhas.append(("Grand Total", float(somas.sum())))

    logger.info(
        f"  Pivot {year}/{month:02d} ({origin}, {cargo}): "
        f"{len(somas)} destino(s), {somas.sum():,.2f} tons"
    )
    return pd.DataFrame(linhas)


def salvar_onedrive(df: pd.DataFrame, path: Path) -> None:
    """
    Salva o arquivo no OneDrive com sheets extras:
      - data_base  : banco completo
      - 2025 / 2026: recorte por ano
      - Pivot_2025 : Tons por Destination — dezembro/2025, ARGENTINA, CORN
      - Pivot_2026 : Tons por Destination — mes corrente/2026, ARGENTINA, CORN
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    df_2025 = df[df["Year"] == 2025].copy()
    df_2026 = df[df["Year"] == 2026].copy()

    pivot_2025 = montar_pivot(df, year=2025, month=PIVOT_MES_ANO_ANTERIOR)
    pivot_2026 = montar_pivot(df, year=2026, month=datetime.date.today().month)

    with pd.ExcelWriter(path, engine="openpyxl", mode="w") as writer:
        df.to_excel(writer, sheet_name="data_base", index=False)
        df_2025.to_excel(writer, sheet_name="2025", index=False)
        df_2026.to_excel(writer, sheet_name="2026", index=False)
        pivot_2025.to_excel(writer, sheet_name="Pivot_2025", index=False, header=False)
        pivot_2026.to_excel(writer, sheet_name="Pivot_2026", index=False, header=False)

    logger.info(f"Arquivo OneDrive salvo com sheets extras: {path}")
    _forcar_sync_onedrive(path)
    verificar_sincronizacao(path, espera_segundos=SYNC_ESPERA_SEGUNDOS)


# O OneDrive pode estar instalado por usuario ou por maquina. Ate 2026-07-29 o
# codigo olhava so o primeiro caso, entao nesta maquina — onde a instalacao e por
# maquina — ele nunca encontrava o executavel e registrava
# "OneDrive.exe nao encontrado - sync automatico indisponivel" em TODA execucao,
# desde sempre. Falso alarme: o cliente estava rodando o tempo inteiro.
_ONEDRIVE_CANDIDATOS = (
    ("LOCALAPPDATA", r"Microsoft\OneDrive\OneDrive.exe"),        # por usuario
    ("PROGRAMFILES", r"Microsoft OneDrive\OneDrive.exe"),        # por maquina
    ("PROGRAMFILES(X86)", r"Microsoft OneDrive\OneDrive.exe"),   # por maquina, 32 bits
)


def _localizar_onedrive() -> Path | None:
    """Procura o OneDrive.exe nos locais de instalação conhecidos."""
    import os

    for variavel, relativo in _ONEDRIVE_CANDIDATOS:
        raiz = os.environ.get(variavel)
        if not raiz:
            continue
        candidato = Path(raiz) / relativo
        if candidato.exists():
            return candidato
    return None


def _onedrive_em_execucao() -> bool:
    """True se há um processo OneDrive.exe ativo."""
    import subprocess

    try:
        saida = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq OneDrive.exe", "/NH"],
            capture_output=True, text=True, timeout=15, check=False,
        )
        return "OneDrive.exe" in saida.stdout
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Verificação de que o arquivo chegou ao servidor
# ---------------------------------------------------------------------------
# O pipeline grava a cópia local; quem publica no SharePoint é o cliente do
# OneDrive. Quando ele falha, o arquivo local fica certo e o do servidor, velho —
# e ninguém percebe. Foi o que aconteceu em 2026-07-28: o arquivo ficou
# "Sincronização pendente" por mais de 9 horas enquanto todo o resto da
# biblioteca sincronizava normalmente.
#
# O Windows expõe o status por arquivo numa coluna do shell. O índice varia por
# versão e idioma, então procuramos pelo nome em vez de fixar o número.
_NOMES_COLUNA_STATUS = ("status de disponibilidade", "availability status")

# Textos que indicam que ainda não subiu (o shell é localizado)
_TEXTOS_PENDENTE = ("pendente", "pending", "sincronizando", "syncing")

_coluna_status_cache: int | None = None


def _indice_coluna_status(folder) -> int | None:
    """Descobre o índice da coluna de status pelo nome, uma vez por processo."""
    global _coluna_status_cache
    if _coluna_status_cache is not None:
        return _coluna_status_cache
    for i in range(400):
        try:
            nome = (folder.GetDetailsOf(None, i) or "").strip().lower()
        except Exception:
            continue
        if nome in _NOMES_COLUNA_STATUS:
            _coluna_status_cache = i
            return i
    return None


def status_sincronizacao(path: Path) -> str | None:
    """
    Status de sincronização do arquivo, como o Explorer mostra.

    Retorna None quando não foi possível determinar — nunca levanta: é
    diagnóstico, não pode derrubar o pipeline.
    """
    try:
        import win32com.client as win32

        shell = win32.Dispatch("Shell.Application")
        folder = shell.Namespace(str(path.parent))
        if folder is None:
            return None
        indice = _indice_coluna_status(folder)
        if indice is None:
            return None
        item = folder.ParseName(path.name)
        if item is None:
            return None
        return (folder.GetDetailsOf(item, indice) or "").strip() or None
    except Exception as exc:
        logger.debug(f"Não foi possível ler o status de sincronização: {exc}")
        return None


def verificar_sincronizacao(path: Path, espera_segundos: int = 60) -> bool:
    """
    Aguarda o arquivo sair do estado pendente e avisa se não sair.

    O aviso sai como WARNING, e o _extract_warnings do report.py o recolhe do
    log — então aparece na seção "Avisos" do e-mail sem acoplamento extra.

    Returns
    -------
    True se sincronizou (ou se o status não pôde ser lido, para não gerar
    alarme falso); False se continuou pendente até o fim da espera.
    """
    import time

    inicio = time.time()
    status = status_sincronizacao(path)

    if status is None:
        logger.info("Status de sincronização indisponível — seguindo sem verificar.")
        return True

    while any(t in status.lower() for t in _TEXTOS_PENDENTE):
        if time.time() - inicio >= espera_segundos:
            logger.warning(
                f"⚠️ ARQUIVO NÃO SINCRONIZADO: '{path.name}' continua em "
                f"'{status}' após {espera_segundos}s. A cópia local está correta, "
                f"mas quem abrir pelo SharePoint verá dados antigos. "
                f"Verifique o cliente do OneDrive."
            )
            return False
        time.sleep(5)
        status = status_sincronizacao(path) or status

    decorrido = time.time() - inicio
    logger.info(f"Sincronização confirmada em {decorrido:.0f}s — status: {status}")
    return True


def _forcar_sync_onedrive(path: Path) -> None:
    """
    Garante que o OneDrive perceba o arquivo logo após a gravação.

    O arquivo consumido pelo Power BI vive numa biblioteca do SharePoint
    sincronizada pelo cliente. O pipeline grava a cópia local; quem publica no
    servidor é o OneDrive. Se ele estiver parado, o arquivo local fica correto e
    o do servidor, velho — sem nenhum sinal para quem abrir pela web.
    """
    import os, subprocess, sys

    # Atualiza o timestamp para o OneDrive detectar a mudança
    os.utime(path, None)

    if sys.platform != "win32":
        return

    if _onedrive_em_execucao():
        logger.info("OneDrive: cliente em execução — sincronização a cargo dele.")
        return

    # Nao esta rodando: o arquivo NAO vai chegar ao servidor sozinho.
    exe = _localizar_onedrive()
    if exe is None:
        logger.warning(
            "OneDrive não está em execução e o executável não foi encontrado — "
            "o arquivo permanecerá apenas local, e quem abrir pela web verá dados antigos."
        )
        return

    try:
        subprocess.Popen(
            [str(exe), "/background"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        logger.warning(f"OneDrive não estava em execução — iniciado a partir de {exe}.")
    except Exception as e:
        logger.error(f"Não foi possível iniciar o OneDrive: {e}")


