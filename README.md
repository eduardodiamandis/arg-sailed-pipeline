# Argentina Sailed — Pipeline de Atualização

Pipeline automatizado para baixar, tratar e persistir dados de embarques de grãos da Argentina.

## Estrutura

```
argentina_sailed/
├── main.py                  # Orquestrador — ponto de entrada
├── src/
│   ├── config.py            # Carrega variáveis do .env
│   ├── logger_config.py     # Logger centralizado
│   ├── downloader.py        # Download dos arquivos via HTTP
│   ├── latest_file.py       # Encontra o arquivo mais recente no backup
│   └── database.py          # Transformação, merge e persistência
├── tests/
│   └── test_database.py     # Testes unitários da lógica de merge
├── .env.example             # Template de configuração (commitar)
├── .env                     # Configuração real (NÃO commitar)
├── .gitignore
└── requirements.txt
```

## Fluxo do Pipeline

```
Download Sailed ──┐
                  ├──► Arquivo mais recente
Download Line-Up  │
                  │
Banco existente ──┼──► merge_com_banco() ──► Salvar local
                                         ──► Salvar OneDrive
                                         ──► SQL Server
                                         ──► Pivot Tables (win32com)
```

## Lógica de Merge

O arquivo novo pode conter **um ou mais meses** (ex: jan + fev + mar parcial de 2026).

A função `merge_com_banco` identifica **todos os períodos mês/ano** presentes no arquivo novo e os remove do banco antes de inserir — garantindo que:

- Não há duplicatas mesmo que o pipeline rode múltiplas vezes no mesmo dia
- Meses históricos (ex: 2025 completo) nunca são apagados
- Casos especiais como inserção manual de múltiplos meses funcionam corretamente

## Configuração

```bash
# 1. Clone o repositório
git clone <url>
cd argentina_sailed

# 2. Instale as dependências
pip install -r requirements.txt

# 3. Configure o ambiente
copy .env.example .env
# Edite o .env com seus caminhos e credenciais

# 4. Execute
python main.py
```

## Testes

```bash
pytest tests/ -v
```

## Logs

Os logs são gravados em:
```
C:\Users\server\Desktop\Argentina\logs\argentina_updater.log
```
Rotação automática: 5 MB por arquivo, 3 backups mantidos.

## Saídas geradas

| Arquivo | Conteúdo |
|---|---|
| `Arg_sailed_database_AT.xlsx` | Banco local atualizado (sheet `data_base`) |
| `Arg_sailed_databease.xlsx` (OneDrive) | Banco + sheets 2025, 2026, Pivot_2025, Pivot_2026 |
| SQL Server `[dbo].[Arg_Sailed]` | Banco completo atualizado |
