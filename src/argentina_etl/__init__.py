"""
argentina_etl
-------------
Pipeline de coleta e consolidacao de embarques de navios na Argentina,
a partir dos boletins do NABSA.

Estrutura (ver ESTRUTURA.md):
  pipelines/  regra de negocio — decide o que os dados devem ser
  storage/    persistencia — escreve o que recebeu, sem opinar
  reporting/  relatorio por e-mail
  utils/      utilitarios sem dependencia de dominio
"""
