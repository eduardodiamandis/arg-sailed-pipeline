# Solicitação de permissão — Microsoft Graph API

Texto para abertura de chamado junto à equipe de TI / administração do Microsoft 365.

---

## Assunto

Concessão de permissão `Sites.Selected` para o aplicativo **ETL Argentina - Upload SharePoint**

## Contexto

Existe um processo automatizado que consolida diariamente os dados de embarques
de navios na Argentina (boletins NABSA) e publica o resultado em uma planilha
consumida pela área de Research.

Hoje essa publicação depende do cliente do OneDrive sincronizar uma pasta local.
Esse mecanismo não oferece confirmação de entrega ao processo: em 28/07/2026 o
arquivo permaneceu no estado "Sincronização pendente" por mais de 9 horas — a
cópia local correta e a do servidor desatualizada — sem qualquer sinalização.
Durante esse período, quem abriu o arquivo pelo SharePoint consultou dados
antigos sem saber.

A correção é publicar o arquivo diretamente via Microsoft Graph API, que retorna
confirmação de gravação no servidor e permite ao processo detectar e reportar
falhas.

## O que é solicitado

Concessão de permissão para um App Registration **já criado** no tenant:

| Item | Valor |
|---|---|
| Nome do aplicativo | `ETL Argentina - Upload SharePoint` |
| Tipo de conta | Single tenant — CGB Enterprises, Inc. |
| Application (client) ID | `<preencher com o ID da aba Overview>` |

### Permissão necessária

| API | Permissão | Tipo |
|---|---|---|
| Microsoft Graph | `Sites.Selected` | **Aplicativo** (não Delegada) |

**Por que `Sites.Selected` e não `Sites.ReadWrite.All`:** `Sites.Selected` não
concede acesso a nenhum site por si só. Ela apenas habilita que se conceda acesso
a sites específicos, individualmente. É o princípio de menor privilégio, e evita
dar ao aplicativo escrita sobre todos os sites do tenant.

### Duas ações são necessárias

**1. Consentimento do administrador** para a permissão `Sites.Selected` no
registro do aplicativo.

**2. Concessão de escrita apenas neste site**, via chamada ao Graph (requer
`Sites.FullControl.All`):

```http
POST https://graph.microsoft.com/v1.0/sites/{site-id}/permissions
Content-Type: application/json

{
  "roles": ["write"],
  "grantedToIdentities": [
    {
      "application": {
        "id": "<application-client-id>",
        "displayName": "ETL Argentina - Upload SharePoint"
      }
    }
  ]
}
```

> Este segundo passo é o que costuma ser esquecido. Sem ele, o aplicativo recebe
> a permissão mas continua sem acesso a nenhum site, e as chamadas retornam
> `403 accessDenied`.

## Site e pasta de destino

| | |
|---|---|
| Host | `cgbent.sharepoint.com` |
| Site | `/sites/ZGC-PBIResearch` |
| Biblioteca | Documents (*Shared Documents*) |
| Pasta | `Dataset Data Files/Trade Flow/ARG` |
| Arquivo | `Arg_sailed_databease.xlsx` (~5 MB, sobrescrito uma vez por dia) |

O `site-id` necessário à chamada acima pode ser obtido com:

```http
GET https://graph.microsoft.com/v1.0/sites/cgbent.sharepoint.com:/sites/ZGC-PBIResearch
```

## Escopo de acesso solicitado

- **Escrita apenas nesta biblioteca**, para sobrescrever um arquivo, uma vez por dia
- Sem acesso a outros sites do tenant
- Sem leitura de caixas de correio, calendários, usuários ou qualquer outro recurso
- Sem interação com usuários: o processo autentica como aplicativo, sem sessão de
  usuário e sem MFA

## Observação sobre o segredo

O client secret associado expira em **28/07/2028**. Convém registrar essa data,
pois na expiração a publicação passa a falhar.
