# Permission request — Microsoft Graph API

Text for opening a ticket with IT / Microsoft 365 administration.

---

## Subject

Grant `Sites.Selected` permission to the application **ETL Argentina - Upload SharePoint**

## Background

An automated process consolidates daily vessel shipment data for Argentina (NABSA
bulletins) and publishes the result to a spreadsheet used by the Research team.

Today this publication relies on the OneDrive desktop client to sync a local
folder. That mechanism provides no delivery confirmation to the process: on
2026-07-28 the file remained in "Sync pending" state for more than 9 hours — the
local copy correct, the server copy outdated — with no warning of any kind.
During that period, anyone opening the file from SharePoint was reading stale
data without knowing it.

The fix is to publish the file directly through the Microsoft Graph API, which
returns confirmation that the file was written to the server and allows the
process to detect and report failures.

## What is being requested

Permission for an App Registration **already created** in the tenant:

| Item | Value |
|---|---|
| Application name | `ETL Argentina - Upload SharePoint` |
| Application (client) ID | `cc2efd1d-20be-498a-a761-5d95b277951e` |
| Object ID | `9b9e8adc-602f-4426-b9c4-9ceeeb3673de` |
| Directory (tenant) ID | `54438057-1d4c-4652-ac18-d7ed3908dfa0` |
| Supported account types | Single tenant — CGB Enterprises, Inc. |

### Permission needed

| API | Permission | Type |
|---|---|---|
| Microsoft Graph | `Sites.Selected` | **Application** (not Delegated) |

**Why `Sites.Selected` rather than `Sites.ReadWrite.All`:** `Sites.Selected`
grants no access to any site on its own. It only makes it possible to grant
access to specific sites, individually. This follows least privilege and avoids
giving the application write access across every site in the tenant.

### Two actions are required

**1. Admin consent** for the `Sites.Selected` application permission on the app
registration.

**2. Grant write access to this single site**, through a Graph call (requires
`Sites.FullControl.All`):

```http
POST https://graph.microsoft.com/v1.0/sites/{site-id}/permissions
Content-Type: application/json

{
  "roles": ["write"],
  "grantedToIdentities": [
    {
      "application": {
        "id": "cc2efd1d-20be-498a-a761-5d95b277951e",
        "displayName": "ETL Argentina - Upload SharePoint"
      }
    }
  ]
}
```

> This second step is the one commonly missed. Without it the application holds
> the permission but still has access to no site at all, and calls return
> `403 accessDenied`.

The `site-id` for the call above can be obtained with:

```http
GET https://graph.microsoft.com/v1.0/sites/cgbent.sharepoint.com:/sites/ZGC-PBIResearch
```

## Target site and folder

| | |
|---|---|
| Host | `cgbent.sharepoint.com` |
| Site | `/sites/ZGC-PBIResearch` |
| Library | Documents (*Shared Documents*) |
| Folder | `Dataset Data Files/Trade Flow/ARG` |
| File | `Arg_sailed_databease.xlsx` (~5 MB, overwritten once per day) |

## Current status — already verified

Authentication was tested on 2026-07-29 and **works**: the application
successfully obtains an access token using the client credentials flow.

Graph calls currently return **HTTP 401**, because no application permission has
been consented yet. This confirms the app registration itself is correct and that
the only missing piece is the permission described above.

## Scope of access requested

- **Write access to this document library only**, to overwrite one file, once per day
- No access to any other site in the tenant
- No access to mailboxes, calendars, users or any other resource
- No user interaction: the process authenticates as an application, with no user
  session and no MFA

## Note on the client secret

The associated client secret expires on **2028-07-28**. It is worth recording this
date, as publication will start failing when it expires.
