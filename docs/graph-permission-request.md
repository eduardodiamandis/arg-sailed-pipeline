# Permission request — email draft

Ready to send. Replace the recipient name and sign-off.

---

**Subject:** Permission request — Microsoft Graph access for "ETL Argentina - Upload SharePoint"

---

Hi [name],

Hope you're doing well.

I'd like to ask for a permission grant on an application I registered in our tenant.

**What it's for:** a daily automated process publishes a spreadsheet to the
Research SharePoint library. Today it relies on the OneDrive desktop client to
sync the file, which gives the process no confirmation that the upload actually
happened — last week the file sat unsynced for over 9 hours, and people opening
it from SharePoint were reading outdated data without knowing. Publishing
directly through the Graph API fixes this, since it confirms delivery.

**What I need:**

1. Admin consent for **Microsoft Graph → `Sites.Selected`** (Application permission)
2. Write access granted on **one site only** (details below)

**Application:**

```
Name       : ETL Argentina - Upload SharePoint
Client ID  : cc2efd1d-20be-498a-a761-5d95b277951e
Tenant ID  : 54438057-1d4c-4652-ac18-d7ed3908dfa0
```

**Target:** `cgbent.sharepoint.com/sites/ZGC-PBIResearch` → Documents →
`Dataset Data Files/Trade Flow/ARG`

I went with `Sites.Selected` instead of `Sites.ReadWrite.All` so the app has no
access to anything else in the tenant. It grants nothing on its own, so a second
step is needed — granting write on this specific site:

```http
POST https://graph.microsoft.com/v1.0/sites/{site-id}/permissions

{
  "roles": ["write"],
  "grantedToIdentities": [
    { "application": {
        "id": "cc2efd1d-20be-498a-a761-5d95b277951e",
        "displayName": "ETL Argentina - Upload SharePoint" } }
  ]
}
```

The `site-id` comes from:
`GET https://graph.microsoft.com/v1.0/sites/cgbent.sharepoint.com:/sites/ZGC-PBIResearch`

I've already tested authentication and it works — the app gets a token without
issues, and calls return `401` only because no permission has been consented yet.
So this should be the last piece.

The access is limited to overwriting a single file, once a day, in that one
folder. No other sites, mailboxes or user data involved.

Happy to help with anything from my side, and glad to jump on a quick call if
that's easier.

Thanks a lot!

Best regards,
Eduardo
