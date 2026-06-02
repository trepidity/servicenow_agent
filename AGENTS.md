# Agent Rules

## Public-Safe Content

This repository must stay public-safe. Do not add organization-specific names,
vault paths, 1Password item names, hostnames, tenant identifiers, internal URLs,
usernames, record IDs, credential labels, environment values, or other
deployment-specific details to committed code, docs, tests, fixtures, snapshots,
or generated artifacts.

Use generic placeholders such as `example.service-now.com`, `op://vault/item`,
`shared-vault`, `service-account-item`, and `user@example.com`. Keep real values
only in ignored local files or the operator's external secret manager.
