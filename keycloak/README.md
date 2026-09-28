# Keycloak realm

`realm-vitalstream.json` defines the `vitalstream` realm and is imported when the Keycloak container
starts (`docker compose up`). Keycloak runs in dev mode without a volume, so this file is the source of
truth: recreate the container (`docker compose up -d --force-recreate keycloak`) to reset to it.
Admin console: http://localhost:8180 (admin / admin).

## Roles

| Role        | Meaning                              |
|-------------|--------------------------------------|
| `admin`     | Manage patients and devices          |
| `clinician` | View patients, devices and vitals    |
| `device`    | Send readings                        |

## Users (dev only, passwords = usernames)

| User    | Role        |
|---------|-------------|
| `alice` | `clinician` |
| `bob`   | `admin`     |

## Clients

- **`vitalstream-cli`**: a *public* client (no secret) with the password grant enabled, so you can get a
  user's token with curl. The password grant is deprecated (OAuth 2.1 removes it) because the app sees
  the user's password; a real web or mobile app would use the authorization code flow with PKCE, which
  sends the user to Keycloak's own login page.
- **`device-simulator`**: a *confidential* client (it has a secret) using the client credentials grant:
  a machine logs in as itself, no user involved. Its service account has the `device` role.
  The secret `device-simulator-secret` is only acceptable because this is a local dev setup.

Both clients add `vitalstream-api` to the token's audience (`aud` claim), which the services check, so a
token issued for some other application in the realm isn't accepted by them.

Access tokens live 5 minutes (`accessTokenLifespan`); clients fetch a new one when theirs expires.
