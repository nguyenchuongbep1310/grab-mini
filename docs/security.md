# Security notes

## Token signing and verification (asymmetric)

Tokens are JWTs signed with **Ed25519** (`alg: EdDSA`) and carry a `kid` header. Only the issuer holds the private key;
every other service verifies with public keys, so a compromised service can read tokens but cannot mint one.

| | Issuer (`auth-service`, or a real identity provider) | Every verifying service |
|---|---|---|
| Needs | `JWT_PRIVATE_KEY` or `JWT_PRIVATE_KEY_FILE`: a PKCS#8 PEM Ed25519 key | `JWT_JWKS_URL` **or** `JWT_PUBLIC_KEY` / `JWT_PUBLIC_KEY_FILE` (a PEM bundle of public keys) |
| Publishes | `GET /.well-known/jwks.json` (`Cache-Control: public, max-age=300`) | nothing |
| Optional | `JWT_PREVIOUS_PUBLIC_KEYS[_FILE]`: retired public keys to keep publishing during a rotation | |

```bash
openssl genpkey -algorithm ed25519 -out jwt-private.pem          # the issuer's secret
openssl pkey -in jwt-private.pem -pubout -out jwt-public.pem     # safe to distribute
```

Development: `APP_ENV=development` makes the issuer sign with, and the services trust, a key that is published in
`services/pkg/auth/keys.go`. **That key is only accepted with `APP_ENV=development`** (unlike the older "anything but
production" defaults): a deployment that forgot `APP_ENV`, or runs with `staging`, fails to start with an explicit
error instead of trusting a public key. `scripts/dev.sh`, `deploy/loadtest/stack.sh` and the e2e test set it for you.

### What a verifier does

- Allows exactly one algorithm, `EdDSA`. `none`, every HMAC (`HS256/384/512`), RSA and ECDSA tokens are rejected before
  any key is looked up, so algorithm-confusion (an HMAC token "signed" with the public key) cannot work. The key
  function also checks that the key is an Ed25519 key.
- Requires a `kid`, and looks the key up by it. Issuer, audience and expiry are checked as before.
- With `JWT_JWKS_URL`: fetches lazily on first use (a service can start before the issuer), caches for 10 minutes,
  refreshes on an **unknown `kid`** but at most once per 15 seconds however many such tokens arrive (forged kids cannot
  turn a service into a request amplifier), reads at most 64 KiB, needs a 200, and a response with no usable Ed25519
  key never replaces the cache. If a refresh fails, keys already held keep working for at most one more hour.
- With a public key file: reads it at start; rotate by replacing the file (a bundle may hold several keys) and
  restarting.

### Rotation

1. Generate the new key pair. Give the issuer the new private key as `JWT_PRIVATE_KEY[_FILE]` and the **old public key**
   as `JWT_PREVIOUS_PUBLIC_KEYS[_FILE]`, then restart it. The JWKS now lists both; new tokens carry the new `kid`.
   Verifiers using the JWKS pick up the new key on the first token they don't know (within 15 s); nothing to restart.
2. After the longest token lifetime (`TOKEN_TTL`, 1 h by default) plus the cache time (10 min), remove
   `JWT_PREVIOUS_PUBLIC_KEYS`. Verifiers drop the old key on their next refresh.
3. Verifiers on a static `JWT_PUBLIC_KEY_FILE` need the bundle (old + new) rolled out **before** step 1, and the old key
   removed after step 2.

### Migrating from the shared HS256 secret

HS256 verification was **removed, not kept behind a flag**. Reasons: the whole fleet is built and deployed together from
this repository; tokens live 1 hour and the apps fetch a new one on any 401, so the cost of cutting over is a few
rejected requests, not a data migration; and a "default-off" HS256 path would keep the shared secret in every service's
configuration (the thing this change removes) and keep an algorithm-confusion surface that an attacker only needs one
misconfigured environment to use.

Rollout order:

1. Create the Ed25519 key pair; give the private key to the issuer only.
2. Deploy the issuer first (it serves the JWKS). It now issues EdDSA tokens.
3. Deploy the services with `JWT_JWKS_URL` set (the Kubernetes ConfigMap key is `JWT_JWKS_URL`). Until a service is
   rolled it rejects EdDSA tokens and after it is rolled it rejects the old HS256 ones; callers get a 401 and fetch a new
   token. Expect a short burst of 401s (see the runbook), not an outage.
4. Delete the `grab-mini-jwt` Secret and every `JWT_SECRET` / `JWT_SECRET_FILE` from your environments. A service that
   still has `JWT_SECRET` set logs that it is ignored.

Env var changes: `JWT_SECRET[_FILE]` is gone from every service. New: `JWT_JWKS_URL`, `JWT_PUBLIC_KEY[_FILE]`
(verifiers); `JWT_PRIVATE_KEY[_FILE]`, `JWT_PREVIOUS_PUBLIC_KEYS[_FILE]` (issuer only).

Residual risk: `JWT_JWKS_URL` may be plain `http://` (an in-cluster Auth Service). Anyone who can alter that traffic can
publish their own key, so prefer `https://` or a mounted public key file, and keep the egress NetworkPolicy in place.

## Schema Registry

`pkg/eventcodec` limits any registry response to 1 MiB (a larger body is an error, for success and error responses
alike) and speaks to the registry with:

| Variable | Meaning |
|---|---|
| `SCHEMA_REGISTRY_URL` | `http://` for the local registry, `https://` for a protected one |
| `SCHEMA_REGISTRY_USER`, `SCHEMA_REGISTRY_PASSWORD` | HTTP basic auth; each also accepts `_FILE`. Both or neither. Unset = no auth (local dev) |
| `SCHEMA_REGISTRY_CA_FILE` | optional PEM CA bundle to trust for an `https` registry |

With `APP_ENV=production`, credentials are refused over plain `http` (a bad setting is reported on the first registry
call, never ignored). The Kubernetes base mounts the Secret `grab-mini-schema-registry` (`username`, `password`,
`ca.crt`) into every service.

## Network policy

The Kubernetes base denies ingress **and egress** for every grab-mini pod and then allows only what each needs: DNS;
Kafka (`9093`, services only); Postgres (`5432`; the database-owning services and the migration and retention jobs);
Redis (`6379`, `26379`; the services that use it); the Schema Registry (`8081`); the OTLP collector in `monitoring`
(`4317`, `4318`); and the JWKS (the Auth Service pod on `3005`, or an external identity provider on `443`). Postgres,
Redis, the registry and the identity provider are not deployed by the base: each is allowed as an in-namespace pod
labelled `app.kubernetes.io/name=postgres|redis|schema-registry|auth-service` or as a private-range address on that
one port. Narrow `PRIVATE_CIDRS` in `deploy/k8s/generate.py` (or patch the policies) to your real endpoints.
The CNI must enforce NetworkPolicy (Calico, Cilium, ...) and kube-dns must be in `kube-system` labelled `k8s-app=kube-dns`.

## CI supply chain

Every action in `.github/workflows/*.yml` is pinned to a full commit SHA with the release in a trailing comment.
To update one: resolve the tag (`gh api repos/<owner>/<repo>/git/ref/tags/<tag>`, dereferencing an annotated tag with
`.../git/tags/<sha>`), then change the SHA and the comment together.
