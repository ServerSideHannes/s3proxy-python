<p align="center">
  <img src="https://img.shields.io/badge/AES--256--GCM-00d4aa?style=for-the-badge" alt="AES-256-GCM">
  <img src="https://img.shields.io/badge/Python_3.14+-3776ab?style=for-the-badge&logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/Kubernetes-326ce5?style=for-the-badge&logo=kubernetes&logoColor=white" alt="Kubernetes">
</p>

<h1 align="center">S3Proxy</h1>

<p align="center">
  <strong>Transparent client-side encryption for S3. Zero code changes.</strong>
</p>

<p align="center">
  <a href="https://github.com/ServerSideHannes/s3proxy-python/actions/workflows/helm-install-test.yml">
    <img src="https://github.com/ServerSideHannes/s3proxy-python/actions/workflows/helm-install-test.yml/badge.svg" alt="Helm Install">
  </a>
  <img src="https://img.shields.io/badge/Ceph_s3--tests-59%25-yellowgreen" alt="Ceph S3 Compatibility">
</p>

---

## Overview

S3's server-side encryption is great, but your cloud provider holds the keys. S3Proxy sits between your app and S3, encrypting everything **before** it leaves your infrastructure.

```
┌──────────┐         ┌──────────┐         ┌──────────┐
│          │  plain  │          │  AES    │          │
│ Your App │ ──────▶ │ S3Proxy  │ ──────▶ │    S3    │
│          │  data   │          │  256    │          │
└──────────┘         └──────────┘         └──────────┘
                           │
                     You own the keys.
```

<p align="center">
  <img src="https://img.shields.io/badge/✓_Streaming_I/O-00d4aa?style=flat-square" alt="Streaming">
  <img src="https://img.shields.io/badge/✓_Multipart_Uploads-00d4aa?style=flat-square" alt="Multipart">
  <img src="https://img.shields.io/badge/✓_SigV4_Auth-00d4aa?style=flat-square" alt="SigV4">
  <img src="https://img.shields.io/badge/✓_Redis_HA-00d4aa?style=flat-square" alt="Redis HA">
  <img src="https://img.shields.io/badge/✓_Horizontal_Scaling-00d4aa?style=flat-square" alt="Scaling">
</p>

---

## Install

Each AWS credential is configured with its **own encryption key** (KEK). The proxy verifies the client's signature with the credential's secret key, then encrypts/decrypts that credential's objects with its KEK.

**Option A** — inline secrets (quick start):

```bash
helm install s3proxy oci://ghcr.io/serversidehannes/s3proxy-python/charts/s3proxy-python \
  --set secrets.credentials[0].accessKey="AKIA..." \
  --set secrets.credentials[0].secretKey="wJalr..." \
  --set secrets.credentials[0].kek="this-credentials-encryption-secret"
```

**Option B** — existing K8s secret (recommended for production):

```bash
kubectl create secret generic s3proxy-secrets \
  --from-literal=S3PROXY_CREDENTIALS='[{"access_key":"AKIA...","secret_key":"wJalr...","kek":"this-credentials-encryption-secret"}]'

helm install s3proxy oci://ghcr.io/serversidehannes/s3proxy-python/charts/s3proxy-python \
  --set secrets.existingSecrets.enabled=true \
  --set secrets.existingSecrets.name=s3proxy-secrets
```

Then point any S3 client at the proxy:

```bash
aws s3 --endpoint-url http://s3proxy-python:4433 cp file.txt s3://bucket/
```

Use the **same credentials** you configured above. That's it.

> **Endpoints** — In-cluster: `http://s3proxy-python.<ns>:4433` · Front proxy (even load distribution, `frontproxy.enabled=true`): `http://s3proxy-python-frontproxy.<ns>` · Outside the cluster: enable `ingress` in front of the front proxy
>
> **Health** — `GET /healthz` · `GET /readyz` · **Metrics** — `GET /metrics`

---

## Battle-Tested

Verified with real database operators: **backup, cluster delete, restore, data integrity check.**

| Database | Operator | Backup Tool |
|:--------:|:--------:|:-----------:|
| PostgreSQL 17 | CloudNativePG 1.25 | Barman S3 |
| Elasticsearch 9.x | ECK 3.2.0 | S3 Snapshots |
| ScyllaDB 6.x | Scylla Operator 1.19 | Scylla Manager |
| ClickHouse 24.x | Altinity Operator | clickhouse-backup |

---

## How It Works

**Credential flow** — S3 clients sign requests with their secret key. When S3Proxy encrypts the payload, the body changes and the original signature is invalidated. The proxy re-signs with the same key. Configure credentials once on the proxy, all clients use them.

**Envelope encryption** — Your master key derives a KEK (Key Encryption Key). Each object gets a random DEK (Data Encryption Key), encrypted with AES-256-GCM. The DEK is wrapped by the KEK and stored as object metadata. Your master key never touches S3.

```
Master Key → KEK (derived via SHA-256)
              └→ wraps DEK (random per object)
                   └→ encrypts data (AES-256-GCM)
```

**Per-credential keys** — Each AWS credential has its own KEK. The proxy verifies the client's signature with the credential's secret key, then wraps that credential's DEKs with the credential's KEK. So a leaked KEK only exposes the data written by that one credential. The access key that wrapped each object is recorded in the object's metadata (`isec-kid`), so **decryption always uses the key that actually encrypted the object** — reconfiguring credentials never orphans existing data, as long as that access key's KEK is still present.

```bash
# Each credential: access_key + secret_key + its own kek (SHA-256'd into the KEK)
S3PROXY_CREDENTIALS='[
  {"access_key":"AKIA-ACME","secret_key":"...","kek":"acme-kek-secret"},
  {"access_key":"AKIA-GLOBEX","secret_key":"...","kek":"globex-kek-secret"}
]'
```

A request signed by an access key with no configured KEK is rejected. Via Helm: set `secrets.credentials` (see [chart/values.yaml](chart/values.yaml)).

---

## Configuration

| Value | Default | Description |
|-------|---------|-------------|
| `replicaCount` | `3` | Pod replicas |
| `s3.host` | `s3.amazonaws.com` | S3 endpoint (AWS, MinIO, R2, etc.) |
| `s3.region` | `us-east-1` | AWS region |
| `secrets.credentials` | `[]` | AWS credentials, each `{accessKey, secretKey, kek}` |
| `secrets.existingSecrets.enabled` | `false` | Use existing K8s secret |
| `admin.secret` | `change-me` | Secret signing admin session cookies (when admin UI on) |
| `redis-ha.enabled` | `true` | Deploy embedded Redis HA |
| `frontproxy.enabled` | `false` | Bundled HAProxy for even load distribution (no external dependency) |
| `ingress.enabled` | `false` | Expose S3 outside the cluster via Ingress (requires `frontproxy.enabled`) |
| `performance.memoryLimitMb` | `64` | Memory budget for streaming concurrency |

See [chart/README.md](chart/README.md) for all options.

---

## FAQ

<details>
<summary><strong>Can I use existing unencrypted data?</strong></summary>
Yes. S3Proxy detects unencrypted objects and returns them as-is. Migrate by copying through the proxy.
</details>

<details>
<summary><strong>What if I lose an encryption key?</strong></summary>
Data written by that credential is unrecoverable. Each object records the access key that encrypted it, so keep every credential's <code>kek</code> as long as objects written by that credential exist. Back up your keys.
</details>

<details>
<summary><strong>What if Redis fails mid-upload?</strong></summary>
Upload fails and must restart. Use <code>redis-ha.enabled=true</code> with persistence.
</details>

<details>
<summary><strong>MinIO / R2 / Spaces?</strong></summary>
Yes. Set <code>s3.host</code> to your endpoint.
</details>

<details>
<summary><strong>Presigned URLs?</strong></summary>
Yes. The proxy verifies the presigned signature, then makes its own authenticated request to S3.
</details>

---

## Roadmap

- [ ] Key rotation (re-encrypt objects with a new master key)
- [x] Multiple AWS credential pairs (per-client auth)
- [x] Per-credential encryption keys
- [ ] S3 Select passthrough
- [ ] Ceph S3 compatibility > 80%
- [ ] Batch re-encryption CLI tool
- [ ] Audit logging (who accessed what, when)
- [ ] Web dashboard for key & upload status

---

## Changelog

### 2026.2.0

- Modular handler architecture (`objects/`, `multipart/`, `routing/`, `client/`, `streaming/`)
- Memory-based concurrency limiting (replaces count-based), default 64 MB budget
- Redis state management with automatic recovery; fix data loss on multipart complete/retry
- Hardened input validation, XML escaping, backpressure, and error handling
- Helm chart restructured (`manifests/` → `chart/`) with PDB, standardized labels, and [config reference](chart/README.md)
- E2E tests for PostgreSQL, Elasticsearch, ScyllaDB, ClickHouse, and S3 compatibility
- CI workflows for ruff linting and unit tests
- Prometheus-compatible metrics endpoint
- Slimmer Dockerfile and Makefile improvements

---

## License

MIT
