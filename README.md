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

**Option A** — inline secrets (quick start):

```bash
helm install s3proxy oci://ghcr.io/serversidehannes/s3proxy-python/charts/s3proxy-python \
  --set secrets.encryptKey="your-32-byte-key" \
  --set secrets.awsAccessKeyId="AKIA..." \
  --set secrets.awsSecretAccessKey="wJalr..."
```

**Option B** — existing K8s secret (recommended for production):

```bash
kubectl create secret generic s3proxy-secrets \
  --from-literal=S3PROXY_ENCRYPT_KEY="your-32-byte-key" \
  --from-literal=AWS_ACCESS_KEY_ID="AKIA..." \
  --from-literal=AWS_SECRET_ACCESS_KEY="wJalr..."

helm install s3proxy oci://ghcr.io/serversidehannes/s3proxy-python/charts/s3proxy-python \
  --set secrets.existingSecrets.enabled=true \
  --set secrets.existingSecrets.name=s3proxy-secrets
```

Then point any S3 client at the proxy:

```bash
aws s3 --endpoint-url http://s3proxy-python:4433 cp file.txt s3://bucket/
```

Use the **same credentials** you configured above. That's it.

> **Endpoints** — In-cluster: `http://s3proxy-python.<ns>:4433` · Gateway: `http://s3-gateway.<ns>` · Ingress: `https://s3proxy.example.com`
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

---

## Configuration

| Value | Default | Description |
|-------|---------|-------------|
| `replicaCount` | `3` | Pod replicas |
| `s3.host` | `s3.amazonaws.com` | S3 endpoint (AWS, MinIO, R2, etc.) |
| `s3.region` | `us-east-1` | AWS region |
| `secrets.encryptKey` | — | Encryption key |
| `secrets.existingSecrets.enabled` | `false` | Use existing K8s secret |
| `redis-ha.enabled` | `true` | Deploy embedded Redis HA |
| `gateway.enabled` | `false` | Create gateway service |
| `ingress.enabled` | `false` | Enable ingress |
| `performance.memoryLimitMb` | `64` | Memory budget for streaming concurrency |

See [chart/README.md](chart/README.md) for all options.

---

## FAQ

<details>
<summary><strong>Can I use existing unencrypted data?</strong></summary>
Yes. S3Proxy detects unencrypted objects and returns them as-is. Migrate by copying through the proxy.
</details>

<details>
<summary><strong>What if I lose my encryption key?</strong></summary>
Data is unrecoverable. Back up your key.
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
- [ ] Multiple AWS credential pairs (per-client auth)
- [ ] Per-bucket / per-prefix encryption keys
- [ ] S3 Select passthrough
- [ ] Ceph S3 compatibility > 80%
- [ ] Batch re-encryption CLI tool
- [ ] Audit logging (who accessed what, when)
- [ ] Web dashboard for key & upload status

---

## License

MIT
