<p align="center">
  <img src="https://img.shields.io/badge/encryption-AES--256--GCM-00d4aa?style=for-the-badge" alt="AES-256-GCM">
  <img src="https://img.shields.io/badge/python-3.13+-3776ab?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.11+">
  <img src="https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white" alt="FastAPI">
  <img src="https://img.shields.io/badge/S3-compatible-ff9900?style=for-the-badge&logo=amazons3&logoColor=white" alt="S3 Compatible">
</p>

<h1 align="center">S3Proxy</h1>

<p align="center">
  <strong>Transparent encryption for your S3 storage. Zero code changes required.</strong>
</p>

<p align="center">
  Drop-in S3 proxy that encrypts everything on the fly with AES-256-GCM.<br/>
  Your apps talk to S3Proxy. S3Proxy talks to S3. Your data stays yours.
</p>

---

## Why S3Proxy?

Most teams store sensitive data in S3. Most of that data? **Unencrypted at the application level.**

S3's server-side encryption is great, but your cloud provider still holds the keys. With S3Proxy, **you** control encryption. Every object is encrypted before it ever touches S3.

```
┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│              │         │              │         │              │
│   Your App   │ ──────▶ │   S3Proxy    │ ──────▶ │   AWS S3     │
│              │         │  (encrypts)  │         │  (storage)   │
│              │ ◀────── │  (decrypts)  │ ◀────── │              │
└──────────────┘         └──────────────┘         └──────────────┘
      ▲                        │
      │                        │
    Plain                 AES-256-GCM
    Data                  Encrypted
```

---

## ✨ Features

🔐 **End-to-End Encryption** — AES-256-GCM with per-object keys wrapped via AES-KWP

🔄 **100% S3 Compatible** — Works with any S3 client, SDK, or CLI. No code changes.

⚡ **Streaming I/O** — Async Python with streaming encryption, no memory buffering

📦 **Multipart Support** — Large file uploads just work, encrypted seamlessly

✅ **AWS SigV4 Verified** — Full signature verification for all requests

🏗️ **Production Ready** — Redis-backed state, horizontal scaling, Kubernetes native

---

## 🚀 Quick Start

### 1. Start the proxy

```bash
docker run -p 4433:4433 \
  -e S3PROXY_ENCRYPT_KEY="your-32-byte-encryption-key-here" \
  -e S3PROXY_NO_TLS=true \
  -e AWS_ACCESS_KEY_ID="AKIA..." \
  -e AWS_SECRET_ACCESS_KEY="wJalr..." \
  s3proxy:latest
```

### 2. Configure your client with the same credentials

The client must use the **same credentials** that the proxy is configured with:

```bash
export AWS_ACCESS_KEY_ID="AKIA..."        # Same as proxy
export AWS_SECRET_ACCESS_KEY="wJalr..."   # Same as proxy
```

### 3. Point your application at the proxy

```bash
# Upload through S3Proxy - data is encrypted before reaching S3
aws s3 --endpoint-url http://localhost:4433 cp secret.pdf s3://my-bucket/

# Download through S3Proxy - data is decrypted automatically
aws s3 --endpoint-url http://localhost:4433 cp s3://my-bucket/secret.pdf ./

# Works with any S3 client/SDK - just change the endpoint URL
```

Your file is now encrypted at rest with AES-256-GCM. The encryption is transparent—your application code doesn't change, only the endpoint URL.

> **Note:** The proxy supports any bucket accessible with the configured credentials. You don't configure a specific bucket—just point any S3 request at the proxy and it forwards to the appropriate bucket.

---

## 🔍 How It Works

S3Proxy sits between your application and S3, transparently encrypting all data before it reaches storage.

### Request Flow

```
1. Client signs request with credentials (same credentials configured on proxy)
2. Proxy receives request and verifies SigV4 signature
3. Proxy encrypts the payload with AES-256-GCM
4. Proxy re-signs the request (encryption changes the body, invalidating original signature)
5. Proxy forwards to S3
6. S3 stores the encrypted data
```

### Why Does the Proxy Need My Credentials?

**Short answer:** Because encryption changes the request body, which invalidates the client's signature. The proxy must re-sign requests, and re-signing requires the secret key.

With S3's SigV4 authentication, clients sign requests using their secret key but only send the signature—never the key itself. When S3Proxy encrypts your data, it modifies:
- The request body (now ciphertext instead of plaintext)
- The `Content-Length` header
- The `Content-MD5` / `x-amz-content-sha256` headers

This breaks the original signature. To forward the request to S3, the proxy must create a new valid signature, which requires having the secret key.

**The proxy acts as a trusted intermediary**, not a transparent passthrough. You configure credentials once on the proxy, and all clients use those same credentials to authenticate.

```
┌──────────────┐  SigV4 signed   ┌──────────────┐  Re-signed     ┌──────────────┐
│              │  (credentials   │              │  (same         │              │
│    Client    │ ─────────────▶  │   S3Proxy    │ ─────────────▶ │    AWS S3    │
│              │   from proxy)   │              │  credentials)  │              │
└──────────────┘                 └──────────────┘                └──────────────┘
```

### Encryption

S3Proxy uses a **layered key architecture**:

| Layer | Key | Purpose |
|-------|-----|---------|
| **KEK** | Derived from your master key | Wraps all DEKs |
| **DEK** | Random per object | Encrypts actual data |
| **Nonce** | Random/deterministic | Ensures uniqueness |

Your master key never touches S3. DEKs are wrapped and stored as object metadata. Even if someone accesses your bucket, they get nothing but ciphertext.

### Multipart Uploads

Large files are handled via S3 multipart upload. Each part is encrypted independently with its own nonce, and part metadata is tracked in Redis (or in-memory for single-instance). This enables streaming uploads of arbitrary size without buffering entire files in memory.

---

## ⚙️ Configuration

Configure via environment variables (Docker) or Helm values (Kubernetes).

| Setting | Environment Variable | Helm Value | Default |
|---------|---------------------|------------|---------|
| **Encryption key** | `S3PROXY_ENCRYPT_KEY` | `secrets.encryptKey` | — |
| **AWS Access Key** | `AWS_ACCESS_KEY_ID` | `secrets.awsAccessKeyId` | — |
| **AWS Secret Key** | `AWS_SECRET_ACCESS_KEY` | `secrets.awsSecretAccessKey` | — |
| S3 endpoint | `S3PROXY_HOST` | `s3.host` | `s3.amazonaws.com` |
| AWS region | `S3PROXY_REGION` | `s3.region` | `us-east-1` |
| Listen port | `S3PROXY_PORT` | `server.port` | `4433` |
| Disable TLS | `S3PROXY_NO_TLS` | `server.noTls` | `false` |
| Log level | `S3PROXY_LOG_LEVEL` | `server.logLevel` | `INFO` |
| Redis URL | `S3PROXY_REDIS_URL` | `externalRedis.url` | *(empty)* |
| Max concurrent requests | `S3PROXY_THROTTLING_REQUESTS_MAX` | `performance.throttlingRequestsMax` | `10` |
| Max upload size (MB) | `S3PROXY_MAX_UPLOAD_SIZE_MB` | `performance.maxUploadSizeMb` | `45` |

> **Credentials:** Clients must use the same `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` configured on the proxy. See [How It Works](#-how-it-works).

> **S3-compatible:** Works with AWS S3, MinIO, Cloudflare R2, DigitalOcean Spaces, etc.

> **Redis:** Only required for multi-instance (HA) deployments. Single-instance uses in-memory storage.

---

## ☸️ Production Deployment

### Kubernetes with Helm

The Helm chart is in `manifests/` and includes Redis HA with Sentinel for distributed state.

#### Quick Start

```bash
# Install Helm dependencies (redis-ha)
cd manifests && helm dependency update && cd ..

# Install with inline secrets (dev/test only)
helm install s3proxy ./manifests \
  --set secrets.encryptKey="your-32-byte-encryption-key" \
  --set secrets.awsAccessKeyId="AKIA..." \
  --set secrets.awsSecretAccessKey="wJalr..."
```

#### Production Setup

For production, use Kubernetes secrets instead of inline values:

```bash
# Create secret manually
kubectl create secret generic s3proxy-secrets \
  --from-literal=S3PROXY_ENCRYPT_KEY="your-32-byte-encryption-key" \
  --from-literal=AWS_ACCESS_KEY_ID="AKIA..." \
  --from-literal=AWS_SECRET_ACCESS_KEY="wJalr..."

# Install referencing the existing secret
helm install s3proxy ./manifests \
  --set secrets.existingSecrets.enabled=true \
  --set secrets.existingSecrets.name=s3proxy-secrets
```

#### Accessing the Proxy

Point your S3 clients at the proxy endpoint:

```bash
# From within the cluster (default service)
aws s3 --endpoint-url http://s3proxy-python.<namespace>:4433 cp file.txt s3://bucket/

# With gateway enabled (recommended for internal access)
aws s3 --endpoint-url http://s3-gateway.<namespace> cp file.txt s3://bucket/

# With ingress (external access)
aws s3 --endpoint-url https://s3proxy.example.com cp file.txt s3://bucket/
```

#### Kubernetes-Specific Settings

| Helm Value | Default | Description |
|------------|---------|-------------|
| `replicaCount` | `3` | Number of proxy replicas |
| `redis-ha.enabled` | `true` | Deploy embedded Redis HA with Sentinel |
| `resources.requests.memory` | `512Mi` | Memory request per pod |
| `resources.limits.memory` | `512Mi` | Memory limit per pod |
| `ingress.enabled` | `false` | Enable ingress for load balancing |
| `ingress.className` | `nginx` | Ingress class |
| `ingress.hosts` | `[]` | Hostnames for external access |
| `gateway.enabled` | `false` | Create internal DNS alias (`s3-gateway.<namespace>`) |

**Gateway vs Ingress:**

| gateway | ingress | Use case |
|---------|---------|----------|
| `false` | `true` | External access via custom hostname (requires DNS setup) |
| `true` | `true` | Internal access via `s3-gateway.<namespace>` (no DNS setup needed) |

#### Example: External Access with Ingress

```yaml
# values-prod.yaml
gateway:
  enabled: true
ingress:
  enabled: true
  className: nginx
  hosts:
    - s3proxy.example.com
  tls:
    - secretName: s3proxy-tls
      hosts:
        - s3proxy.example.com
```

```bash
helm install s3proxy ./manifests -f values-prod.yaml \
  --set secrets.existingSecrets.enabled=true \
  --set secrets.existingSecrets.name=s3proxy-secrets
```

#### Example: Using External Redis (ElastiCache, etc.)

```bash
helm install s3proxy ./manifests \
  --set redis-ha.enabled=false \
  --set externalRedis.url="redis://my-elasticache.xxx.cache.amazonaws.com:6379/0" \
  --set secrets.existingSecrets.enabled=true \
  --set secrets.existingSecrets.name=s3proxy-secrets
```

### Health Checks

The proxy exposes health endpoints for Kubernetes probes:
- `GET /healthz` — Liveness probe
- `GET /readyz` — Readiness probe

### Security Considerations

- **TLS Termination**: The chart defaults to `noTls=true`, expecting TLS termination at the ingress/load balancer
- **Secrets**: Always use `secrets.existingSecrets` in production—never commit secrets to values files
- **Network Policy**: Consider restricting pod-to-pod traffic to only allow proxy → Redis
- **Encryption Key**: Back up your encryption key securely. Losing it means losing access to all encrypted data

### Resource Recommendations

| Workload | Memory | CPU | Concurrency | Notes |
|----------|--------|-----|-------------|-------|
| Standard | 512Mi | 100m | 10 | Default settings |
| Heavy | 1Gi+ | 500m | 20+ | Large files, high concurrency |

Memory scales with concurrent uploads. Use `performance.throttlingRequestsMax` to bound memory usage

---

## 🧪 Testing

```bash
make test           # Unit tests
make cluster-test   # Full Kubernetes cluster test
```

---

## ❓ FAQ

**Why can't I use my own AWS credentials with the proxy?**

The proxy must re-sign requests after encryption (see [How It Works](#-how-it-works)). Re-signing requires the secret key, but S3's SigV4 protocol only sends signatures—never the secret key itself. So the proxy must already have the credentials configured. All clients share the same credentials configured on the proxy.

**Can I use different credentials for different clients?**

Not currently. The proxy supports one credential pair. If you need per-client credentials, you would deploy multiple proxy instances or implement a credential lookup mechanism.

**Can I use this with existing unencrypted data?**

Yes. S3Proxy only encrypts data written through it. Existing objects remain readable—S3Proxy detects unencrypted objects and returns them as-is. To migrate, simply copy objects through S3Proxy:

```bash
aws s3 cp --endpoint-url http://localhost:4433 s3://bucket/file.txt s3://bucket/file.txt
```

**What happens if I lose my encryption key?**

Your data is unrecoverable. The KEK is never stored—it exists only in your environment variables. Back up your key securely.

**Can I rotate encryption keys?**

Not currently. Key rotation would require re-encrypting all objects. This is on the roadmap.

**Does S3Proxy support SSE-C or SSE-KMS?**

No. S3Proxy implements its own client-side encryption. Server-side encryption options are orthogonal—you can enable both if desired.

---

## 🤝 Contributing

Contributions are welcome.

---

## 📄 License

MIT
