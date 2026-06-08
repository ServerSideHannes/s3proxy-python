# S3Proxy Helm Chart

## Install

```bash
helm install s3proxy oci://ghcr.io/serversidehannes/s3proxy-python/charts/s3proxy-python \
  --set secrets.credentials[0].accessKey="AKIA..." \
  --set secrets.credentials[0].secretKey="wJalr..." \
  --set secrets.credentials[0].kek="this-credentials-encryption-secret"
```

## Values

| Value | Default | Description |
|-------|---------|-------------|
| `replicaCount` | `3` | Pod replicas |
| `image.repository` | `ghcr.io/ServerSideHannes/s3proxy-python` | Container image |
| `image.tag` | `latest` | Image tag |
| `image.pullPolicy` | `IfNotPresent` | Pull policy |
| `s3.host` | `s3.amazonaws.com` | S3 endpoint |
| `s3.region` | `us-east-1` | AWS region |
| `server.port` | `4433` | Proxy listen port |
| `server.noTls` | `true` | Disable TLS (in-cluster only) |
| `performance.memoryLimitMb` | `64` | Memory budget for streaming |
| `logLevel` | `DEBUG` | Log level |
| `secrets.credentials` | `[]` | AWS credentials, each `{accessKey, secretKey, kek}` — the credential's KEK encrypts its objects |
| `secrets.existingSecrets.enabled` | `false` | Use pre-created K8s secret |
| `secrets.existingSecrets.name` | `""` | Existing secret name |
| `secrets.existingSecrets.keys.credentials` | `S3PROXY_CREDENTIALS` | Credentials key name in existing secret |
| `admin.enabled` | `false` | Enable the admin dashboard |
| `admin.path` | `/admin` | URL path prefix for the dashboard |
| `admin.username` | `admin` | Dashboard username (stored in the Secret; override in production) |
| `admin.password` | `admin` | Dashboard password (stored in the Secret; override in production) |
| `admin.secret` | `change-me` | Secret signing dashboard session cookies (override in production) |
| `admin.existingSecret.name` | `""` | Pre-created secret holding admin credentials |
| `admin.existingSecret.usernameKey` | `S3PROXY_ADMIN_USERNAME` | Username key in the existing secret |
| `admin.existingSecret.passwordKey` | `S3PROXY_ADMIN_PASSWORD` | Password key in the existing secret |
| `admin.existingSecret.secretKey` | `S3PROXY_ADMIN_SECRET` | Session-secret key in the existing secret |
| `admin.ingress.enabled` | `false` | Dedicated Ingress for the dashboard (keep off unless intentionally exposing it) |
| `admin.ingress.className` | `""` | Ingress class for the admin Ingress (set to your cluster's controller) |
| `admin.ingress.host` | `""` | Hostname for the dashboard (required when enabled) |
| `admin.ingress.annotations` | `{}` | Annotations (e.g. IP allowlist) for the admin Ingress |
| `admin.ingress.tls` | `[]` | TLS config for the admin Ingress |
| `redis.enabled` | `true` | Deploy the bundled single Redis pod (transient upload state) |
| `redis.image.repository` | `redis` | Redis image (bump manually; not tracked by dependabot) |
| `redis.image.tag` | `7-alpine` | Redis image tag |
| `redis.auth.enabled` | `false` | Require a password on the bundled Redis |
| `redis.auth.password` | `""` | Password (when auth enabled and no existingSecret) |
| `redis.auth.existingSecret` | `""` | Use a pre-created Secret for the Redis password |
| `redis.auth.passwordKey` | `redis-password` | Key name in the Redis password Secret |
| `externalRedis.url` | `""` | External Redis URL (used when `redis.enabled=false`) |
| `externalRedis.uploadTtlHours` | `24` | Upload state TTL |
| `externalRedis.existingSecret` | `""` | K8s secret with Redis password |
| `externalRedis.passwordKey` | `redis-password` | Key name in Redis secret |
| `service.type` | `ClusterIP` | Service type |
| `service.port` | `4433` | Service port |
| `frontproxy.enabled` | `false` | Deploy a bundled HAProxy front proxy for even load distribution (see below) |
| `frontproxy.replicaCount` | `2` | Front proxy replicas |
| `frontproxy.image.repository` | `haproxy` | Front proxy image |
| `frontproxy.image.tag` | `3.0-alpine` | Front proxy image tag |
| `frontproxy.service.type` | `ClusterIP` | Front proxy service type |
| `frontproxy.service.port` | `80` | Front proxy service port (clients connect here) |
| `frontproxy.timeouts.client` | `1h` | Client-side timeout (tolerate large transfers) |
| `frontproxy.timeouts.server` | `1h` | Backend timeout (tolerate large transfers) |
| `frontproxy.timeouts.connect` | `10s` | Backend connect timeout |
| `frontproxy.podDisruptionBudget.enabled` | `true` | Enable front proxy PDB |
| `frontproxy.podDisruptionBudget.minAvailable` | `1` | Min available front proxy pods |
| `ingress.enabled` | `false` | Expose S3 outside the cluster via Ingress (requires `frontproxy.enabled`) |
| `ingress.className` | `""` | Ingress class (set to your cluster's controller) |
| `ingress.annotations` | `{}` | Ingress annotations (controller-specific tuning) |
| `ingress.hosts` | `[]` | Ingress host/path rules |
| `ingress.tls` | `[]` | Ingress TLS config |
| `resources.requests.cpu` | `100m` | CPU request |
| `resources.requests.memory` | `512Mi` | Memory request |
| `resources.limits.cpu` | `500m` | CPU limit |
| `resources.limits.memory` | `512Mi` | Memory limit |
| `nodeSelector` | `{}` | Node selector |
| `tolerations` | `[]` | Tolerations |
| `affinity` | `{}` | Affinity rules |
| `topologySpreadConstraints` | `[]` | Topology spread |
| `podDisruptionBudget.enabled` | `true` | Enable PDB |
| `podDisruptionBudget.minAvailable` | `1` | Min available pods |

## Even load distribution (front proxy)

A plain `ClusterIP` Service balances per **connection** (L4, via kube-proxy). S3
clients (boto3, aws-cli) hold long-lived keep-alive connections, so each client
tends to pin to a single pod — leaving load uneven across replicas.

Setting `frontproxy.enabled=true` deploys a small bundled HAProxy in front of the
s3proxy pods that balances per **request** (L7), spreading even a single client's
stream across every pod. It is fully self-contained — it requires **no** ingress
controller, service mesh, or anything else in the cluster.

```bash
helm install s3proxy ... --set frontproxy.enabled=true
```

Clients then connect to the front proxy Service (`s3proxy-python-frontproxy`)
instead of the s3proxy Service. The front proxy runs `replicaCount: 2` with a PDB so
it is not a single point of failure.

### Exposing S3 outside the cluster

For S3 operations from **outside** the cluster, enable an Ingress in front of the
front proxy. It is controller-agnostic — set `ingress.className` to whatever ingress
controller you run and add any controller-specific tuning via `ingress.annotations`.

```bash
helm install s3proxy ... \
  --set frontproxy.enabled=true \
  --set ingress.enabled=true \
  --set ingress.className=nginx \
  --set 'ingress.hosts[0].host=s3.example.com' \
  --set 'ingress.hosts[0].paths[0].path=/' \
  --set 'ingress.hosts[0].paths[0].pathType=Prefix'
```

The Ingress routes to the front proxy, so external clients also get even per-request
distribution. `ingress.enabled` therefore requires `frontproxy.enabled=true`.
