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
| `server.noTls` | `true` | Disable TLS (in-cluster only; forced off when `server.tls.existingSecret` is set) |
| `server.tls.existingSecret` | `""` | Existing `kubernetes.io/tls` Secret (`tls.crt`/`tls.key`); when set, the proxy serves HTTPS |
| `server.certPath` | `/etc/s3proxy/certs` | Where the TLS cert/key are mounted in the pod |
| `performance.memoryLimitMb` | `64` | Memory budget for streaming |
| `logLevel` | `DEBUG` | Log level |
| `secrets.credentials` | `[]` | AWS credentials, each `{accessKey, secretKey, kek}` — the credential's KEK encrypts its objects |
| `secrets.existingSecrets.enabled` | `false` | Use pre-created K8s secret |
| `secrets.existingSecrets.name` | `""` | Existing secret name |
| `secrets.existingSecrets.keys.credentials` | `S3PROXY_CREDENTIALS` | Credentials key name in existing secret |
| `dashboard.enabled` | `false` | Enable the dashboard |
| `dashboard.path` | `/dashboard` | URL path prefix for the dashboard |
| `dashboard.username` | `admin` | Dashboard username (stored in the Secret; override in production) |
| `dashboard.password` | `admin` | Dashboard password (stored in the Secret; override in production) |
| `dashboard.auth.password.enabled` | `true` | Enable username/password login. Set `false` for SSO-only |
| `dashboard.auth.oidc.enabled` | `false` | Enable OIDC single sign-on (JumpCloud, Okta, Google, Entra ID, ...) |
| `dashboard.auth.oidc.issuer` | `""` | OIDC issuer URL (drives `.well-known/openid-configuration` discovery) |
| `dashboard.auth.oidc.clientId` | `""` | OIDC client ID |
| `dashboard.auth.oidc.clientSecret` | `""` | OIDC client secret (stored in the Secret) |
| `dashboard.auth.oidc.redirectUrl` | `""` | Callback URL; empty = derive from request (`X-Forwarded-Proto`/`Host`) |
| `dashboard.auth.oidc.scopes` | `openid email profile` | Space-separated OIDC scopes |
| `dashboard.auth.oidc.usernameClaim` | `email` | ID-token claim used as the session username |
| `dashboard.auth.oidc.allowedDomains` | `""` | Comma-separated email-domain allowlist (empty = any authenticated user) |
| `dashboard.auth.oidc.buttonLabel` | `Sign in with SSO` | Label for the SSO button on the login page |
| `dashboard.auth.oidc.existingSecret.name` | `""` | Pre-created secret holding the OIDC client secret |
| `dashboard.auth.oidc.existingSecret.clientSecretKey` | `S3PROXY_DASHBOARD_OIDC_CLIENT_SECRET` | Client-secret key in the existing secret |
| `dashboard.frontend.enabled` | `true` | Run the Svelte UI as its own Deployment (nginx serving the static build + reverse-proxying the API) |
| `dashboard.frontend.image.repository` | `ghcr.io/serversidehannes/s3proxy-dashboard` | Dashboard UI image |
| `dashboard.frontend.image.tag` | `latest` | Dashboard UI image tag |
| `dashboard.frontend.replicaCount` | `1` | Dashboard UI replicas |
| `dashboard.frontend.service.port` | `80` | Dashboard Service port |
| `dashboard.existingSecret.name` | `""` | Pre-created secret holding dashboard credentials |
| `dashboard.existingSecret.usernameKey` | `S3PROXY_DASHBOARD_USERNAME` | Username key in the existing secret |
| `dashboard.existingSecret.passwordKey` | `S3PROXY_DASHBOARD_PASSWORD` | Password key in the existing secret |
| `dashboard.ingress.enabled` | `false` | Dedicated Ingress for the dashboard (keep off unless intentionally exposing it) |
| `dashboard.ingress.kind` | `Ingress` | `Ingress` (standard) or `IngressRoute` (Traefik CRD) |
| `dashboard.ingress.className` | `""` | Ingress class for the dashboard Ingress (set to your cluster's controller) |
| `dashboard.ingress.host` | `""` | Hostname for the dashboard (required when enabled) |
| `dashboard.ingress.annotations` | `{}` | Annotations (e.g. IP allowlist) for the dashboard Ingress |
| `dashboard.ingress.tls` | `[]` | TLS config for the dashboard Ingress |
| `redis.enabled` | `true` | Deploy the bundled single Redis pod (transient upload state) |
| `redis.image.repository` | `redis` | Redis image (bump manually; not tracked by dependabot) |
| `redis.image.tag` | `7-alpine` | Redis image tag |
| `redis.auth.enabled` | `false` | Require a password on the bundled Redis |
| `redis.auth.password` | `""` | Password (when auth enabled and no existingSecret) |
| `redis.auth.existingSecret` | `""` | Use a pre-created Secret for the Redis password |
| `redis.auth.passwordKey` | `redis-password` | Key name in the Redis password Secret |
| `redis.persistence.enabled` | `false` | Back Redis `/data` with a PVC instead of an emptyDir |
| `redis.persistence.size` | `1Gi` | PVC size (when persistence enabled) |
| `redis.persistence.storageClassName` | `""` | StorageClass for the PVC (empty = cluster default) |
| `redis.persistence.accessModes` | `[ReadWriteOnce]` | PVC access modes |
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
| `ingress.kind` | `Ingress` | `Ingress` (standard) or `IngressRoute` (Traefik CRD) |
| `ingress.className` | `""` | Ingress class (set to your cluster's controller) |
| `ingress.annotations` | `{}` | Ingress annotations (controller-specific tuning) |
| `ingress.hosts` | `[]` | Ingress host/path rules |
| `ingress.tls` | `[]` | Ingress TLS config |
| `ingress.entryPoints` / `middlewares` | `[]` | Traefik IngressRoute only |
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

## Dashboard login (password + OIDC SSO)

The dashboard supports username/password and/or OIDC SSO; at least one must be on.
For SSO-only, set `dashboard.auth.password.enabled=false`. OIDC is a generic
authorization-code + PKCE flow, so any provider works (JumpCloud, Okta, Google,
Entra ID).

```bash
helm install s3proxy ... \
  --set dashboard.enabled=true \
  --set dashboard.auth.password.enabled=false \
  --set dashboard.auth.oidc.enabled=true \
  --set dashboard.auth.oidc.issuer=https://oauth.id.jumpcloud.com/ \
  --set dashboard.auth.oidc.clientId=<id> \
  --set dashboard.auth.oidc.clientSecret=<secret> \
  --set dashboard.auth.oidc.allowedDomains=example.com
```

- **Redirect URI:** `<dashboard-url>/dashboard/api/oidc/callback` (match `dashboard.path`).
- **Restrict access:** `allowedDomains` (comma-separated) or in the provider.
- **Keep the secret out of values:** `dashboard.auth.oidc.existingSecret.name` + `.clientSecretKey`.

## Serving HTTPS

TLS terminates at the Ingress by default (`*.ingress.tls` `secretName`). To make the
**proxy pod serve HTTPS itself**, point at an existing `kubernetes.io/tls` Secret:

```bash
kubectl create secret tls s3proxy-tls --cert=tls.crt --key=tls.key
helm upgrade s3proxy ... --set server.tls.existingSecret=s3proxy-tls
```

This forces `noTls` off, mounts the cert at `server.certPath`, switches probes to
HTTPS, marks the session cookie `Secure`, and points the dashboard's nginx at the
proxy over HTTPS. Behind a host-rewriting proxy, pin `dashboard.auth.oidc.redirectUrl`.

## Traefik

Both ingresses default to the standard `Ingress` API, which Traefik serves natively —
set `*.ingress.className: traefik`. To emit Traefik's CRD instead, set
`*.ingress.kind: IngressRoute`; then `entryPoints` and `middlewares` apply:

```bash
helm install s3proxy ... \
  --set dashboard.enabled=true \
  --set dashboard.ingress.enabled=true \
  --set dashboard.ingress.kind=IngressRoute \
  --set dashboard.ingress.host=dash.example.com \
  --set dashboard.ingress.entryPoints[0]=websecure \
  --set dashboard.ingress.middlewares[0].name=ipallowlist
```
