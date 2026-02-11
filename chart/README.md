# S3Proxy Helm Chart

## Install

```bash
helm install s3proxy oci://ghcr.io/serversidehannes/s3proxy-python/charts/s3proxy-python \
  --set secrets.encryptKey="your-key" \
  --set secrets.awsAccessKeyId="AKIA..." \
  --set secrets.awsSecretAccessKey="wJalr..."
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
| `secrets.encryptKey` | `""` | AES-256 encryption key |
| `secrets.awsAccessKeyId` | `""` | AWS access key |
| `secrets.awsSecretAccessKey` | `""` | AWS secret key |
| `secrets.existingSecrets.enabled` | `false` | Use pre-created K8s secret |
| `secrets.existingSecrets.name` | `""` | Existing secret name |
| `secrets.existingSecrets.keys.encryptKey` | `S3PROXY_ENCRYPT_KEY` | Key name in existing secret |
| `secrets.existingSecrets.keys.awsAccessKeyId` | `AWS_ACCESS_KEY_ID` | Key name in existing secret |
| `secrets.existingSecrets.keys.awsSecretAccessKey` | `AWS_SECRET_ACCESS_KEY` | Key name in existing secret |
| `redis-ha.enabled` | `true` | Deploy embedded Redis HA |
| `redis-ha.replicas` | `1` | Redis replicas |
| `redis-ha.auth` | `false` | Enable Redis auth |
| `redis-ha.haproxy.enabled` | `true` | Deploy HAProxy for Redis |
| `redis-ha.persistentVolume.enabled` | `true` | Persistent storage |
| `redis-ha.persistentVolume.size` | `10Gi` | Volume size |
| `externalRedis.url` | `""` | External Redis URL |
| `externalRedis.uploadTtlHours` | `24` | Upload state TTL |
| `externalRedis.existingSecret` | `""` | K8s secret with Redis password |
| `externalRedis.passwordKey` | `redis-password` | Key name in Redis secret |
| `service.type` | `ClusterIP` | Service type |
| `service.port` | `4433` | Service port |
| `ingress.enabled` | `false` | Enable ingress |
| `ingress.className` | `nginx` | Ingress class |
| `ingress.annotations` | nginx streaming defaults | Ingress annotations |
| `ingress.hosts` | `[]` | Ingress hostnames |
| `ingress.tls` | `[]` | Ingress TLS config |
| `gateway.enabled` | `false` | ExternalName gateway service |
| `gateway.serviceName` | `s3-gateway` | Gateway service name |
| `gateway.ingressService` | `ingress-nginx-controller...` | Target ingress service |
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
