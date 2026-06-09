# Build the SvelteKit static dashboard, then serve it from nginx. The dashboard API
# (JSON + SSE) and auth endpoints stay in the proxy process; the ingress routes
# ${dashboard.path}/api and ${dashboard.path}/login|logout to the proxy and everything
# else here, so the app uses same-origin relative URLs.

FROM node:24-slim AS builder
WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci

COPY . .
# Base path must match the ingress mount (dashboard.path). Override at build time
# if the chart is configured with a non-default dashboard.path.
ARG DASHBOARD_BASE_PATH=/dashboard
ENV DASHBOARD_BASE_PATH=${DASHBOARD_BASE_PATH}
RUN npm run build


# Unprivileged image: listens on 8080 and runs as a non-root user, so the pod
# can set runAsNonRoot + readOnlyRootFilesystem.
FROM nginxinc/nginx-unprivileged:1.27-alpine
COPY --from=builder /app/build /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 8080
