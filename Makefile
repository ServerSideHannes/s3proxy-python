.PHONY: test test-cov test-full e2e e2e-quick helm helm-cleanup clean bench bench-quick bench-profile

# Unit tests
test:
	pytest

test-cov:
	pytest --cov=s3proxy

# Full test suite (e2e + helm)
test-full: e2e helm

# E2E tests
e2e:
	./e2e/test-e2e-fast.sh

e2e-quick:
	QUICK_MODE=true ./e2e/test-e2e-fast.sh

# Helm tests
helm-test:
	./e2e/test-helm-validate.sh

helm:
	./e2e/test-helm.sh run

helm-status:
	./e2e/test-helm.sh status

helm-logs:
	./e2e/test-helm.sh logs

helm-load-test:
	./e2e/test-helm-with-load.sh

helm-redis:
	./e2e/test-helm.sh redis

helm-pods:
	./e2e/test-helm.sh pods

helm-watch:
	./e2e/test-helm.sh watch

helm-shell:
	./e2e/test-helm.sh shell

helm-cleanup:
	./e2e/test-helm.sh cleanup

# Cleanup
clean:
	./e2e/test-helm.sh cleanup
	docker-compose -f e2e/docker-compose.e2e.yml down -v 2>/dev/null || true

# Benchmarks (Docker only, no external deps)
bench:
	./benchmarks/run.sh

bench-quick:
	./benchmarks/run.sh --quick

bench-profile:
	./benchmarks/profile.sh
