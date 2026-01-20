.PHONY: test e2e cluster-test cluster-up cluster-down clean bench

test:
	pytest

e2e:
	./e2e/test-e2e-fast.sh

cluster-test:
	./e2e/test-helm-with-load.sh

cluster-up:
	./e2e/test-helm.sh run

cluster-down:
	./e2e/test-helm.sh cleanup

clean:
	./e2e/test-helm.sh cleanup
	docker compose -f e2e/docker-compose.e2e.yml down -v 2>/dev/null || true

bench:
	./benchmarks/run.sh
