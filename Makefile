.PHONY: test e2e cluster-test cluster-up cluster-load clean

test:
	pytest

e2e:
	./e2e/test-e2e-fast.sh

# Full cluster test (CI) - creates cluster, runs load test, cleans up
cluster-test:
	./e2e/test-cluster.sh

# Start cluster and keep running (local dev) - use cluster-load to test
cluster-up:
	docker build -t s3proxy:latest .
	./e2e/cluster.sh run

# Run load test against running cluster
cluster-load:
	./e2e/cluster.sh load-test

clean:
	./e2e/cluster.sh cleanup
	docker compose -f e2e/docker-compose.e2e.yml down -v 2>/dev/null || true
