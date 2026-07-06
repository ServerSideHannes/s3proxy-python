.PHONY: test test-all test-unit test-integration test-integration-shard test-run test-oom e2e cluster lint

# Lint: ruff check + format check
lint:
	uv run ruff check .
	uv run ruff format --check .

# Default: run unit tests only (no containers needed)
test: test-unit

# Run unit tests (excludes e2e and ha tests)
test-unit:
	uv run pytest -m "not e2e and not ha" -v -n auto

# Integration shards for parallel CI (make test-integration-shard SHARD=memory_usage)
INTEGRATION_memory_usage_TESTS = tests/integration/test_memory_usage.py
INTEGRATION_memory_leak_TESTS = tests/integration/test_memory_leak.py
INTEGRATION_memory_copy_TESTS = tests/integration/test_copy_memory_governor.py
INTEGRATION_core_TESTS = \
	tests/integration/test_integration.py \
	tests/integration/test_handlers.py \
	tests/integration/test_concurrent_operations.py \
	tests/integration/test_per_key_encryption.py \
	tests/integration/test_redis_coordination.py
INTEGRATION_multipart_TESTS = \
	tests/integration/test_upload_part_copy.py \
	tests/integration/test_part_ordering.py \
	tests/integration/test_sequential_part_numbering.py \
	tests/integration/test_sequential_part_numbering_e2e.py \
	tests/integration/test_large_file_streaming.py \
	tests/integration/test_entity_too_small_errors.py \
	tests/integration/test_entity_too_small_fix.py \
	tests/integration/test_partial_complete_fix.py \
	tests/integration/test_multipart_range_validation.py
INTEGRATION_copy_range_TESTS = \
	tests/integration/test_copy_passthrough.py \
	tests/integration/test_download_range_requests.py \
	tests/integration/test_get_prefetch.py \
	tests/integration/test_elasticsearch_range_scenario.py \
	tests/integration/test_state_recovery.py \
	tests/integration/test_state_recovery_e2e.py
INTEGRATION_misc_TESTS = \
	tests/integration/test_delete_objects_errors.py \
	tests/integration/test_metadata_and_errors.py

# Run integration tests (needs minio/redis containers)
test-integration:
	@docker compose -f tests/docker-compose.yml down 2>/dev/null || true
	@docker compose -f tests/docker-compose.yml up -d
	@sleep 3
	@AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin uv run pytest -m "e2e" -v -n auto --dist loadgroup; \
		EXIT_CODE=$$?; \
		docker compose -f tests/docker-compose.yml down; \
		exit $$EXIT_CODE

# Run one integration shard (CI matrix). SHARD=memory_usage|memory_leak|memory_copy|...
test-integration-shard:
ifndef SHARD
	$(error SHARD is required, e.g. make test-integration-shard SHARD=memory)
endif
	@docker compose -f tests/docker-compose.yml down 2>/dev/null || true
	@docker compose -f tests/docker-compose.yml up -d
	@sleep 3
	@AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
		uv run pytest -m "e2e" -v -n auto --dist loadgroup $(INTEGRATION_$(SHARD)_TESTS); \
		EXIT_CODE=$$?; \
		docker compose -f tests/docker-compose.yml down; \
		exit $$EXIT_CODE

# Run all tests with containers (unit + integration)
test-all:
	@docker compose -f tests/docker-compose.yml down 2>/dev/null || true
	@docker compose -f tests/docker-compose.yml up -d
	@sleep 3
	@AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin uv run pytest -v -n auto --dist loadgroup; \
		EXIT_CODE=$$?; \
		docker compose -f tests/docker-compose.yml down; \
		exit $$EXIT_CODE

# Run specific test file/pattern with containers
# Usage: make test-run TESTS=tests/integration/test_foo.py
test-run:
	@docker compose -f tests/docker-compose.yml down 2>/dev/null || true
	@docker compose -f tests/docker-compose.yml up -d
	@sleep 3
	@AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin uv run pytest -v -n auto --dist loadgroup $(TESTS); \
		EXIT_CODE=$$?; \
		docker compose -f tests/docker-compose.yml down; \
		exit $$EXIT_CODE

# OOM proof test: runs s3proxy in a 256MB container and hammers it
test-oom:
	@docker compose -f tests/docker-compose.yml --profile oom down 2>/dev/null || true
	@docker compose -f tests/docker-compose.yml --profile oom up -d --build
	@sleep 5
	@AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
		uv run pytest -v tests/integration/test_memory_leak.py; \
		EXIT_CODE=$$?; \
		docker compose -f tests/docker-compose.yml --profile oom down; \
		exit $$EXIT_CODE

# E2E cluster commands
e2e:
	./e2e/cluster.sh $(filter-out $@,$(MAKECMDGOALS))

cluster:
	./e2e/cluster.sh $(filter-out $@,$(MAKECMDGOALS))

%:
	@:
