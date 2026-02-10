.PHONY: test test-all test-unit test-run test-oom e2e cluster lint

# Lint: ruff check + format check
lint:
	uv run ruff check .
	uv run ruff format --check .

# Default: run unit tests only (no containers needed)
test: test-unit

# Run unit tests (excludes e2e and ha tests)
test-unit:
	uv run pytest -m "not e2e and not ha" -v -n auto

# Run all tests with containers (parallel execution)
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

# OOM proof test: runs s3proxy in a 128MB container and hammers it
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
