# Set CGO_ENABLED to 0 by default for all targets
export CGO_ENABLED = 0

# Run all tests (short, deps up, all tests, deps down)
.PHONY: test
test: test-short deps-up test-all deps-down

# Run short/unit tests (smoke test before spinning up dependencies)
.PHONY: test-short
test-short:
	go test -timeout 2m -short ./...

# Start test dependencies
.PHONY: deps-up
deps-up:
	docker compose up --detach --wait

# Stop test dependencies
.PHONY: deps-down
deps-down:
	docker compose down --volumes --remove-orphans

# Run all tests (requires dependencies to be running)
.PHONY: test-all
test-all:
	$(eval PG_PORT := $(shell docker compose port postgres 5432 | cut -f2 -d":"))
	TEST_POSTGRES="postgres://test:test@localhost:$(PG_PORT)/test?sslmode=disable" \
		go test -timeout 2m -cover -coverprofile=coverage.txt -covermode=atomic ./...

# Check spelling
.PHONY: spell-lint
spell-lint:
	docker run \
		--interactive --tty --rm \
		--volume "$(CURDIR):/workdir" \
		--workdir "/workdir" \
		python:3.14-slim bash -c "python -m pip install --upgrade pip && pip install 'codespell>=2.4.1' && codespell"
