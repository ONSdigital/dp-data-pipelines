.PHONY: all

version = v0.13.0
remote_tools_config = { git = "https://github.com/ONSdigital/dp-python-tools.git", tag = "${version}" }
local_tools_config = { path = "../dp-python-tools",  develop = true }

# Help menu on a naked make
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

install: ## Install development dependencies
	poetry install

fmt: install ## (Format) - runs Ruff against the codebase (auto triggered on pre-commit)
	poetry run ruff format

lint: install ## Run the ruff python linter
	poetry run ruff check --fix

tests-unit: install
	poetry run pytest --cov-report term-missing --cov=dpypelines ./tests/pipelines

tests-integration: install
	poetry run pytest --cov-report term-missing --cov=dpypelines ./tests/integration

test: install ## Run pytest and check test coverage
	make tests-unit
	make tests-integration

deploy:
	./buildpackage.sh

symlink:
	# Switch to production version
	sed -i '' 's|${remote_tools_config}|${local_tools_config}|' pyproject.toml
	poetry lock
	poetry install

unlink:
	# Switch to production version
	sed -i '' 's|${local_tools_config}|${remote_tools_config}|' pyproject.toml
	poetry lock
	poetry install
