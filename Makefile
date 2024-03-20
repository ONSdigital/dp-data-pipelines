.PHONY: all

# Help menu on a naked make
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

install: ## Install LOCAL development dependencies
	poetry install

fmt: install ## (Format) - runs black and isort against the codebase (auto triggered on pre-commit)
	poetry run black ./dpypelines/*
	poetry run isort ./dpypelines/*

lint: install ## Run the ruff python linter (auto triggered on pre-commit)
	poetry run ruff ./dpypelines/*

test: install ## Run pytest and check test coverage (auto triggered on pre-push)
	poetry run pytest --cov-report term-missing --cov=dpypelines

feature: install
	poetry run behave