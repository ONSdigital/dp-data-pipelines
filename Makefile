.PHONY: all

# Help menu on a naked make
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

install: ## Install LOCAL development dependencies
	pipenv sync --dev

fmt: install ## (Format) - runs black and isort against the codebase (auto triggered on pre-commit)
	pipenv run black ./pipelines/*
	pipenv run isort ./pipelines/*

lint: install ## Run the ruff python linter (auto triggered on pre-commit)
	pipenv run ruff ./pipelines/*

test: install ## Run pytest and check test coverage (auto triggered on pre-push)
	pipenv run pytest --cov-report term-missing --cov=pipelines