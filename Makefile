.PHONY: all

# Help menu on a naked make
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

install: ## Install development dependencies
	poetry install

fmt: install ## (Format) - runs black and isort against the codebase (auto triggered on pre-commit)
	poetry run black ./dpypelines/ ./tests/*
	poetry run isort ./dpypelines/ ./tests/*

lint: install ## Run the ruff python linter
	poetry run ruff check ./dpypelines/ ./tests/*

test: install ## Run pytest and check test coverage
	poetry run pytest --cov-report term-missing --cov=dpypelines

feature: install
	poetry run behave


symlink:
	# Switch to production version
	sed -i '' 's|{ git = "https://github.com/ONSdigital/dp-python-tools.git", tag = "v0.5.1" }|{path = "../dp-python-tools",  develop = true}|' pyproject.toml
	poetry lock
	poetry install


unlink:
	# Switch to production version
	sed -i '' 's|{path = "../dp-python-tools",  develop = true}|{ git = "https://github.com/ONSdigital/dp-python-tools.git", tag = "v0.5.1" }|' pyproject.toml
	poetry lock
	poetry install