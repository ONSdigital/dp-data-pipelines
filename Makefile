.PHONY: all

version = v0.2.0-wip.11
remote_tools_config = { git = "https://github.com/ONSdigital/dp-python-tools.git", tag = "${version}" }
local_tools_config = {path = "../dp-python-tools",  develop = true}
# Help menu on a naked make
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

install: ## Install development dependencies
	poetry install

fmt: install ## (Format) - runs black and isort against the codebase (auto triggered on pre-commit)
	poetry run black .
	poetry run isort ./dpypelines/ ./tests/*

lint: install ## Run the ruff python linter
	poetry run ruff check

test: install ## Run pytest and check test coverage
	poetry run pytest --cov-report term-missing --cov=dpypelines

feature: install
	poetry run behave

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
