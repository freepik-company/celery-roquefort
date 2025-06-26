.PHONY: help setup install dev run docker-build docker-run clean

# Default target
help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

setup: ## Set up development environment
	@echo "Setting up development environment..."
	uv sync
	@echo "Development environment ready!"

install: setup ## Install dependencies (alias for setup)

dev: ## Run the application in development mode
	uv run python src/roquefort.py

run: dev ## Run the application (alias for dev)

docker-build: ## Build Docker image
	docker build -t roquefort:develop .

docker-run: ## Run Docker container
	docker run -p 8000:8000 roquefort:develop

docker-dev: docker-build docker-run ## Build and run Docker container

clean: ## Clean up cache and temporary files
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .uv_cache

check: ## Show dependency tree and project status
	@echo "Project dependency tree:"
	@uv tree

update: ## Update dependencies
	uv sync --upgrade

shell: ## Open a shell with the project environment
	uv run python

info: ## Show project information
	@echo "Project: celery-roquefort"
	@echo "Python version: $(shell uv run python --version)"
	@echo "UV version: $(shell uv --version)"
	@echo "Dependencies:"
	@uv tree 

git-prune: ## Prune the git repository
	@git branch --format '%(refname:short) %(upstream:track)' | \
		grep -E '\[gone\]|\[desaparecido\]' | \
		awk '{print $$1}' > .branches_to_delete
	@vim .branches_to_delete
	@if [ -s .branches_to_delete ]; then \
		echo "Deleting selected branches..."; \
		cat .branches_to_delete | xargs -I {} git branch -D {}; \
	else \
		echo "No branches were selected for deletion."; \
	fi
	@rm .branches_to_delete