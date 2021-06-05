lint: ## check style with pylint
	@echo "Linting started"
	@poetry run pylint app_events/
