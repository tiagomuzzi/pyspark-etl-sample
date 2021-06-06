all: build run

THIS_DIR := $(dir $(abspath $(firstword $(MAKEFILE_LIST))))


lint: ## check style with pylint
	@echo "Linting started"
	@poetry run pylint app_events/

build:
	docker build . -t fysp/pyspark-etl-sample

run:
	echo $(THIS_DIR)
	docker run \
	-v $(THIS_DIR)/datasets/:/opt/application/datasets/ \
	-v $(THIS_DIR)/.outputs/:/opt/application/.outputs/ \
	fysp/pyspark-etl-sample \
	driver local:///opt/application/main.py \
	--log-level=warn