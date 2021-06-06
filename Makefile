all: build run

THIS_DIR := $(dir $(abspath $(firstword $(MAKEFILE_LIST))))
DOCKER_TAG ?= pyspark-etl-sample

requirements:
	poetry export --with-credentials --without-hashes \
	-f requirements.txt \
	-o requirements.txt

build:
	make requirements
	docker build . -t $(DOCKER_TAG)

run:
	docker run \
	-v $(THIS_DIR)/datasets/:/opt/application/datasets/ \
	-v $(THIS_DIR)/.outputs/:/opt/application/.outputs/ \
	$(DOCKER_TAG) \
	driver local:///opt/application/main.py

lint:
	poetry run python -m pylint src