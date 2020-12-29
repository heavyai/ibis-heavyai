.PHONY: develop init-service run-tests start-service stop-service


DOCKER_RUN := docker-compose --file ./ci/docker-compose.yaml
TEST_PARAMS :=

start-service:
	$(DOCKER_RUN) up -d omniscidb
	$(DOCKER_RUN) run --rm waiter


stop-service:
	$(DOCKER_RUN) stop --rm omniscidb


init-service: start-service
	python ci/setup_tests.py


run-tests:
	python -m pytest ibis_omniscidb \
		-ra \
		--junitxml=junit.xml \
		--cov=ibis \
		--cov-report=xml:coverage.xml \
		${TEST_PARAMS}


develop:
	python -m pip install -e .
