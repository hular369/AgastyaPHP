.PHONY: all test demo worker docker-up docker-down lint clean

all: test

test:
	@echo "==> Running test suite..."
	php bin/test

demo:
	@echo "==> Running e-commerce simulation demo..."
	php bin/demo

worker:
	@echo "==> Starting Agastya worker daemon..."
	php bin/worker

docker-up:
	@echo "==> Starting Apache Kafka (KRaft), Kafka UI, and Prometheus..."
	docker compose -f deployments/docker-compose.yml up -d

docker-down:
	@echo "==> Stopping Docker infrastructure..."
	docker compose -f deployments/docker-compose.yml down

lint:
	@echo "==> Checking PHP syntax across codebase..."
	@find src tests bin -name "*.php" -exec php -l {} \; > /dev/null
	@echo "All PHP files passed syntax check cleanly!"

clean:
	@rm -rf vendor/ composer.lock
