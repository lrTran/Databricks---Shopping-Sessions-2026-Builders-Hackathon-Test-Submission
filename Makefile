IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
# The scorer's platform. PLATFORM=linux/arm64 builds and runs natively on an Apple-silicon Mac.
PLATFORM ?= linux/amd64
DEV    ?= dev
ANSWER ?= dev-answer
FIXTURES ?= fixtures
BIN := target/release/sessions
SEEDS ?= 1 2 3 4 5 6
SYNTH_EVENTS ?= 150000
export IMAGE CPUS MEM PLATFORM

.PHONY: bin test fixtures-local synth-check build fixtures dev run-dev run-dev-local check-dev clean

## ---- Fast local loop (no Docker): needs cargo and `pip install polars==2.1.0` ----

bin:
	cargo build --release

## Unit tests for the CSV dialect and timestamp parsers
test:
	cargo test --release

## All 36 edge-case fixtures against the native binary
fixtures-local: bin
	python3 scripts/run_fixtures.py $(FIXTURES) -- $(BIN)

## Differential test: synthetic dirty data, Rust vs the slow Python reference, several seeds
synth-check: bin
	@set -e; for s in $(SEEDS); do \
	  rm -rf synth/$$s synth/out-rs-$$s synth/out-py-$$s; \
	  python3 scripts/synth.py --seed $$s --events $(SYNTH_EVENTS) --out synth/$$s $$( [ $$((s % 2)) = 0 ] && echo --cr ); \
	  $(BIN) synth/$$s synth/out-rs-$$s 2>/dev/null; \
	  python3 scripts/reference.py synth/$$s synth/out-py-$$s; \
	  python3 scripts/compare.py synth/out-rs-$$s synth/out-py-$$s | tail -1; \
	done

## Native run on the dev set (timing is only indicative)
run-dev-local: bin
	rm -rf out && time $(BIN) $(DEV) out

## ---- Scorer-like loop (Docker) ----

## Build the image for the scorer's platform
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

## Run the image on all 36 fixtures with scorer flags
fixtures: build
	python3 scripts/run_fixtures.py $(FIXTURES) -- scripts/docker_run.sh

## Make the 1M-event dev set with the published generator (needs `generate` on PATH, from generator.zip)
dev:
	generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

## Run the image on the dev set with scorer flags and time it
run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

## Exact diff of out/ against the published answer (unzip dev-answer.zip here)
check-dev:
	python3 scripts/compare.py out $(ANSWER)

clean:
	rm -rf out synth target
