.PHONY: ipums bls build figs lint test all

PY := python

ipums:
	$(PY) -m src.data.ipums_cps

bls:
	$(PY) -m src.data.bls_absences

build:
	$(PY) -m src.features.parent_flags
	$(PY) -m src.analysis.did_model

figs:
	$(PY) -m src.viz.plots

lint:
	ruff check . || true
	black --check . || true
	isort --check-only . || true

test:
	pytest -q

all: ipums build figs

