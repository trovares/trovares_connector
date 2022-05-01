#/usr/bin/env bash

python -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install sphinx pyarrow ipython xgt neo4j

cd docs && make html
