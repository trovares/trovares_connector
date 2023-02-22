#/usr/bin/env bash
# -*- coding: utf-8 -*- --------------------------------------------------===#
#
#  Copyright 2022-2023 Trovares Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#===----------------------------------------------------------------------===#

python -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install sphinx pyarrow ipython xgt neo4j

cd docs && make html
