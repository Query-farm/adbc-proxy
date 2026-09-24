# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generate Foundry capability documentation from validation reports."""

import argparse
from pathlib import Path

from adbc_drivers_validation import generate_documentation

from .grainlift import get_quirks

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    repository = Path(__file__).resolve().parents[2]
    reports = sorted(repository.glob("validation-report*.xml"))
    generate_documentation.generate(
        "grainlift",
        get_quirks,
        [
            ("grainlift-sqlite", "SQLite"),
            ("grainlift-duckdb", "DuckDB"),
            ("grainlift-postgresql", "PostgreSQL"),
        ],
        reports,
        repository / "docs/grainlift.md",
        args.output.resolve(),
    )
