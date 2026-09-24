---
{}
---

<!--
  Copyright (c) 2026 ADBC Drivers Contributors
  Copyright (c) 2026 Query Farm LLC

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

{{ cross_reference|safe }}
# Grainlift ADBC Driver {{ version }}

{{ heading|safe }}

Grainlift is a client-side ADBC driver that connects to the Grainlift
service. The service owns the downstream ADBC driver and its stateful database,
connection, statement, transaction, and result-stream objects.

## Installation

Once released through the ADBC Driver Foundry, install the client driver with
[`dbc`](https://docs.columnar.tech/dbc/):

```console
dbc install grainlift
```

The Grainlift service is deployed separately. It must have the desired downstream
ADBC drivers installed and configured as authorized targets.

## Connecting

Supply the Grainlift endpoint and server-configured target as database options:

```python
from adbc_driver_manager import dbapi

with dbapi.connect(
    driver="grainlift",
    db_kwargs={
        "grainlift.uri": "grainlift://grainlift.example.com",
        "grainlift.target": "analytics",
        "grainlift.auth.bearer_token": "TOKEN",
    },
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1")
        print(cursor.fetch_arrow_table())
```

Capabilities and type behavior depend on both Grainlift and the selected
downstream driver. The tables below show the downstream configurations covered
by the Foundry validation suite.

## Feature and type support

{{ features|safe }}

### Types

{{ types|safe }}

{{ footnotes|safe }}

## Compatibility

{{ compatibility_info|safe }}

For deployment, authentication, transport, and option details, see the
[project README](https://github.com/Query-farm/grainlift).
