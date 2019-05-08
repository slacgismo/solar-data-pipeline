# solar-data-pipeline

Gets solar data from various source

## Usage

#### Example 1: Example to get data from Cassandra Database.

```python
import numpy as np
from solar_data_pipeline.database.cassandra import CassandraDataAccess

cassandra_ip_address = "127.0.0.1"
data_access = CassandraDataAccess(cassandra_ip_address)
data = data_access.retrieve()
```

## Versioning

We use [Semantic Versioning](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/bmeyers/StatisticalClearSky/tags).

## Authors

* **Tadatoshi Takahashi** - [Tadatoshi Takahashi GitHub](https://github.com/tadatoshi)

## License

This project is licensed under the BSD 2-Clause License - see the [LICENSE](LICENSE) file for details
