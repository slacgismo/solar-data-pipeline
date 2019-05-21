# solar-data-pipeline

Gets solar data from various source

## Usage

#### Example 1: Example to get random data from Cassandra Database.

```python
import numpy as np
from solar_data_pipeline.database.raw_cassandra import RawCassandraDataAccess

number_of_sites = 4
number_of_days_per_site = 10

data_access = CassandraDataAccess()
data = data_access.retrieve(number_of_sites=number_of_sites,
        number_of_days_per_site=number_of_days_per_site)
```

## Versioning

We use [Semantic Versioning](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/bmeyers/StatisticalClearSky/tags).

## Authors

* **Tadatoshi Takahashi** - [Tadatoshi Takahashi GitHub](https://github.com/tadatoshi)

## License

This project is licensed under the BSD 2-Clause License - see the [LICENSE](LICENSE) file for details
