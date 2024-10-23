# Requirements for running the benchmark
You'll need to pass the the GCP_IP of your main instance in order to run queries. Additionally, you'll need Python with the following installed packages at least:
```
psycopg2
```

You can then run the benchmark using the following command, with "5432" being the default port of PostgreSQL servers (which MobilityDB is based on )
```
python runMiniBenchmark.py <GCP_IP> 5432
Example:
python runMiniBenchmark.py 32.219.34.10 5432