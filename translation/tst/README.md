# Translation test queries

`test1.yaml` ... : small YAML queries, each covering one feature of the translation (filters, sorting,
joins, spatial and temporal predicates, aggregations, ...). `generate_test_queries.sh` runs
`../script/generateQueries.py` on all of them and writes the generated queries to `tstqueries/`.
