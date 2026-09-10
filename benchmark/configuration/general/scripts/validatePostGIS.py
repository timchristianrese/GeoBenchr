import psycopg2
import yaml
import os


def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="postgres",
        user="postgres",
        password="test"
    )


def validate_sql_queries(yaml_file):
    conn = get_db_connection()
    cursor = conn.cursor()

    with open(yaml_file, "r") as f:
        queries = yaml.safe_load(f)

    results = []

    for query_def in queries:
        name = query_def.get("name")
        sql = query_def.get("sql")

        print(f"\nValidating: {name}")

        try:
            cursor.execute(sql)

            # Fetch only a small sample to confirm execution
            rows = cursor.fetchmany(5)

            results.append({
                "name": name,
                "valid": True,
                "sample_rows": rows
            })

            print("  ✓ Valid")
            print(f"  Sample rows: {rows[:2]}")

        except Exception as e:
            conn.rollback()

            results.append({
                "name": name,
                "valid": False,
                "error": str(e)
            })

            print("  ✗ Invalid")
            print(f"  Error: {e}")

    cursor.close()
    conn.close()

    return results


if __name__ == "__main__":
    yaml_file = "../queries/postgisSQL_queries.yaml"

    validation_results = validate_sql_queries(yaml_file)

    print("\nSummary")
    print("=" * 50)

    for result in validation_results:
        status = "PASS" if result["valid"] else "FAIL"
        print(f"{status}: {result['name']}")